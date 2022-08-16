package app

import (
	"encoding/json"
	"io"
	"rabbit/config"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second

	// When resending messages the server didn't confirm
	resendDelay = 5 * time.Second
)

var ErrClosed = errors.New("rmq closed")

type Params struct {
	Exchange  string
	RouteKeys []string
}

type Message struct {
	Text string `json:"text"`
}

type Rmq interface {
	io.Closer
}

type rmq struct {
	wg  *sync.WaitGroup
	cfg *config.Config

	connection  *amqp.Connection
	channel     *amqp.Channel
	notifyClose chan *amqp.Error

	recvQ     <-chan amqp.Delivery
	exchange  string
	routeKeys []string

	close chan struct{}

	isConnected bool
}

func New(cfg *config.Config, params *Params) (Rmq, error) {
	rmq := &rmq{
		wg:        &sync.WaitGroup{},
		cfg:       cfg,
		close:     make(chan struct{}),
		recvQ:     make(<-chan amqp.Delivery),
		exchange:  params.Exchange,
		routeKeys: params.RouteKeys,
	}

	if err := rmq.Connect(); err != nil {
		return nil, errors.Wrap(err, "connect or rabbitmq error")
	}

	rmq.wg.Add(2)
	go rmq.listenCloseConn()
	go rmq.run()

	return rmq, nil
}

func (r *rmq) Connect() error {
	var err error

	r.connection, err = amqp.Dial(r.cfg.Addr)
	if err != nil {
		return errors.Wrap(err, `dial rabbit error`)
	}

	r.channel, err = r.connection.Channel()
	if err != nil {
		return errors.Wrap(err, `conn to channel error`)
	}

	r.notifyClose = make(chan *amqp.Error)
	r.connection.NotifyClose(r.notifyClose)

	q, err := r.channel.QueueDeclare(
		"",
		false, // Durable
		false, // Delete when unused
		true,  // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		return errors.Wrap(err, `declare queue error`)
	}

	if err := r.channel.ExchangeDeclare(
		r.exchange,          // name
		amqp.ExchangeDirect, // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // noWait
		nil,                 // arguments
	); err != nil {
		return errors.Wrap(err, "declare exchange error")
	}

	for _, v := range r.routeKeys {
		if err := r.channel.QueueBind(q.Name, v, r.exchange, false, nil); err != nil {
			return errors.Wrap(err, `bind queue error`)
		}
	}

	recvQ, err := r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return errors.Wrap(err, `consume queue error`)
	}

	r.isConnected = true
	r.recvQ = recvQ

	return nil
}

func (r *rmq) listenCloseConn() {
	defer r.wg.Done()

main:
	for {
		select {
		case <-r.close:
			break main

		case <-r.notifyClose:
			if r.isConnected == false {
				continue
			}

			r.isConnected = false
			logrus.Warn("Connection is closed! Trying to reconnect...")
			r.handleRec()
		}
	}

	logrus.Info("Connection listener is closed")
}

func (r *rmq) handleRec() {
	attempts := 1
	for {
		time.Sleep(reconnectDelay + time.Second*time.Duration(attempts))
		r.channel = nil
		r.connection = nil

		select {
		case <-r.close:
			logrus.Infof("Stop trying to connect: %v", ErrClosed)
			return

		default:
			logrus.Infof("%d attempt to reconnect...", attempts)

			if err := r.Connect(); err != nil {
				logrus.Errorf("Failed to connect: %v", err)
				attempts++
				continue
			} else {
				logrus.Info("Successfully connected!")
				r.isConnected = true
				return
			}
		}
	}
}

func (r *rmq) run() {
	defer r.wg.Done()

main:
	for {
		if !r.isConnected {
			continue
		}

		select {
		case <-r.close:
			break main

		case msg, ok := <-r.recvQ:
			if !ok {
				continue
			}

			res := &Message{}

			if err := json.Unmarshal(msg.Body, res); err != nil {
				logrus.Error("Failed unmarshal message: %v", string(msg.Body))
				continue
			}

			logrus.Infof("Msg from %+s: %s", r.routeKeys[:], res.Text)
		}
	}

	logrus.Info("RBT closed!")
}

func (r *rmq) Close() error {
	select {
	case <-r.close:
		return ErrClosed
	default:
		close(r.close)
		r.wg.Wait()

		if r.channel != nil {
			if err := r.channel.Close(); err != nil {
				return err
			}
		}

		if r.connection != nil {
			if err := r.connection.Close(); err != nil {
				return err
			}
		}

		return nil
	}
}
