package app

import (
	"fmt"
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
		wg:    &sync.WaitGroup{},
		cfg:   cfg,
		close: make(chan struct{}),
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

	if err := r.channel.ExchangeDeclare(
		r.exchange,      // name
		amqp.ExchangeDirect, // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // noWait
		nil,                 // arguments
	); err != nil {
		return errors.Wrap(err, "declare exchange error")
	}

	r.isConnected = true

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


func (r *rmq) handleReconnect() {
	defer func() {
		r.wg.Done()
		logrus.Printf("Stop reconnecting to rabbitMQ")
	}()

	for r.alive {
		r.isConnected = false
		t := time.Now()
		fmt.Printf("Attempting to connect to rabbitMQ: %s\n", r.cfg.Addr)
		var retryCount int
		for !r.connect(r.cfg) {
			if !r.alive {
				return
			}
			select {
			case <-r.close:
				return
			case <-time.After(reconnectDelay + time.Duration(retryCount)*time.Second):
				logrus.Printf("disconnected from rabbitMQ and failed to connect")
				retryCount++
			}
		}
		logrus.Printf("Connected to rabbitMQ in: %vms", time.Since(t).Milliseconds())
		select {
		case <-r.close:
			return
		case <-r.notifyClose:
		}
	}
}

func (r *rmq) connect(cfg *config.Config) bool {
	conn, err := amqp.Dial(cfg.Addr)
	if err != nil {
		logrus.Errorf(`dial rabbit error: %v`, err)
		return false
	}

	ch, err := conn.Channel()
	if err != nil {
		logrus.Errorf(`conn to channel error: %v`, err)
		return false
	}

	q, err := ch.QueueDeclare(
		"",
		false, // Durable
		false, // Delete when unused
		true,  // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		logrus.Errorf(`declare %s queue error: %v`, r.queue, err)
		return false
	}
	r.q = q.Name

	exchange := "logs"
	if err := ch.ExchangeDeclare(
		exchange,
		amqp.ExchangeDirect,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		logrus.Errorf(`declare exchange %s error: %v`, exchange, err)

		return false
	}

	if err := ch.QueueBind(q.Name, "warn_key", exchange, false, nil); err != nil {
		logrus.Errorf(`bind queue %s error: %v`, r.queue, err)

		return false
	}
	// for _, _ = range r.routeKey {
	// }

	recvQ, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logrus.Errorf(`consume queue %s error: %v`, r.queue, err)

		return false
	}

	// go func() {
	// 	for d := range recvQ {
	// 		fmt.Printf("Recieved Message: %s\n", d.Body)
	// 	}
	// }()

	r.recvQ = recvQ

	r.changeConnection(conn, ch)
	r.isConnected = true

	return true
}

func (r *rmq) changeConnection(connection *amqp.Connection, channel *amqp.Channel) {
	r.connection = connection
	r.channel = channel
	r.notifyClose = make(chan *amqp.Error)
	r.notifyConfirm = make(chan amqp.Confirmation)
	r.channel.NotifyClose(r.notifyClose)
	r.channel.NotifyPublish(r.notifyConfirm)
	r.d = true
}

func (r *rmq) run() {
	defer r.wg.Done()

main:
	for {
		if !r.d {
			continue
		}

		select {
		case <-r.close:
			break main

		case msg := <-r.recvQ:
			// if !ok {
			// 	continue
			// }

			logrus.Infof("Msg from %+s: %s", r.routeKeys[:], string(msg.Body))
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
