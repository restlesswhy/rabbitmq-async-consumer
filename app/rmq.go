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
	Queue    string
	RouteKey []string
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

	connection    *amqp.Connection
	channel       *amqp.Channel
	notifyClose   chan *amqp.Error
	notifyConfirm chan amqp.Confirmation
	recvQ         <-chan amqp.Delivery

	close chan struct{}

	queue    string
	routeKey []string

	isConnected bool
	alive       bool
}

func New(cfg *config.Config, params *Params) Rmq {
	rmq := &rmq{
		wg:       &sync.WaitGroup{},
		cfg:      cfg,
		close:    make(chan struct{}),
		queue:    params.Queue,
		routeKey: params.RouteKey,
		alive:    true,
	}

	rmq.wg.Add(2)
	go rmq.handleReconnect()
	go rmq.run()

	return rmq
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
		true,  // Durable
		true,  // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		logrus.Errorf(`declare %s queue error: %v`, r.queue, err)
		return false
	}

	exchange := `direct_logs`
	if err := ch.ExchangeDeclare(
		exchange,
		amqp.ExchangeDirect,
		true,
		true,
		false,
		false,
		nil,
	); err != nil {
		logrus.Errorf(`declare exchange %s error: %v`, exchange, err)

		return false
	}
	
	for _, key := range r.routeKey {
		if err := ch.QueueBind(q.Name, key, exchange, false, nil); err != nil {
			logrus.Errorf(`bind queue %s error: %v`, r.queue, err)

			return false
		}
	}

	recvQ, err := ch.Consume(
		r.queue,
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

			logrus.Infof("Msg from %+s: %s", r.routeKey[:], string(msg.Body))
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

		err := r.channel.Close()
		if err != nil {
			return err
		}

		err = r.connection.Close()
		if err != nil {
			return err
		}

		return nil
	}
}
