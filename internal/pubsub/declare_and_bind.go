package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

type SimpleQueueType int

const (
	SimpleQueueDurable   SimpleQueueType = iota
	SimpleQueueTransient SimpleQueueType = iota
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	durable := queueType == SimpleQueueDurable
	autoDelete := queueType == SimpleQueueTransient
	exclusive := queueType == SimpleQueueTransient

	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false, // noWait
		amqp.Table{
			"x-dead-letter-exchange": routing.ExchangePerilDeadLetter,
		},
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(
		queue.Name,
		key,
		exchange,
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}