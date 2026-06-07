package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body:        buf.Bytes(),
		},
	)
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	bytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        bytes,
		},
	)
}

func unmarshalJSON[T any](data []byte) (T, error) {
	var msg T
	err := json.Unmarshal(data, &msg)
	return msg, err
}

func unmarshalGob[T any](data []byte) (T, error) {
	var msg T
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(&msg)
	return msg, err
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}

	err = ch.Qos(10, 0, false)
	if err != nil {
		return err
	}

	deliveries, err := ch.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveries {
			msg, err := unmarshaller(delivery.Body)
			if err != nil {
				log.Printf("failed to unmarshal message: %v", err)
				continue
			}
			switch handler(msg) {
			case Ack:
				log.Println("Acknowledging message")
				delivery.Ack(false)
			case NackRequeue:
				log.Println("Nacking message with requeue")
				delivery.Nack(false, true)
			case NackDiscard:
				log.Println("Nacking message with discard")
				delivery.Nack(false, false)
			}
		}
	}()

	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshalJSON[T])
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshalGob[T])
}