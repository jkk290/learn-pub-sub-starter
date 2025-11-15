package pubsub

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	consumedCh, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for msg := range consumedCh {
		var data T
		if err := json.Unmarshal(msg.Body, data); err != nil {
			return err
		}
		go handler(data)
		if err := amqp.Delivery.Ack(msg, false); err != nil {
			return err
		}
	}
	return nil
}
