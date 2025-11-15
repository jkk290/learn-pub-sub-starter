package pubsub

import (
	"encoding/json"
	"log"

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
	go func() {
		for msg := range consumedCh {
			var data T
			if err := json.Unmarshal(msg.Body, &data); err != nil {
				log.Printf("error unmarshaling: %v", err)
				continue
			}
			handler(data)
			if err := msg.Ack(false); err != nil {
				log.Printf("error acknowledging: %v", err)
				continue
			}
		}
	}()
	return nil
}
