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
	handler func(T) string,
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
		defer ch.Close()
		for msg := range consumedCh {
			var data T
			if err := json.Unmarshal(msg.Body, &data); err != nil {
				log.Printf("error unmarshaling: %v", err)
				continue
			}
			ack := handler(data)
			switch ack {
			case "Ack":
				if err := msg.Ack(false); err != nil {
					log.Printf("error acknowledging: %v", err)
					continue
				}
				log.Print("Ack")
			case "NackRequeue":
				if err := msg.Nack(false, true); err != nil {
					log.Printf("error nack and requeue: %v", err)
					continue
				}
				log.Print("Nack and requeue")
			case "NackDiscard":
				if err := msg.Nack(false, false); err != nil {
					log.Printf("error nack and discard: %v", err)
					continue
				}
				log.Print("Nack and discard")
			}

		}
	}()
	return nil
}
