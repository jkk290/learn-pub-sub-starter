package main

import (
	"fmt"
	"log"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	_, _, bindErr := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, "game_logs", routing.GameLogSlug, pubsub.Durable)
	if bindErr != nil {
		log.Fatalf("error binding queue: %v", bindErr)
	}

	gamelogic.PrintServerHelp()

	for {
		userInput := gamelogic.GetInput()
		switch userInput[0] {
		case "pause":
			log.Println("sending pause message...")
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				log.Printf("could not publish time: %v", err)
			}
			fmt.Println("Pause message sent!")

		case "resume":
			log.Println("sending resume message...")
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				log.Printf("could not publish time: %v", err)
			}
			fmt.Println("Resume message sent!")

		case "quit":
			log.Println("Shutting down server...")
			os.Exit(0)

		default:
			log.Println("Unknown command")
		}
	}
}
