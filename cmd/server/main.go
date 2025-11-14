package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const connectionString = "amqp://guest:guest@localhost:5672/"
	amqpConnection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("error creating amqp connection: %v", err)
	}
	defer amqpConnection.Close()
	fmt.Println("amqp connection successful!")
	amqpCh, err := amqpConnection.Channel()
	if err != nil {
		log.Fatalf("error creating channel: %v", err)
	}

	if err := pubsub.PublishJSON(amqpCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: true,
	}); err != nil {
		log.Fatalf("error publishing JSON: %v", err)
	}

	fmt.Println("Starting Peril server...")
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Server shutting down...")
}
