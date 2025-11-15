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
	fmt.Println("Starting Peril client...")
	const connectionString = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("error connecting to rabbit mq: %v", err)
	}
	defer conn.Close()
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	queueName := routing.PauseKey + "." + username
	_, _, bindErr := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient)
	if bindErr != nil {
		log.Fatalf("error binding queue: %v", bindErr)
	}

	gameState := gamelogic.NewGameState(username)
	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient, handlerPause(gameState)); err != nil {
		log.Fatalf("error subscribing JSON: %v", err)
	}

	for {
		userInput := gamelogic.GetInput()

		switch userInput[0] {
		case "spawn":
			if err := gameState.CommandSpawn(userInput); err != nil {
				log.Printf("error spawning unit: %v\n", err)
			}
		case "move":
			armyMove, err := gameState.CommandMove(userInput)
			if err != nil {
				log.Printf("error moving unit: %v\n", err)
			}
			log.Printf("successfully moved: %v\n", armyMove)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			log.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			os.Exit(0)
		default:
			log.Println("unknown command")
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(routing.PlayingState{})
	}
}
