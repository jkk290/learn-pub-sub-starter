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
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("error creating channel")
	}
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	queueName := routing.PauseKey + "." + username

	gameState := gamelogic.NewGameState(username)
	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient, handlerPause(gameState)); err != nil {
		log.Fatalf("error subscribing JSON: %v", err)
	}

	armyMovesQueue := routing.ArmyMovesPrefix + "." + username
	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, armyMovesQueue, "army_moves.*", pubsub.Transient, handlerMove(gameState, ch)); err != nil {
		log.Fatalf("error subscribing to army move: %v", err)
	}

	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, "war", routing.WarRecognitionsPrefix+".*", pubsub.Durable, handlerWar(gameState)); err != nil {
		log.Fatalf("error subscribing to war: %v", err)
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
			if err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				armyMovesQueue,
				armyMove,
			); err != nil {
				log.Println("error moving unit")
			}
			log.Printf("move successful %v", armyMove)
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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) string {
	return func(ps routing.PlayingState) string {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return "Ack"
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) string {
	return func(am gamelogic.ArmyMove) string {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(am)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return "Ack"
		case gamelogic.MoveOutcomeMakeWar:
			if err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+gs.Player.Username, gamelogic.RecognitionOfWar{
				Attacker: am.Player,
				Defender: gs.GetPlayerSnap(),
			}); err != nil {
				return "NackRequeue"
			}
			return "Ack"
		case gamelogic.MoveOutcomeSamePlayer:
			return "Ack"
		default:
			return "NackDiscard"
		}
	}
}

func handlerWar(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) string {
	return func(rw gamelogic.RecognitionOfWar) string {
		defer fmt.Print("> ")
		warOutcome, _, _ := gs.HandleWar(rw)
		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return "NackRequeue"
		case gamelogic.WarOutcomeNoUnits:
			return "NackDiscard"
		case gamelogic.WarOutcomeYouWon:
			return "Ack"
		case gamelogic.WarOutcomeDraw:
			return "Ack"
		default:
			fmt.Print("error handling war")
			return "NackDiscard"
		}
	}
}
