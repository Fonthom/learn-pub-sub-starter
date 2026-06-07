package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	fmt.Println("Starting Peril client...")

	connString := "amqp://guest:guest@localhost:5672/"

	var conn *amqp.Connection
	var err error
	for i := 1; i <= 10; i++ {
		conn, err = amqp.Dial(connString)
		if err == nil {
			break
		}
		fmt.Printf("RabbitMQ not ready, retrying (%d/10)...\n", i)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ after retries: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Successfully connected to RabbitMQ!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("Failed to get username: %v\n", err)
		os.Exit(1)
	}

	gs := gamelogic.NewGameState(username)

	// Subscribe to pause messages
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gs),
	)
	if err != nil {
		fmt.Printf("Failed to subscribe to pause: %v\n", err)
		os.Exit(1)
	}

	// Subscribe to move messages
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.SimpleQueueTransient,
		handlerMove(gs, conn),
	)
	if err != nil {
		fmt.Printf("Failed to subscribe to moves: %v\n", err)
		os.Exit(1)
	}

	// Subscribe to war messages (durable, shared queue)
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war",
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.SimpleQueueDurable,
		handlerWar(gs, conn),
	)
	if err != nil {
		fmt.Printf("Failed to subscribe to war: %v\n", err)
		os.Exit(1)
	}

	// REPL
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "move":
			move, err := gs.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
			ch, err := conn.Channel()
			if err != nil {
				fmt.Printf("Failed to open channel: %v\n", err)
				continue
			}
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
				move,
			)
			if err != nil {
				fmt.Printf("Failed to publish move: %v\n", err)
			} else {
				fmt.Printf("Moved to %s\n", move.ToLocation)
			}
		case "spawn":
			err := gs.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
			}
		case "spam":
			if len(words) < 2 {
				fmt.Println("usage: spam <count>")
				continue
			}
			n, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Printf("invalid count: %v\n", err)
				continue
			}
			ch, err := conn.Channel()
			if err != nil {
				fmt.Printf("Failed to open channel: %v\n", err)
				continue
			}
			for i := 0; i < n; i++ {
				log := routing.GameLog{
					CurrentTime: time.Now(),
					Message:     gamelogic.GetMaliciousLog(),
					Username:    username,
				}
				err = pubsub.PublishGob(
					ch,
					routing.ExchangePerilTopic,
					fmt.Sprintf("%s.%s", routing.GameLogSlug, username),
					log,
				)
				if err != nil {
					fmt.Printf("Failed to publish log: %v\n", err)
					break
				}
			}
			fmt.Printf("Published %d malicious logs\n", n)
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unknown command")
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, conn *amqp.Connection) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)
		switch outcome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			ch, err := conn.Channel()
			if err != nil {
				fmt.Printf("Failed to open channel for war: %v\n", err)
				return pubsub.NackRequeue
			}
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername()),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("Failed to publish war: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		return pubsub.NackDiscard
	}
}

func handlerWar(gs *gamelogic.GameState, conn *amqp.Connection) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		var logMsg string
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeYouWon:
			fmt.Printf("%s won the war against %s!\n", winner, loser)
			logMsg = fmt.Sprintf("%s won a war against %s", winner, loser)
		case gamelogic.WarOutcomeOpponentWon:
			fmt.Printf("%s won the war against %s!\n", winner, loser)
			logMsg = fmt.Sprintf("%s won a war against %s", winner, loser)
		case gamelogic.WarOutcomeDraw:
			fmt.Printf("The war between %s and %s was a draw!\n", winner, loser)
			logMsg = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
		default:
			fmt.Printf("Unknown war outcome: %v\n", outcome)
			return pubsub.NackDiscard
		}

		ch, err := conn.Channel()
		if err != nil {
			fmt.Printf("Failed to open channel for log: %v\n", err)
			return pubsub.NackRequeue
		}

		err = pubsub.PublishGob(
			ch,
			routing.ExchangePerilTopic,
			fmt.Sprintf("%s.%s", routing.GameLogSlug, rw.Attacker.Username),
			routing.GameLog{
				CurrentTime: time.Now(),
				Message:     logMsg,
				Username:    rw.Attacker.Username,
			},
		)
		if err != nil {
			fmt.Printf("Failed to publish log: %v\n", err)
			return pubsub.NackRequeue
		}

		return pubsub.Ack
	}
}