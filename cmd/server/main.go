package main

import (
	"fmt"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerLog() func(routing.GameLog) pubsub.AckType {
	return func(gl routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gl)
		if err != nil {
			fmt.Printf("Failed to write log: %v\n", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}

func main() {
	fmt.Println("Starting Peril server...")

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

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to open channel: %v\n", err)
		os.Exit(1)
	}

	err = ch.ExchangeDeclare(
		routing.ExchangePerilDirect,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("Failed to declare direct exchange: %v\n", err)
		os.Exit(1)
	}

	err = ch.ExchangeDeclare(
		routing.ExchangePerilTopic,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("Failed to declare topic exchange: %v\n", err)
		os.Exit(1)
	}

	err = ch.ExchangeDeclare(
		routing.ExchangePerilDeadLetter,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("Failed to declare dead letter exchange: %v\n", err)
		os.Exit(1)
	}

	gs := gamelogic.NewGameState("server")

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		fmt.Sprintf("%s.*", routing.GameLogSlug),
		pubsub.SimpleQueueDurable,
		handlerLog(),
	)
	if err != nil {
		fmt.Printf("Failed to subscribe to game logs: %v\n", err)
		os.Exit(1)
	}

	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true},
			)
			if err != nil {
				fmt.Printf("Failed to publish pause message: %v\n", err)
			}
		case "resume":
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false},
			)
			if err != nil {
				fmt.Printf("Failed to publish resume message: %v\n", err)
			}
		case "spawn":
			err = gs.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			_, err := gs.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintServerHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unknown command")
		}
	}
}
