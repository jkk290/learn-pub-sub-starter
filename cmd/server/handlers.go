package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerLogs() func(gl routing.GameLog) pubsub.Acktype {
	return func(gl routing.GameLog) pubsub.Acktype {
		defer fmt.Print("> ")

		if err := gamelogic.WriteLog(gl); err != nil {
			fmt.Printf("error writing log: %v", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}

}
