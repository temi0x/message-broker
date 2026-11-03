package main

import (
	"fmt"
	"log"

	"message-broker/broker"
)

func main() {
	messageBroker := broker.NewBroker()
	publisher := broker.NewPublisher(messageBroker)

	for number := 1; number <= 5; number++ {
		publisher.Publish(fmt.Sprintf("message-%d", number))
	}

	subscriberOne := broker.NewSubscriber("subscriber-1")
	if err := messageBroker.Subscribe(subscriberOne); err != nil {
		log.Fatal(err)
	}

	for number := 6; number <= 10; number++ {
		publisher.Publish(fmt.Sprintf("message-%d", number))
	}

	subscriberTwo := broker.NewSubscriber("subscriber-2")
	if err := messageBroker.Subscribe(subscriberTwo); err != nil {
		log.Fatal(err)
	}

	printMessages(subscriberOne)
	printMessages(subscriberTwo)
}

func printMessages(subscriber *broker.Subscriber) {
	subscriber.Drain(func(message string) {
		fmt.Printf("[%s] %s\n", subscriber.Name(), message)
	})
}
