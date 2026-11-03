// Package broker implements an in-memory publish-subscribe broker.
package broker

import (
	"errors"
	"sync"
)

var (
	ErrNilSubscriber = errors.New("broker: subscriber is nil")

	ErrSubscriberAlreadySubscribed = errors.New("broker: subscriber already belongs to another broker")
)

type Broker struct {
	mu          sync.Mutex
	messages    []string
	subscribers map[*Subscriber]struct{}
}

func NewBroker() *Broker {
	return &Broker{subscribers: make(map[*Subscriber]struct{})}
}

// Publish a new message.
func (b *Broker) Publish(message string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.messages = append(b.messages, message)
}

// Register a subscriber
func (b *Broker) Subscribe(subscriber *Subscriber) error {
	if subscriber == nil {
		return ErrNilSubscriber
	}

	subscriber.mu.Lock()
	defer subscriber.mu.Unlock()

	if subscriber.broker != nil {
		if subscriber.broker == b {
			return nil
		}
		return ErrSubscriberAlreadySubscribed
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	subscriber.broker = b
	subscriber.nextOffset = 0
	b.subscribers[subscriber] = struct{}{}
	return nil
}

func (b *Broker) messageFor(subscriber *Subscriber) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, subscribed := b.subscribers[subscriber]; !subscribed || subscriber.nextOffset == len(b.messages) {
		return "", false
	}

	message := b.messages[subscriber.nextOffset]
	subscriber.nextOffset++
	return message, true
}

// Return the number of stored messages.
func (b *Broker) MessageCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.messages)
}
