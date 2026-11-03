package broker

import "sync"

type Subscriber struct {
	mu         sync.Mutex
	drainMu    sync.Mutex
	name       string
	broker     *Broker
	nextOffset int
}

// Create a subscriber.
func NewSubscriber(name string) *Subscriber {
	return &Subscriber{name: name}
}

func (s *Subscriber) Name() string {
	return s.name
}

// Return the next available message.
func (s *Subscriber) ReadNext() (message string, available bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.broker == nil {
		return "", false
	}
	return s.broker.messageFor(s)
}

// Consume available messages.
func (s *Subscriber) Drain(consume func(string)) int {
	s.drainMu.Lock()
	defer s.drainMu.Unlock()

	consumed := 0
	for {
		message, available := s.ReadNext()
		if !available {
			return consumed
		}
		consume(message)
		consumed++
	}
}
