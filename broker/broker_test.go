package broker_test

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"message-broker/broker"
)

func mustSubscribe(t *testing.T, messageBroker *broker.Broker, subscriber *broker.Subscriber) {
	t.Helper()
	if err := messageBroker.Subscribe(subscriber); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
}

func publishAll(publisher *broker.Publisher, messages ...string) {
	for _, message := range messages {
		publisher.Publish(message)
	}
}

func drain(subscriber *broker.Subscriber) []string {
	var messages []string
	subscriber.Drain(func(message string) {
		messages = append(messages, message)
	})
	return messages
}

func assertMessages(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("received %d messages %v; want %d %v", len(got), got, len(want), want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("message at index %d = %q; want %q (all received: %v)", index, got[index], want[index], got)
		}
	}
}

func TestSubscriberReadsMessagesInPublicationOrder(t *testing.T) {
	messageBroker := broker.NewBroker()
	publisher := broker.NewPublisher(messageBroker)
	subscriber := broker.NewSubscriber("audit")

	mustSubscribe(t, messageBroker, subscriber)
	publishAll(publisher, "created", "approved", "settled")

	assertMessages(t, drain(subscriber), []string{"created", "approved", "settled"})
}

func TestLateSubscriberReplaysAllMessages(t *testing.T) {
	messageBroker := broker.NewBroker()
	publisher := broker.NewPublisher(messageBroker)

	publishAll(publisher, "message-1", "message-2", "message-3", "message-4", "message-5")

	firstSubscriber := broker.NewSubscriber("first")
	mustSubscribe(t, messageBroker, firstSubscriber)

	publishAll(publisher, "message-6", "message-7", "message-8", "message-9", "message-10")

	secondSubscriber := broker.NewSubscriber("second")
	mustSubscribe(t, messageBroker, secondSubscriber)

	want := []string{
		"message-1", "message-2", "message-3", "message-4", "message-5",
		"message-6", "message-7", "message-8", "message-9", "message-10",
	}
	assertMessages(t, drain(firstSubscriber), want)
	assertMessages(t, drain(secondSubscriber), want)
}

func TestSubscribeIsIdempotent(t *testing.T) {
	messageBroker := broker.NewBroker()
	publisher := broker.NewPublisher(messageBroker)
	subscriber := broker.NewSubscriber("audit")

	publishAll(publisher, "message-1", "message-2")
	mustSubscribe(t, messageBroker, subscriber)

	first, available := subscriber.ReadNext()
	if !available || first != "message-1" {
		t.Fatalf("first ReadNext() = (%q, %t); want (message-1, true)", first, available)
	}

	mustSubscribe(t, messageBroker, subscriber)

	second, available := subscriber.ReadNext()
	if !available || second != "message-2" {
		t.Fatalf("read after duplicate subscription = (%q, %t); want (message-2, true)", second, available)
	}
}

func TestSubscribersHaveIndependentCursors(t *testing.T) {
	messageBroker := broker.NewBroker()
	publisher := broker.NewPublisher(messageBroker)
	firstSubscriber := broker.NewSubscriber("first")
	secondSubscriber := broker.NewSubscriber("second")

	publishAll(publisher, "message-1", "message-2", "message-3")
	mustSubscribe(t, messageBroker, firstSubscriber)
	mustSubscribe(t, messageBroker, secondSubscriber)

	assertMessages(t, drain(firstSubscriber), []string{"message-1", "message-2", "message-3"})

	message, available := secondSubscriber.ReadNext()
	if !available || message != "message-1" {
		t.Fatalf("second subscriber first message = (%q, %t); want (message-1, true)", message, available)
	}
}

func TestMessageIsNotReadTwiceByOneSubscriber(t *testing.T) {
	messageBroker := broker.NewBroker()
	publisher := broker.NewPublisher(messageBroker)
	subscriber := broker.NewSubscriber("audit")

	mustSubscribe(t, messageBroker, subscriber)
	publishAll(publisher, "message-1")

	message, available := subscriber.ReadNext()
	if !available || message != "message-1" {
		t.Fatalf("first ReadNext() = (%q, %t); want (message-1, true)", message, available)
	}

	message, available = subscriber.ReadNext()
	if available {
		t.Fatalf("second ReadNext() = (%q, true); want unavailable", message)
	}
}

func TestMultipleSubscribersReceiveSameOrderedLog(t *testing.T) {
	messageBroker := broker.NewBroker()
	publisher := broker.NewPublisher(messageBroker)
	firstSubscriber := broker.NewSubscriber("first")
	secondSubscriber := broker.NewSubscriber("second")

	mustSubscribe(t, messageBroker, firstSubscriber)
	mustSubscribe(t, messageBroker, secondSubscriber)
	publishAll(publisher, "created", "approved", "settled")

	want := []string{"created", "approved", "settled"}
	assertMessages(t, drain(firstSubscriber), want)
	assertMessages(t, drain(secondSubscriber), want)
}

func TestSubscriberCanBelongToOnlyOneBroker(t *testing.T) {
	subscriber := broker.NewSubscriber("audit")
	firstBroker := broker.NewBroker()
	secondBroker := broker.NewBroker()

	mustSubscribe(t, firstBroker, subscriber)
	if err := secondBroker.Subscribe(subscriber); err != broker.ErrSubscriberAlreadySubscribed {
		t.Fatalf("Subscribe() error = %v; want ErrSubscriberAlreadySubscribed", err)
	}
}

func TestNilSubscriberIsRejected(t *testing.T) {
	messageBroker := broker.NewBroker()
	if err := messageBroker.Subscribe(nil); err != broker.ErrNilSubscriber {
		t.Fatalf("Subscribe(nil) error = %v; want ErrNilSubscriber", err)
	}
}

func TestConcurrentPublishesPreserveAllMessages(t *testing.T) {
	const (
		publisherCount       = 8
		messagesPerPublisher = 250
	)

	messageBroker := broker.NewBroker()
	publisher := broker.NewPublisher(messageBroker)

	var publishers sync.WaitGroup
	for publisherID := 0; publisherID < publisherCount; publisherID++ {
		publishers.Add(1)
		go func(publisherID int) {
			defer publishers.Done()
			for messageID := 0; messageID < messagesPerPublisher; messageID++ {
				publisher.Publish(fmt.Sprintf("publisher-%d-message-%03d", publisherID, messageID))
			}
		}(publisherID)
	}
	publishers.Wait()

	subscriber := broker.NewSubscriber("audit")
	mustSubscribe(t, messageBroker, subscriber)
	messages := drain(subscriber)

	wantCount := publisherCount * messagesPerPublisher
	if len(messages) != wantCount {
		t.Fatalf("received %d messages; want %d", len(messages), wantCount)
	}
	if messageBroker.MessageCount() != wantCount {
		t.Fatalf("MessageCount() = %d; want %d", messageBroker.MessageCount(), wantCount)
	}

	sort.Strings(messages)
	for publisherID := 0; publisherID < publisherCount; publisherID++ {
		for messageID := 0; messageID < messagesPerPublisher; messageID++ {
			want := fmt.Sprintf("publisher-%d-message-%03d", publisherID, messageID)
			index := sort.SearchStrings(messages, want)
			if index == len(messages) || messages[index] != want {
				t.Fatalf("missing %q", want)
			}
		}
	}
}

func TestConcurrentSubscribersRemainIndependent(t *testing.T) {
	const messageCount = 500

	messageBroker := broker.NewBroker()
	publisher := broker.NewPublisher(messageBroker)
	for messageID := 0; messageID < messageCount; messageID++ {
		publisher.Publish(fmt.Sprintf("message-%03d", messageID))
	}

	firstSubscriber := broker.NewSubscriber("first")
	secondSubscriber := broker.NewSubscriber("second")
	mustSubscribe(t, messageBroker, firstSubscriber)
	mustSubscribe(t, messageBroker, secondSubscriber)

	var (
		readers sync.WaitGroup
		first   []string
		second  []string
	)
	readers.Add(2)
	go func() {
		defer readers.Done()
		first = drain(firstSubscriber)
	}()
	go func() {
		defer readers.Done()
		second = drain(secondSubscriber)
	}()
	readers.Wait()

	want := make([]string, messageCount)
	for messageID := range want {
		want[messageID] = fmt.Sprintf("message-%03d", messageID)
	}
	assertMessages(t, first, want)
	assertMessages(t, second, want)
}

func TestConcurrentDrainsProcessMessagesInOrder(t *testing.T) {
	messageBroker := broker.NewBroker()
	publisher := broker.NewPublisher(messageBroker)
	subscriber := broker.NewSubscriber("audit")

	mustSubscribe(t, messageBroker, subscriber)
	publishAll(publisher, "message-1", "message-2")

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondProcessed := make(chan struct{})
	var (
		readers   sync.WaitGroup
		processed []string
		mu        sync.Mutex
	)

	readers.Add(1)
	go func() {
		defer readers.Done()
		subscriber.Drain(func(message string) {
			if message == "message-1" {
				close(firstStarted)
				<-releaseFirst
			}
			mu.Lock()
			processed = append(processed, message)
			mu.Unlock()
		})
	}()

	<-firstStarted
	readers.Add(1)
	go func() {
		defer readers.Done()
		subscriber.Drain(func(message string) {
			mu.Lock()
			processed = append(processed, message)
			mu.Unlock()
			close(secondProcessed)
		})
	}()

	select {
	case <-secondProcessed:
		t.Fatal("second drain processed a message before the first drain completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseFirst)
	readers.Wait()
	assertMessages(t, processed, []string{"message-1", "message-2"})
}
