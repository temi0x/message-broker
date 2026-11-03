# An In-Memory Message Broker

A Go implementation of a simple publish-subscribe message broker.

## Prerequisite

Go 1.22 or later.

## Run the demo

```sh
go run ./cmd/demo
```

The output has 20 message lines: `subscriber-1` receives `message-1` through
`message-10`, followed by the same ordered sequence for `subscriber-2`.

## Validation and Tests

```sh
gofmt -w broker cmd
go vet ./...
go test ./...
go test -race ./...
```

## Design

The broker holds one append-only message log. Every subscriber has a private
cursor—the index of its next unread message.

- Publishing appends to the shared log.
- A new subscriber starts at cursor `0`, so it can read messages published
  before it subscribed.
- A read returns the message at that subscriber's cursor and advances only that
  cursor. A subscriber therefore reads messages in order and at most once.
- Moving one cursor does not affect another, so subscribers consume
  independently.
- A mutex protects the log, registrations, and cursor changes against
  concurrent access.

## Behaviours

| Area | Behaviour |
|---|---|
| Delivery | At-most-once per subscriber: the cursor advances before a message is returned. |
| Ordering | A subscriber receives messages in append/publication order. |
| Retention | All messages stay in memory for the process lifetime. |
| Read behaviour | `ReadNext` is non-blocking; it returns `available=false` when caught up. |
