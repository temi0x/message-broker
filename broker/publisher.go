package broker

type Publisher struct {
	broker *Broker
}

func NewPublisher(broker *Broker) *Publisher {
	return &Publisher{broker: broker}
}

func (p *Publisher) Publish(message string) {
	p.broker.Publish(message)
}
