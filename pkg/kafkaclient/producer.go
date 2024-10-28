package kafkaclient

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Producer interface {
	Connect() error
	Produce(*kafka.Message) error
}

type KafkaProducer struct {
	conn                  *kafka.Producer
	Servers               string
	GroupId               string
	Topic                 string
	AutoOffsetReset       string
	EnableAutoOffsetStore bool
}

func (p *KafkaProducer) Connect() error {
	var err error
	p.conn, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": p.Servers})
	return err
}

func (p *KafkaProducer) Produce(m *kafka.Message) error {
	err := p.conn.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.Topic, Partition: kafka.PartitionAny},
		Value:          m.Value,
		Headers:        m.Headers,
	}, nil)
	return err
}
