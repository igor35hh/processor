package kafkaclient

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Consumer interface {
	Connect() error
	ReadMessage() (*kafka.Message, error)
	StoreMessage(*kafka.Message) error
}

type KafkaConsumer struct {
	conn                  *kafka.Consumer
	Servers               string
	GroupId               string
	Topics                []string
	AutoOffsetReset       string
	EnableAutoOffsetStore bool
}

func (c *KafkaConsumer) Connect() error {
	var err error
	c.conn, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        c.Servers,
		"group.id":                 c.GroupId,
		"auto.offset.reset":        c.AutoOffsetReset,
		"enable.auto.offset.store": c.EnableAutoOffsetStore,
	})
	if err != nil {
		return err
	}

	err = c.conn.SubscribeTopics(c.Topics, nil)

	return err
}

func (c *KafkaConsumer) ReadMessage() (*kafka.Message, error) {
	return c.conn.ReadMessage(-1)
}

func (c *KafkaConsumer) StoreMessage(m *kafka.Message) error {
	var err error
	_, err = c.conn.StoreMessage(m)
	return err
}
