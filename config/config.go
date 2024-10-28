package config

import (
	"flag"
	"strings"
)

var (
	kafkaServer          = flag.String("KAFKA_SERVERS", "localhost", "")
	kafkaGroupId         = flag.String("KAFKA_GROUP_ID", "myGroup", "")
	kafkaConsumerTopics  = flag.String("KAFKA_CONSUMER_TOPICS", "input_topic", "")
	kafkaProducerTopic   = flag.String("KAFKA_CONSUMER_TOPIC", "output_topic", "")
	kafkaOffsetReset     = flag.String("KAFKA_OFFSET_RESET", "earliest", "")
	kafkaAutoOffsetStore = flag.Bool("KAFKA_AUTO_OFFSET_STORE", false, "")
	workerPool           = flag.Int64("WORKER_POOL", 6, "")
	tasksToPick          = flag.Uint("TASKS_PICK_UP", 2, "")
	consumersCount       = flag.Int("CONSUMERS_COUNT", 2, "")
	producersCount       = flag.Int("PRODUCERS_COUNT", 2, "")
)

type Consumer struct {
	Servers               string
	GroupId               string
	Topics                []string
	AutoOffsetReset       string
	EnableAutoOffsetStore bool
}

type Producer struct {
	Servers string
	Topic   string
}

type Scheduler struct {
	TasksBuffer      int64
	CountTasksToPick uint
}

type Config struct {
	ConsumersCount int
	Consumer       *Consumer
	ProducersCount int
	Producer       *Producer
	Scheduler      *Scheduler
}

func NewConfig() *Config {
	return &Config{
		ConsumersCount: *consumersCount,
		Consumer: &Consumer{
			Servers:               *kafkaServer,
			GroupId:               *kafkaGroupId,
			Topics:                strings.Split(*kafkaConsumerTopics, " "),
			AutoOffsetReset:       *kafkaOffsetReset,
			EnableAutoOffsetStore: *kafkaAutoOffsetStore,
		},
		ProducersCount: *producersCount,
		Producer: &Producer{
			Servers: *kafkaServer,
			Topic:   *kafkaProducerTopic,
		},
		Scheduler: &Scheduler{
			TasksBuffer:      *workerPool,
			CountTasksToPick: *tasksToPick,
		},
	}
}
