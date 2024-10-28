package usecase

import (
	"context"
	"os"

	"github.com/igor35hh/scheduler"

	"github.com/igor35hh/processor/config"
	"github.com/igor35hh/processor/internal/entity"
	"github.com/igor35hh/processor/pkg/kafkaclient"
	lg "github.com/igor35hh/processor/pkg/logger"
)

type Processor interface {
	ConsumeMessages(ctx context.Context)
	ProduceMessages(ctx context.Context)
}

type Service struct {
	scheduler scheduler.Scheduler
	consumer  kafkaclient.Consumer
	producer  kafkaclient.Producer
	log       lg.Logger
}

func NewService(conf *config.Config, log lg.Logger, sch scheduler.Scheduler) Processor {
	return &Service{
		scheduler: sch,
		log:       log,
		consumer: &kafkaclient.KafkaConsumer{
			Servers:               conf.Consumer.Servers,
			GroupId:               conf.Consumer.GroupId,
			Topics:                conf.Consumer.Topics,
			AutoOffsetReset:       conf.Consumer.AutoOffsetReset,
			EnableAutoOffsetStore: conf.Consumer.EnableAutoOffsetStore,
		},
		producer: &kafkaclient.KafkaProducer{
			Servers: conf.Producer.Servers,
			Topic:   conf.Producer.Topic,
		},
	}
}

func (s *Service) ConsumeMessages(ctx context.Context) {
	if err := s.consumer.Connect(); err != nil {
		s.log.Error("consumer can't connect to kafka", err)
		os.Exit(1)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			m, err := s.consumer.ReadMessage()
			if err != nil {
				s.log.Error("error consuming message form kafka", err)
			}
			msg := entity.NewMessage(m)
			s.scheduler.Schedule(msg.Validate)
		}
	}
}

func (s *Service) ProduceMessages(ctx context.Context) {
	if err := s.producer.Connect(); err != nil {
		s.log.Error("producer can't connect to kafka", err)
		os.Exit(1)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if task := s.scheduler.GetReady(); task != nil {
				if m, ok := task.(*entity.Message); ok {
					if m.Error != nil {
						s.log.Error("error message processing", m.Error)
						continue
					}
					if err := s.producer.Produce(m.Msg); err != nil {
						s.log.Error("error to produce message to kafka", err)
						continue
					}
					if err := s.consumer.StoreMessage(m.Msg); err != nil {
						s.log.Error("error to store message to kafka", err)
					}
				}
			}
		}
	}
}
