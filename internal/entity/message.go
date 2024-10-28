package entity

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const Valid string = "valid"
const Invalid string = "invalid"

type Object struct {
	Id        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Data      string `json:"data"`
	Status    string `json:"status"`
}

type Message struct {
	Msg   *kafka.Message
	Error error
}

func NewMessage(msg *kafka.Message) *Message {
	return &Message{
		Msg: msg,
	}
}

func (m *Message) Validate() (interface{}, error) {
	defer func() {
		r := recover()
		m.Error = fmt.Errorf("recovered from panic %v", r)
	}()

	var msg Object
	if err := json.Unmarshal(m.Msg.Value, &msg); err != nil {
		return m, fmt.Errorf("unable to unmarshal json %v", err)
	}

	t, err := time.Parse(time.RFC3339, msg.Timestamp)
	if err != nil {
		m.Error = err
		return m, fmt.Errorf("can't parse timestamp %v", err)
	}

	now := time.Now()
	res := now.UTC().UnixMicro() - t.UTC().UnixMicro()
	if res > 86400000000 {
		m.Error = err
		return m, fmt.Errorf("message age is more than 24 hours")
	}

	if len(msg.Data) > 10 {
		msg.Status = Valid
	} else {
		msg.Status = Invalid
	}

	val, err := json.Marshal(&msg)
	if err != nil {
		return m, fmt.Errorf("unable to unmarshal json %v", err)
	}

	m.Msg.Value = val

	return m, nil
}
