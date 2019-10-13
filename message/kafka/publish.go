package kafka

import (
	"context"
	"github.com/feynman-go/workshop/client"
	"github.com/feynman-go/workshop/message"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

type WriteConfig = kafka.WriterConfig

type PublisherOption struct {
	WriteConfig WriteConfig
}

type Publisher struct {
	clt *client.Client
	opt PublisherOption
}

func NewPublisher(option PublisherOption) *Publisher {
	agent := newPublishAgent(option)
	clt := client.New(agent, client.Option{}.SetParallelCount(1))
	return &Publisher{
		clt: clt,
	}
}

func (publisher *Publisher) Publish(ctx context.Context, messages []message.OutputMessage) error {
	if len(messages) == 0 {
		return nil
	}
	err := publisher.clt.Do(ctx, func(ctx context.Context, agent client.Agent) error {
		pub := agent.(*publishAgent)
		ms := []kafka.Message{}
		for _, m := range messages {
			ms = append(ms, kafka.Message {
				Topic: m.Topic,
				Key: []byte(m.UID),
				Value: m.PayLoad,
				Partition: int(m.Partition),
			})
		}
		err := pub.publishMessage(ctx, ms)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func newKafkaWriter(opt PublisherOption) *kafka.Writer {
	wConfig := opt.WriteConfig
	return kafka.NewWriter(wConfig)
}

func newPublishAgent(option PublisherOption) *publishAgent {
	return &publishAgent{
		closed: false,
		opt: option,
		writer: newKafkaWriter(option),
	}
}

type publishAgent struct {
	closed bool
	opt PublisherOption
	writer *kafka.Writer
}

func (agent *publishAgent) publishMessage(ctx context.Context, msg []kafka.Message) error {
	return agent.writer.WriteMessages(ctx, msg...)
}

func (agent *publishAgent) Reset(ctx context.Context) error {
	if agent.closed {
		return errors.New("closed")
	}
	if agent.writer != nil {
		agent.writer.Close()
		agent.writer = nil
	}
	agent.writer = newKafkaWriter(agent.opt)
	return nil
}

func (agent *publishAgent) CloseWithContext(ctx context.Context) error {
	if agent.closed {
		return errors.New("closed")
	}
	if agent.writer != nil {
		err := agent.writer.Close()
		if err != nil {
			return err
		}
		agent.writer = nil
	}
	agent.closed = true
	return nil
}

