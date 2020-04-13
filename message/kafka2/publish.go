package kafka2

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/feynman-go/workshop/client"
	"github.com/pkg/errors"
	"log"
	"time"
)

type Publisher struct {
	clt *client.Client
}

func NewKafkaPublisher(addr []string, topic string, config sarama.Config) (*Publisher, error) {
	agent, err := newPublishAgent(addr, topic, config)
	if err != nil {
		return nil, err
	}
	clt := client.New(agent, client.Option{}.SetParallelCount(1))
	return &Publisher{
		clt: clt,
	}, nil
}

func (publisher *Publisher) Publish(ctx context.Context, messages []*sarama.ProducerMessage) error {
	if len(messages) == 0 {
		return nil
	}
	err := publisher.clt.Do(ctx, func(ctx context.Context, agent client.Agent) error {
		pub := agent.(*publishAgent)
		err := pub.publishMessage(ctx, messages)
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

func (publisher *Publisher) Close() error {
	return publisher.clt.CloseWithContext(context.Background())
}

func newPublishAgent(addr []string, topic string, config sarama.Config) (*publishAgent, error) {
	agent := &publishAgent{
		closed: false,
		addr:   addr,
		config: &config,
		topic:  topic,
	}
	err := agent.refreshProducer()
	if err != nil {
		return nil, err
	}
	return agent, nil
}

type publishAgent struct {
	closed   bool
	producer sarama.SyncProducer
	addr     []string
	config   *sarama.Config
	cn       chan struct{}
	topic    string
}

func (agent *publishAgent) refreshProducer() error {
	producer, err := sarama.NewSyncProducer(agent.addr, agent.config)
	if err != nil {
		return err
	}

	agent.producer = producer
	agent.cn = make(chan struct{})
	return err
}

func (agent *publishAgent) publishMessage(ctx context.Context, msg []*sarama.ProducerMessage) error {
	if agent.producer == nil {
		return errors.New("empty producer")
	}
	return agent.producer.SendMessages(msg)
}

func (agent *publishAgent) Reset(ctx context.Context) error {
	if agent.closed {
		return errors.New("closed")
	}
	if agent.producer != nil {
		agent.producer.Close()
		close(agent.cn)
		agent.producer = nil
	}
	var err error
	err = agent.refreshProducer()
	if err != nil {
		log.Println("reset publish agent err")
	}
	return err
}

func (agent *publishAgent) CloseWithContext(ctx context.Context) error {
	if agent.closed {
		return errors.New("closed")
	}
	if agent.producer != nil {
		err := agent.producer.Close()
		if err != nil {
			return err
		}
		agent.producer = nil
		close(agent.cn)
	}
	agent.closed = true
	return nil
}

func NewKafkaProducerConfig(cfg KafkaPublishConfig) (*sarama.Config, error) {
	mqConfig := sarama.NewConfig()

	if cfg.Username != "" && cfg.Password != "" {
		if !cfg.SASLEnable {
			mqConfig.Net.SASL.Enable = false
		} else {
			mqConfig.Net.SASL.Enable = true
		}
		mqConfig.Net.SASL.User = cfg.Username
		mqConfig.Net.SASL.Password = cfg.Password
		mqConfig.Net.SASL.Handshake = true
	}
	mqConfig.Producer.Return.Successes = true
	mqConfig.Producer.RequiredAcks = sarama.WaitForLocal
	mqConfig.Producer.Timeout = time.Second

	if err := mqConfig.Validate(); err != nil {
		log.Println("Kafka common producer config invalidate", err)
		return nil, err
	}

	return mqConfig, nil
}

type KafkaPublishConfig struct {
	Topic      string
	ServerAddr []string
	Username   string
	Password   string
	SASLEnable bool
}
