package kafka

import (
	"context"
	"github.com/feynman-go/workshop/client"
	"github.com/feynman-go/workshop/message"
	"github.com/feynman-go/workshop/mutex"
	"github.com/feynman-go/workshop/syncrun/prob"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

const (
	PartVarPartitionIDField = "kafkaPartId"
)

type SubscribeOption struct {
	Addr    []string
	Topic   *string
	GroupID *string
	PartitionID int64
}

func (option SubscribeOption) AddAddr(addr ...string) SubscribeOption {
	option.Addr = append(option.Addr, addr...)
	return option
}

func (option SubscribeOption) SetGroupID(groupID string) SubscribeOption {
	option.GroupID = &groupID
	return option
}

func (option SubscribeOption) SetTopic(topic string) SubscribeOption {
	option.Topic = &topic
	return option
}

type Decoder interface {
	Decode(message kafka.Message) (message.Message, error)
}

type Subscriber struct {
	setupStatus  int32
	mx           *mutex.Mutex
	readerConfig kafka.ReaderConfig
	opt          SubscribeOption
	pb           *prob.Prob
	outputChan   chan message.InputMessage
}

func NewKafkaSubscriber(option SubscribeOption) (*Subscriber, error) {
	option = completeSubscribeOption(option)
	readerCfg := kafka.ReaderConfig{
		Brokers: option.Addr,
	}

	if option.Topic == nil || *option.Topic == "" {
		return nil, errors.New("empty topic")
	}

	if option.GroupID != nil {
		readerCfg.GroupID = *option.GroupID
	}

	reader := &Subscriber{
		mx:           new(mutex.Mutex),
		setupStatus:  0,
		readerConfig: readerCfg,
		opt:          option,
		outputChan:   make(chan message.InputMessage, 1),
	}
	reader.pb = prob.New(reader.runNotify)
	reader.pb.Start()
	return reader, nil
}

func (s *Subscriber) Close() error {
	s.pb.Stop()
	return nil
}

func completeSubscribeOption(option SubscribeOption) SubscribeOption {
	return option
}


func (s *Subscriber) runNotify(ctx context.Context) {
	var partId = s.opt.PartitionID

	agent, err := s.newPartReader(ctx, partId)
	if err != nil {
		return
	}

	clt := client.New(agent, client.Option{}.SetParallelCount(1))

	for ctx.Err() == nil {
		err = clt.Do(ctx, func(ctx context.Context, a client.Agent) error {
			reader := a.(*partReader)
			input, err := reader.read(ctx)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case s.outputChan <- input:
				return nil
			}
		})
	}

	return
}

func (s *Subscriber) Read(ctx context.Context) (message.InputMessage, error) {
	select {
	case <-ctx.Done():
		return message.InputMessage{}, ctx.Err()
	case input := <-s.outputChan:
		return input, nil
	}
}

func (s *Subscriber) newPartReader(ctx context.Context, part int64) (*partReader, error) {
	pr := &partReader{
		partition: part,
		sb:        s,
	}
	err := pr.Reset(ctx)
	if err != nil {
		return nil, err
	}
	return pr, nil
}

func (s *Subscriber) newKafkaReader(partition int64) *kafka.Reader {
	config := s.readerConfig
	config.Partition = int(partition)
	reader := kafka.NewReader(config)
	return reader
}

func (s *Subscriber) decodeMessage(kafkaMsg kafka.Message, ak acker) (message.InputMessage, error) {
	return message.InputMessage{
		Topic: kafkaMsg.Topic,
		Acker: ak,
		Message: message.Message{
			UID: string(kafkaMsg.Key),
			PayLoad: kafkaMsg.Value,
		},
	}, nil
}

type partReader struct {
	partition int64
	sb        *Subscriber
	reader    *kafka.Reader
}

func (reader *partReader) read(ctx context.Context) (message.InputMessage, error) {
	msg, err := reader.reader.FetchMessage(ctx)
	if err != nil {
		return message.InputMessage{}, err
	}

	return reader.sb.decodeMessage(msg, acker{
		msg:    msg,
		reader: reader.reader,
	})
}

func (reader *partReader) Reset(ctx context.Context) error {
	if reader.reader != nil {
		reader.reader.Close()
		reader.reader = nil
	}
	reader.reader = reader.sb.newKafkaReader(reader.partition)
	return nil
}

func (reader *partReader) Close(ctx context.Context) error {
	return reader.reader.Close()
}

type acker struct {
	msg    kafka.Message
	reader *kafka.Reader
}

func (ack acker) Ack(ctx context.Context) {
	ack.reader.CommitMessages(ctx, ack.msg)
}