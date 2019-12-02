package kafka

import (
	"context"
	"github.com/feynman-go/workshop/client"
	"github.com/feynman-go/workshop/client/richclient"
	"github.com/feynman-go/workshop/health"
	"github.com/feynman-go/workshop/message"
	"github.com/feynman-go/workshop/mutex"
	"github.com/feynman-go/workshop/record"
	"github.com/feynman-go/workshop/syncrun/prob"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
	"time"
)

const (
	PartVarPartitionIDField = "kafkaPartId"
)

type SubscribeOption struct {
	Addr    []string
	Topic   *string
	GroupID *string
	PartitionID int64
	Records record.Factory
	Reporter *health.StatusReporter
	Limiter *rate.Limiter
}

func (option SubscribeOption) AddAddr(addr ...string) SubscribeOption {
	option.Addr = append(option.Addr, addr...)
	return option
}

func (option SubscribeOption) SetLimiter(limiter rate.Limit) SubscribeOption {
	option.Limiter = rate.NewLimiter(limiter, 1)
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

func (option SubscribeOption) SetRecords(records record.Factory) SubscribeOption {
	option.Records = records
	return option
}

func (option SubscribeOption) SetHealthReporter(reporter *health.StatusReporter) SubscribeOption {
	option.Reporter = reporter
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
	c 			 chan message.InputMessage
}

func NewKafkaSubscriber(option SubscribeOption) (*Subscriber, error) {
	readerCfg, err := completeSubscribeOption(option)
	if err != nil {
		return nil, err
	}
	reader := &Subscriber{
		mx:           new(mutex.Mutex),
		setupStatus:  0,
		readerConfig: readerCfg,
		opt:          option,
		c: 			  make(chan message.InputMessage, 1),
	}
	reader.pb = prob.New(reader.runLoop)
	reader.pb.Start()
	return reader, nil
}

func (s *Subscriber) Close() error {
	s.pb.Stop()
	return nil
}

func completeSubscribeOption(option SubscribeOption) (kafka.ReaderConfig, error) {
	if option.Topic == nil || *option.Topic == "" {
		return kafka.ReaderConfig{}, errors.New("empty topic")
	}

	readerCfg := kafka.ReaderConfig{
		Brokers: option.Addr,
	}

	if option.Topic != nil {
		readerCfg.Topic = *option.Topic
	}
	if option.GroupID != nil {
		readerCfg.GroupID = *option.GroupID
	}
	if option.PartitionID != 0 {
		readerCfg.Partition = int(option.PartitionID)
	}

	return readerCfg, nil
}

func (s *Subscriber) reportStatus(detail string, code health.StatusCode) {
	if s.opt.Reporter != nil {
		s.opt.Reporter.ReportStatus(detail, code)
	}
}

func (s *Subscriber) runLoop(ctx context.Context) {
	for {
		s.run(ctx)
		select {
		case <- ctx.Done():
		case <- time.After(3 * time.Second):
		}
	}
}

func (s *Subscriber) run(ctx context.Context) error {
	reader := s.newMessageReader()
	middles := s.getClientMiddles()
	clt := client.New(reader, client.Option{}.SetParallelCount(1).AddMiddle(middles...))
	defer func() {
		clt.CloseWithContext(ctx)
	}()

	s.reportStatus("up", health.StatusUp)

	var err error
	for ctx.Err() == nil {
		err = clt.Do(ctx, func(ctx context.Context, agent client.Agent) error {
			reader := agent.(*messageReader)
			input, err := reader.read(ctx)
			if err != nil {
				return err
			}
			select {
			case s.c <- input:
			case <- ctx.Done():
				return err
			}
			return nil
		}, client.ActionOption{}.SetName("receiveInput"))

		if err != nil {
			s.reportStatus(err.Error(), health.StatusAbnormal)
			break
		}
	}

	if err == nil {
		s.reportStatus("down", health.StatusDown)
	}

	return err
}

func (s *Subscriber) getClientMiddles() []client.DoMiddle {
	var mds = []client.DoMiddle{
		richclient.NewRecorderMiddle(s.opt.Records),
	}
	if limiter := s.opt.Limiter; limiter != nil {
		richclient.LimiterMiddle(limiter)
	}
	return mds
}

func (s *Subscriber) newMessageReader() *messageReader {
	config := s.readerConfig
	reader := kafka.NewReader(config)
	return &messageReader {
		subscriber: s,
		reader: reader,
	}
}

func (s *Subscriber) decodeMessage(kafkaMsg kafka.Message, ak committer) (message.InputMessage, error) {
	return message.InputMessage{
		Topic: kafkaMsg.Topic,
		Acker: ak,
		Message: message.Message{
			UID: string(kafkaMsg.Key),
			PayLoad: kafkaMsg.Value,
		},
	}, nil
}

type messageReader struct {
	subscriber *Subscriber
	reader    *kafka.Reader
}

func (reader *messageReader) read(ctx context.Context) (message.InputMessage, error) {
	msg, err := reader.reader.FetchMessage(ctx)
	if err != nil {
		return message.InputMessage{}, err
	}

	return reader.subscriber.decodeMessage(msg, committer{
		msg:    msg,
		reader: reader.reader,
	})
}

func (reader *messageReader) CloseWithContext(ctx context.Context) error {
	return reader.reader.Close()
}

type committer struct {
	msg    kafka.Message
	reader *kafka.Reader
}

func (ack committer) Commit(ctx context.Context) {
	ack.reader.CommitMessages(ctx, ack.msg)
}