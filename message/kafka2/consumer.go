package kafka2

import (
	"context"
	"errors"
	"fmt"
	"github.com/feynman-go/workshop/health"
	"github.com/feynman-go/workshop/randtime"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/routine"
	"go.uber.org/zap"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

type MessageConsumer struct {
	DeadTopic *string
	option ConsumerOption
	rt *routine.Routine
	reporter *health.StatusReporter
	partitionConsumer PartitionConsumer
	deadQueue DeadQueue
}

func NewMessageConsumer(partitionConsumer PartitionConsumer, option ConsumerOption) *MessageConsumer {
	consumer := &MessageConsumer{
		DeadTopic: nil,
		option:    option,
		rt:        nil,
		reporter:  option.Reporter,
		deadQueue: nil,
		partitionConsumer: partitionConsumer,
	}

	consumer.rt = routine.New(consumer.run)
	return consumer
}

type ConsumerOption struct {
	SaramConfig   sarama.Config
	DeadMessage   DeadMessageOption
	ExecPoolOption ExecPoolOption
	ConsumeTopics []string
	Brokers       []string
	GroupID       string
	Reporter 	  *health.StatusReporter
}

type DeadMessageOption struct {
	Topic string
}

type ExecPoolOption struct {
	PoolSize int32
}

func (mc *MessageConsumer) Start() {
	mc.rt.Start()
}

func (mc *MessageConsumer) CloseWithContext(ctx context.Context) error {
	return mc.rt.StopAndWait(ctx)
}

func (mc *MessageConsumer) updateHealthStatus(status health.Status) {
	if mc.reporter != nil {
		mc.reporter.ReportStatus("", status...)
	}
}

func (mc *MessageConsumer) run(ctx context.Context) {
	mc.updateHealthStatus(health.Status{health.StatusUnknown})
	defer func() {
		if mc.deadQueue != nil {
			mc.deadQueue.Close()
		}
	}()

	for ctx.Err() == nil {
		err := mc.runConsumer(ctx)
		if err != nil {
			zap.L().Error("run consumer err", zap.Error(err))
			mc.updateHealthStatus(health.Status{health.StatusFatal})
		}
		select {
		case <- ctx.Done():
			mc.updateHealthStatus(health.Status{health.StatusDown})
			return
		case <- time.After(randtime.RandDuration(3 * time.Second, 5 * time.Second)):
		}
	}
	mc.updateHealthStatus(health.Status{health.StatusDown})
}

func (mc *MessageConsumer) runConsumer(ctx context.Context) error{
	group, err := sarama.NewConsumerGroup(mc.option.Brokers, mc.option.GroupID, &mc.option.SaramConfig)
	if err != nil {
		return fmt.Errorf("new consumerApply group: %w", err)
	}
	defer group.Close()
	c := &consumerApply{
		handlers: mc.partitionConsumer,
		manager: mc,
		deadQueue: mc.deadQueue,
	}

	var consumerErr error
	var clientErr error

	syncrun.RunAsGroup(ctx, func(ctx context.Context) {
		log.Println("MessageConsumer ready")
		if err := group.Consume(ctx, mc.option.ConsumeTopics, c); err != nil {
			consumerErr = err
		}
	}, func(ctx context.Context) {
		select {
		case err := <- group.Errors():
			if err != nil {
				clientErr = err
			}
		case <- ctx.Done():
		}
	})

	log.Println("MessageConsumer consume finished", consumerErr, clientErr)
	if clientErr != nil {
		mc.updateHealthStatus(health.Status{health.StatusAbnormal})
		return clientErr
	}

	if consumerErr != nil && errors.Is(consumerErr, context.DeadlineExceeded) || errors.Is(consumerErr, context.Canceled) {
		mc.updateHealthStatus(health.Status{health.StatusAbnormal})
		return consumerErr
	}

	return nil
}

// consumerApply represents a Sarama consumerApply group consumerApply
type consumerApply struct {
	handlers PartitionConsumer
	manager *MessageConsumer
	deadQueue DeadQueue
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *consumerApply) Setup(sarama.ConsumerGroupSession) error {
	consumer.manager.updateHealthStatus(health.Status{health.StatusUp})
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *consumerApply) Cleanup(sarama.ConsumerGroupSession) error {
	consumer.manager.updateHealthStatus(health.Status{health.StatusDown})
	return nil
}

// ConsumeClaim must start a consumerApply loop of ConsumerGroupClaim's Messages().
func (consumer *consumerApply) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	q := consumer.newConsumerQueue(session, claim)
	zap.L().Info("start run partition consumer",
		zap.Any("topic", claim.Topic()),
		zap.Any("partition", claim.Partition()),
	)
	err := consumer.handlers.RunPartitionConsumer(q)
	if err != nil {
		return err
	}
	return nil
}

func (consumer *consumerApply) newConsumerQueue(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) *ConsumerQueue {
	ctx, cancel := context.WithCancel(session.Context())
	return &ConsumerQueue{
		claim: claim,
		session: session,
		deadQueue: consumer.deadQueue,
		ctx: ctx,
		cancel: cancel,
	}
}

func (consumer *consumerApply) sendDeadMessage(ctx context.Context, message *sarama.ConsumerMessage, err error) {
	if consumer.deadQueue != nil {
		consumer.deadQueue.SendDeadMessage(ctx, message, err)
	}
}

type PartitionConsumer interface {
	RunPartitionConsumer(queue *ConsumerQueue) error
}

type RunConsumerFunc func(queue *ConsumerQueue) error

func (f RunConsumerFunc) RunPartitionConsumer(queue *ConsumerQueue) error{return f(queue)}

type ConsumerQueue struct {
	last *sarama.ConsumerMessage
	claim sarama.ConsumerGroupClaim
	session sarama.ConsumerGroupSession
	deadQueue DeadQueue
	ctx context.Context
	cancel func()
}

func (queue *ConsumerQueue) Context() context.Context {
	return queue.ctx
}

func (queue *ConsumerQueue) Close() error {
	queue.cancel()
	return nil
}

func (queue *ConsumerQueue) Partition() int32 {
	return queue.claim.Partition()
}

func (queue *ConsumerQueue) Topic() string {
	return queue.claim.Topic()
}

var ErrClaimClosed = errors.New("ErrClaimClosed")

func (queue *ConsumerQueue) Pull(ctx context.Context) (*sarama.ConsumerMessage, error) {
	select {
	case <- queue.ctx.Done():
		return nil, nil
	case <- ctx.Done():
		return nil, nil
	case msg, ok := <- queue.claim.Messages():
		if !ok {
			return nil, ErrClaimClosed
		}
		queue.last = msg
		return queue.last, nil
	}
}


// commit all message before and include last pull message. Also send last message to dead message queue
func (queue *ConsumerQueue) Commit(ctx context.Context, deadErr error) {
	if queue.last != nil {
		if deadErr != nil && queue.deadQueue != nil {
			queue.deadQueue.SendDeadMessage(ctx, queue.last, deadErr)
		}
		queue.session.MarkMessage(queue.last, "")
	}
}

type DeadQueue interface {
	SendDeadMessage(ctx context.Context, message *sarama.ConsumerMessage, err error)
	Close() error
}

