package notify

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
	"go.uber.org/zap"
	"sync"
	"time"
)

type Subscriber interface {
	Topic() string
	Read(ctx context.Context) (InputMessage, error)
	Close() error
}

type InputMessage struct {
	Topic string
	Message
	acker Acker
}

func (msg InputMessage) Ack(ctx context.Context) error {
	if msg.acker != nil {
		return msg.acker.Ack(ctx)
	}
	return nil
}


type Acker interface {
	Ack(ctx context.Context) error
}

type Router struct {
	pb *prob.Prob
	router *message.Router
	logger *zap.Logger
	subscriber Subscriber
}

type RouterOption struct {
	Debug bool
	LogTrace bool
}

func NewRouter(option RouterOption, sub Subscriber) (*Router, error) {
	rt, err := message.NewRouter(message.RouterConfig{
		CloseTimeout: time.Second * 10,
	}, watermill.NewStdLogger(option.Debug, option.LogTrace))
	if err != nil {
		return nil, err
	}

	ret := &Router{
		router: rt,
	}
	ret.pb = prob.New(ret.run)
	return ret, nil
}

type Handler interface {
	HandleMessage(msg InputMessage) error
	Name() string
}

func (router *Router) AddHandler(topic string, handler Handler) {
	pubs := router.subscriber[topic]
	for _, pb := range pubs {
		router.router.AddNoPublisherHandler(handler.Name(), topic, subscriberWrap{pb}, func(msg *message.Message) error {
			inputMessage := InputMessage{
				Topic: topic,
				Message: Message{
					UID: msg.UUID,
					PayLoad: msg.Payload,
					Head: msg.Metadata,
				},
				acker: msgAcker{msg,},
			}
			return handler.HandleMessage(inputMessage)
		})
	}
}

func (router *Router) Start(ctx context.Context) error {
	router.pb.Start()
	return nil
}

func (router *Router) run(ctx context.Context) {
	syncrun.FuncWithRandomStart(func(ctx context.Context) bool {
		err := router.router.Run(ctx)
		if err != nil {
			router.logger.Error("run router err", zap.Error(err))
		}
		return true
	}, syncrun.RandRestart(time.Second, 3 * time.Second))(ctx)
}

type msgAcker struct {
	msg *message.Message
}

func (acker msgAcker) Ack(ctx context.Context) error {
	acker.msg.Ack()
	return nil
}

type subscriberWrap struct {
	rw sync.RWMutex
	sub Subscriber
	cn []chan *message.Message
}

func wrapSubscriber(sub Subscriber) subscriberWrap {
	return subscriberWrap {
		sub: sub,
	}
}

func (wrap subscriberWrap) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	cn := make(chan *message.Message)
}

func (wrap subscriberWrap) Close() error {
	return wrap.sub.Close()
}

