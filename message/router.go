package message

import (
	"context"
	"errors"
	"github.com/feynman-go/workshop/mutex"
	"github.com/feynman-go/workshop/record"
	"github.com/feynman-go/workshop/record/easyrecord"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
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
	Acker Acker
}

func (msg InputMessage) Commit(ctx context.Context) {
	if msg.Acker != nil {
		msg.Acker.Commit(ctx)
	}
	return
}

type Acker interface {
	Commit(ctx context.Context)
}

type Router struct {
	pb     *prob.Prob
	logger *zap.Logger
	topics map[string]*subscribeJoint
	middles []Middle
}

type RouterOption struct {
	Middles []Middle
}

func NewRouter(option RouterOption, subs ...Subscriber) (*Router, error) {
	ret := &Router{
		topics: map[string]*subscribeJoint{},
	}

	for _, sub := range subs {
		ret.topics[sub.Topic()] = &subscribeJoint{sub: sub}
	}

	ret.pb = prob.New(ret.run)
	ret.middles = option.Middles
	return ret, nil
}

type Handler interface {
	HandleMessage(ctx context.Context, topic string, msg Message) error
	Name() string
}

func (router *Router) AddHandler(topic string, handler Handler) error {
	joint := router.topics[topic]
	if joint == nil {
		return errors.New("topic not support")
	}

	for _, middle := range router.middles {
		handler = middle.Warp(handler)
	}

	err := joint.addHandler(handler)
	if err != nil {
		return err
	}

	return nil
}

func (router *Router) Start(ctx context.Context) error {
	router.pb.Start()
	return nil
}

func (router *Router) run(ctx context.Context) {
	var fs = []func(ctx context.Context){}

	for topic, _ := range router.topics {
		joint := router.topics[topic]
		fs = append(fs, joint.run)
	}

	syncrun.RunAsGroup(ctx, fs...)
}

type subscribeJoint struct {
	rw       sync.RWMutex
	sub      Subscriber
	handlers []Handler
}

func (joint *subscribeJoint) addHandler(handler Handler) error {
	joint.rw.Lock()
	defer joint.rw.Unlock()

	if len(joint.handlers) >= 64 {
		return errors.New("too much handler count")
	}
	joint.handlers = append(joint.handlers, handler)
	return nil
}

type actor struct {
	ok      bool
	index   int
	err     error
	handler Handler
}

func (joint *subscribeJoint) run(ctx context.Context) {
	for ctx.Err() == nil {
		inMessage, err := joint.sub.Read(ctx)
		if err != nil {
			continue
		}
		joint.handleMessage(ctx, inMessage)
	}
}

func (joint *subscribeJoint) handleMessage(ctx context.Context, msg InputMessage) error {
	actors := actorsPool.Get().([]actor)
	actors = actors[:0]
	defer actorsPool.Put(actors)

	mx := new(mutex.Mutex)

	joint.rw.RLock()
	for i := range joint.handlers {
		actors = append(actors, actor{
			index:   i,
			handler: joint.handlers[i],
		})
	}
	joint.rw.RUnlock()

	var remain int32
	for i := range actors {
		act := actors[i]
		if act.ok {
			continue
		}
		if !mx.HoldForRead(ctx) {
			return ctx.Err()
		}
		go func() {
			defer mx.ReleaseForRead()
			err := act.handler.HandleMessage(ctx, msg.Topic, msg.Message)
			if err != nil {
				actors[i].err = err
				atomic.AddInt32(&remain, 1)
			} else {
				actors[i].err = nil
				actors[i].ok = true
			}
		}()
	}

	if mx.Hold(ctx) {
		mx.Release()
		if remain == 0 {
			msg.Commit(ctx)
			return nil
		}
	}
	return ctx.Err()
}

var actorsPool = &sync.Pool{
	New: func() interface{} {
		return make([]actor, 0, 2)
	},
}

type Middle interface {
	Warp(handler Handler) Handler
}

type TimeOutMiddle struct {
	TimeOut time.Duration
}

func (middle TimeOutMiddle) Warp(handler Handler) Handler {
	return &middleHandler{
		handler, func(ctx context.Context, topic string, msg Message) error {
			ctx, _ = context.WithTimeout(ctx, middle.TimeOut)
			return handler.HandleMessage(ctx, topic, msg)
		},
	}
}

type RetryMiddle struct {
	CheckRetry func(err error) bool
	MaxRetryCount int
}

func (middle RetryMiddle) Warp(handler Handler) Handler {
	return &middleHandler{
		handler, func(ctx context.Context, topic string, msg Message) error {
			maxRetry := middle.MaxRetryCount
			if maxRetry <= 0 {
				maxRetry = 3
			}
			for i := 0 ; i < maxRetry; i ++ {
				err := handler.HandleMessage(ctx, topic, msg)
				if err == nil {
					return nil
				}
				if middle.CheckRetry == nil || !middle.CheckRetry(err) {
					return err
				}
			}
			return nil
		},
	}
}

type RecordMiddle struct {
	Records record.Factory
}

func (middle RecordMiddle) Warp(handler Handler) Handler {
	return &middleHandler {
		handler, func(ctx context.Context, topic string, msg Message) error {
			r := middle.Records
			if r == nil {
				r = easyrecord.ExtraFromContext(ctx)
			}
			return easyrecord.DoWithRecords(ctx, func(ctx context.Context) error {
				return handler.HandleMessage(ctx, topic, msg)
			}, r,handler.Name())
		},
	}
}


type middleHandler struct {
	Handler
	f func(ctx context.Context, topic string, msg Message) error
}

func (handler middleHandler) HandleMessage(ctx context.Context, topic string, msg Message) error {
	if handler.f != nil {
		return handler.f(ctx, topic, msg)
	}
	return handler.Handler.HandleMessage(ctx, topic, msg)
}