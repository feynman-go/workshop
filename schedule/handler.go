package schedule

import (
	"context"
	"github.com/feynman-go/workshop/syncrun/routine"
	"go.uber.org/zap"
	"time"
)

type Result struct {
	id string
	nextSchedule *NextOption
	message string
	ok bool
}

type NextOption struct {
	ExpectTime time.Time
}

func (option NextOption) After(duration time.Duration) NextOption {
	option.ExpectTime = time.Now().Add(duration)
	return option
}

func (option NextOption) Expect(tm time.Time) NextOption {
	option.ExpectTime = tm
	return option
}

func NewResult(id string) *Result {
	return &Result{
		id:           id,
		nextSchedule: nil,
		message:      "",
		ok:           true,
	}
}

func (res *Result) DoNext(option NextOption) *Result {
	res.nextSchedule = &option
	return res
}

func (res *Result) SetExecInfo(message string, ok bool) *Result {
	res.message = message
	res.ok = ok
	return res
}

func (res *Result) NextSchedule() *Schedule {
	if res.nextSchedule == nil {
		return nil
	}
	return &Schedule{
		ID:         res.id,
		ExpectTime: res.nextSchedule.ExpectTime,
		CreateTime: time.Now(),
		MaxDelays:  0,
		Labels:     nil,
	}
}

const (
	_TASK_LABEL_NAME = "taskName"
)

type Message struct {
	awaken Awaken
	ctx    context.Context
}

func (msg Message) ID() string {
	return msg.awaken.Spec().ID
}

func (msg Message) Name() string {
	return msg.awaken.Spec().Labels[_TASK_LABEL_NAME]
}

func (msg Message) ExpectTime() time.Time {
	return msg.awaken.Spec().ExpectTime
}

func (msg Message) Context() context.Context {
	return msg.ctx
}

func (msg Message) WithContext(ctx context.Context) Message {
	msg.ctx = ctx
	return msg
}


type Handler func(message Message, result *Result)

type Middle interface {
	Warp(Handler) Handler
}

type MessageManager struct {
	rt *routine.Routine
	receiver Receiver
	logger *zap.Logger
	middles []Middle
	handler Handler
	scheduler Scheduler
	deadQueue DeadQueue
}

func NewMessageManager(handler Handler, middles []Middle, logger *zap.Logger) *MessageManager {
	if logger == nil {
		logger = zap.L()
	}
	panic("TODO")
}

func (manager *MessageManager) Start() {
	manager.rt.Start()
}

func (manager *MessageManager) CloseWithContext(ctx context.Context) error {
	return manager.rt.StopAndWait(ctx)
}

func (manager *MessageManager) handleMessage(ctx context.Context, awaken Awaken) error {
	handler := manager.handler
	for _, mid := range manager.middles {
		handler = mid.Warp(handler)
	}

	msg := Message{
		awaken: awaken,
		ctx:    ctx,
	}
	var result = NewResult(awaken.Spec().ID)
	handler(msg, result)

	if schedule := result.NextSchedule(); schedule != nil {
		return manager.scheduler.PostSchedule(ctx, *schedule)
	}
	return nil
}

func (manager *MessageManager) run(ctx context.Context) {
	for ctx.Err() == nil {
		awaken, err := manager.receiver.WaitAwaken(ctx)
		if err != nil {
			manager.logger.Error("wait awaken:", zap.Error(err))
			continue
		}
		err = manager.handleMessage(ctx, awaken)
		manager.deadQueue.PushDeadAwaken(ctx, awaken.Spec(), err)
		manager.receiver.Commit(ctx, err)
	}
}

type DeadQueue interface {
	PushDeadAwaken(ctx context.Context, schedule Schedule, err error)
}