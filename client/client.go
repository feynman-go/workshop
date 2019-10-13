package client

import (
	"context"
	"github.com/feynman-go/workshop/breaker"
	"github.com/feynman-go/workshop/mutex"
	"github.com/feynman-go/workshop/promise"
	"github.com/feynman-go/workshop/randtime"
	"github.com/feynman-go/workshop/record"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
	"runtime"
	"sync/atomic"
	"time"
)

type Option struct {
	ParallelCount    *int
	Middles 		 []DoMiddle
}

func (opt Option) SetParallelCount(count int) Option {
	opt.ParallelCount = &count
	return opt
}

func (opt Option) AddMiddle(middle ...DoMiddle) Option {
	opt.Middles = append(opt.Middles, middle...)
	return opt
}

type Client struct {
	name          string
	mx            *mutex.Mutex
	closed        int32
	queueAction   bool
	pool          *promise.Pool
	agent         Agent
	option        Option
}

func New(agent Agent, option Option) *Client {
	option = completeOption(option)
	clt :=  &Client{
		option: option,
		mx: new(mutex.Mutex),
		pool: promise.NewPool(*option.ParallelCount),
		agent: agent,
	}

	return clt
}

func (client *Client) CloseWithContext(ctx context.Context) error {
	if atomic.LoadInt32(&client.closed) > 0 {
		return errors.New("closed")
	}

	if client.mx.Hold(ctx) {
		defer client.mx.Release()
		if atomic.LoadInt32(&client.closed) > 0 {
			return errors.New("closed")
		}
		err := client.agent.Close(ctx)
		if err != nil {
			return err
		}
		if !atomic.CompareAndSwapInt32(&client.closed, 0, 1) {
			return errors.New("closed")
		}
		return nil
	} else {
		return ctx.Err()
	}
}

func (client *Client) Closed() bool {
	return atomic.LoadInt32(&client.closed) == 1
}

func (client *Client) Do(ctx context.Context, operation func(ctx context.Context, agent Agent) error, opts ...ActionOption) error {
	if atomic.LoadInt32(&client.closed) > 0 {
		return errors.New("closed")
	}

	opt := mergeActionOption(opts)
	for _, mid := range client.option.Middles {
		operation = mid.WrapDo(operation, opt)
	}

	pl := client.pool
	if opt.Alone != nil && *opt.Alone {
		pl = promise.NewPool(1)
		defer pl.Close()
	}

	p := promise.NewPromise(client.pool, func(ctx context.Context, req promise.Request) promise.Result {
		err := operation(ctx, client.agent)
		return promise.Result{
			Err: err,
		}
	}, client.getPromiseMiddles(opt)...)

	p.Start(ctx)
	return p.Wait(ctx, true)
}


func (client *Client) getPromiseMiddles(opt ActionOption) []promise.Middle {
	var part int
	if opt.PartitionID != nil {
		part = *opt.PartitionID
		return []promise.Middle{
			promise.EventKeyMiddle(part),
			promise.PartitionMiddle(true),
		}
	}
	return nil
}

type Agent interface {
	CloseWithContext(ctx context.Context) error
}

type ActionOption struct {
	PartitionID *int
	Name        string
	Alone       *bool
}

func (opt ActionOption) SetPartition(partition int) ActionOption {
	opt.PartitionID = &partition
	return opt
}

func (opt ActionOption) SetAlone(alone bool) ActionOption {
	opt.Alone = &alone
	return opt
}

func mergeActionOption(actOption []ActionOption) ActionOption {
	opt := ActionOption{}
	for _, a := range actOption {
		if a.PartitionID != nil {
			opt = opt.SetPartition(*a.PartitionID)
		}
		if a.Name != "" {
			opt.Name = a.Name
		}
		if a.Alone != nil {
			opt.Alone = a.Alone
		}
	}
	return opt
}

func completeOption(option Option) Option {
	if option.ParallelCount == nil {
		option.SetParallelCount(runtime.GOMAXPROCS(0) + 1)
	}
	return option
}

type DoMiddle interface {
	WrapDo(func(ctx context.Context, agent Agent) error, ActionOption) func(ctx context.Context, agent Agent) error
}

type limiterMiddle struct {
	limiter *rate.Limiter
	maxWait time.Duration
}

func LimiterMiddle(limiter *rate.Limiter, maxWait time.Duration) DoMiddle {
	return limiterMiddle{limiter, maxWait}
}

func (lm limiterMiddle) WrapDo(f func(ctx context.Context, agent Agent) error, opt ActionOption) func(ctx context.Context, agent Agent) error {
	return func(ctx context.Context, agent Agent) error {
		if lm.maxWait != 0 {
			ctx, _ = context.WithTimeout(ctx, lm.maxWait)
		}

		err := lm.limiter.Wait(ctx)
		if err != nil {
			return err
		}
		return f(ctx, agent)
	}
}

type breakerMiddle struct {
	breaker *breaker.Breaker
}

type agentRecovery struct {
	r Recoverable
	maxRetryDuration time.Duration
	minRetryDuration time.Duration
}

func (ar *agentRecovery) WaitRecovery(ctx context.Context) {
	max := ar.maxRetryDuration
	min := ar.minRetryDuration
	if min == 0 {
		min = time.Second
	}
	if max < min {
		max = min
	}
	dur := ar.minRetryDuration
	if dur == 0 {
		dur = time.Second
	}

	timer := &time.Timer{}
	for ctx.Err() == nil {
		timer.Reset(dur)
		select {
		case <- ctx.Done():
			return
		case <- time.NewTimer(dur).C:
			rcvCtx, _ := context.WithTimeout(ctx, dur)
			if err := ar.r.TryRecovery(rcvCtx); err == nil {
				return
			}
			dur = randtime.RandDuration(dur, dur * 2)
			if dur > ar.maxRetryDuration {
				dur = ar.maxRetryDuration
			}
		}
	}
}

type Recoverable interface {
	TryRecovery(ctx context.Context) error
}

func NewBasicBreakerMiddle(limiter *rate.Limiter, r Recoverable, resetMinInterval, resetMaxInterval time.Duration) DoMiddle {
	cn := breaker.NewWatcherChain(
		breaker.NewRateWatcher(limiter),
		&agentRecovery{r, resetMaxInterval, resetMinInterval},
	)
	cn.ChainTo(cn)

	mid := &breakerMiddle{breaker.New(cn)}
	return mid
}

func (lm *breakerMiddle) WrapDo(f func(ctx context.Context, agent Agent) error, opt ActionOption) func(ctx context.Context, agent Agent) error {
	return func(ctx context.Context, agent Agent) error {
		if lm.breaker.Opening() {
			return errors.New("closed")
		}
		err := f(ctx, agent)
		lm.breaker.Accept(err != nil)
		return err
	}
}

func NewRecorderMiddle(factory record.Factory) DoMiddle {
	return &recorderMiddle{
		factory: factory,
	}
}

type recorderMiddle struct {
	factory record.Factory
}

func (md *recorderMiddle) WrapDo(f func(ctx context.Context, agent Agent) error, opt ActionOption) func(ctx context.Context, agent Agent) error {
	return func(ctx context.Context, agent Agent) (err error) {
		recorder, ctx := md.factory.ActionRecorder(ctx, opt.Name)
		defer func() {
			recorder.Commit(err)
		}()
		err = f(ctx, agent)
		return err
	}
}

type RecoverableAgent interface {
	Recoverable
	Agent
}