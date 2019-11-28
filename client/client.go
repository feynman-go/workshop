package client

import (
	"context"
	"github.com/feynman-go/workshop/mutex"
	"github.com/feynman-go/workshop/promise"
	"github.com/pkg/errors"
	"runtime"
	"sync/atomic"
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
		err := client.agent.CloseWithContext(ctx)
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

	p := promise.NewPromise(context.Background(), client.pool, func(req promise.Request) promise.Result {
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

func (opt ActionOption) SetName(name string) ActionOption {
	opt.Name = name
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