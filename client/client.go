package client

import (
	"context"
	"github.com/feynman-go/workshop/breaker"
	"github.com/feynman-go/workshop/mutex"
	"github.com/feynman-go/workshop/promise"
	"github.com/feynman-go/workshop/promise/limit"
	"github.com/feynman-go/workshop/promise/metric"
	"github.com/feynman-go/workshop/promise/tracer"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
	"runtime"
	"sync/atomic"
)

type Option struct {
	ParallelCount    *int
	LimitEverySecond *float64
	Breaker          *breaker.StatusConfig
	QueueAction      *bool
	TracerName       *string
	MetricName 		 *string
}

func (opt Option) SetParallelCount(count int) Option {
	opt.ParallelCount = &count
	return opt
}

func (opt Option) SetLimiter(countPerSecond float64) Option {
	opt.LimitEverySecond = &countPerSecond
	return opt
}

func (opt Option) SetBreaker(config breaker.StatusConfig) Option {
	opt.Breaker = &config
	return opt
}

func (opt Option) SetTracerName(name string) Option {
	opt.TracerName = &name
	return opt
}

func (opt Option) SetMetricName(name string) Option {
	opt.MetricName = &name
	return opt
}

// ordered request by partition id
func (opt Option) SetOrderedRequest(queue bool) Option {
	opt.QueueAction = &queue
	return opt
}

type Client struct {
	name          string
	mx            *mutex.Mutex
	closed        int32
	limiter       *rate.Limiter
	breakerStatus *breaker.Status
	breakerConfig breaker.StatusConfig
	pool          *promise.Pool
	agent         Agent
	option        Option
	middles       []promise.Middle
}

func NewClient(agent Agent, option Option) *Client {
	option = completeOption(option)
	clt :=  &Client{
		option: option,
		mx: new(mutex.Mutex),
		pool: promise.NewPool(*option.ParallelCount),
		agent: agent,
	}

	if option.LimitEverySecond != nil {
		clt.limiter = rate.NewLimiter(rate.Limit(*option.LimitEverySecond), *option.ParallelCount + 1)
	}

	if option.Breaker != nil {
		clt.breakerStatus = breaker.NewStatus(*option.Breaker)
	}

	clt.setupMiddles()
	return clt
}

func (client *Client) setupMiddles() {
	mids := []promise.Middle{}
	if client.limiter != nil {
		mids = append(mids, )
	}

	if client.breakerStatus != nil {
		mids = append(mids, promise.WrapProcess("breaker", func(f promise.ProcessFunc) promise.ProcessFunc {
			return limit.WrapWithBreakerStatus(client.breakerStatus, f)
		}))
	}

	if client.limiter != nil {
		mids = append(mids, promise.WrapProcess("limit", func(f promise.ProcessFunc) promise.ProcessFunc {
			return limit.WrapWithRateLimit(client.limiter, f)
		}))
	}

	if client.option.TracerName != nil {
		tc := tracer.OpenTracer {
			Name: *client.option.TracerName,
			RecordHead: false,
		}
		mids = append(mids, promise.WrapProcess("tracer", tc.Wrap))
	}

	if client.option.MetricName != nil {
		m := metric.NewPromMetric(*client.option.MetricName, nil)
		mids = append(mids, promise.WrapProcess("metric", m.Wrap))
	}

	client.middles = mids
}

func (client *Client) Reset(ctx context.Context) error {
	if atomic.LoadInt32(&client.closed) > 0 {
		return errors.New("closed")
	}

	if client.mx.Hold(ctx) {
		defer client.mx.Release()
		if atomic.LoadInt32(&client.closed) > 0  {
			return errors.New("closed")
		}
		return client.agent.Reset(ctx)
	} else {
		return ctx.Err()
	}
}

func (client *Client) Close(ctx context.Context) error {
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

	p := promise.NewPromise(client.pool, func(ctx context.Context, req promise.Request) promise.Result {
		err := operation(ctx, client.agent)
		return promise.Result{
			Err: err,
		}
	}, client.getMiddles(opt)...)

	p.Start(ctx)
	return p.Wait(ctx, true)
}

func (client *Client) getMiddles(opt ActionOption) []promise.Middle {
	var part int
	if opt.PartitionID != nil {
		part =  *opt.PartitionID
	}

	mids := []promise.Middle{promise.EventKeyMiddle(part)}
	return append(mids, client.middles...)
}

type Agent interface {
	Reset(ctx context.Context) error
	Close(ctx context.Context) error
}

type ActionOption struct {
	PartitionID *int
}

func (opt ActionOption) SetPartition(partition int) ActionOption {
	opt.PartitionID = &partition
	return opt
}

func mergeActionOption(actOption []ActionOption) ActionOption {
	opt := ActionOption{}
	for _, a := range actOption {
		if a.PartitionID != nil {
			opt = opt.SetPartition(*a.PartitionID)
		}
	}
	return opt
}

func IsRateLimiter(err error) bool {
	//TODO
	return false
}

func IsBreakerLimiter(err error) bool {
	//TODO
	return false
}

func completeOption(option Option) Option {
	if option.ParallelCount == nil {
		option.SetParallelCount(runtime.GOMAXPROCS(0))
	}
	return option
}