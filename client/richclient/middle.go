package richclient

import (
	"context"
	"github.com/feynman-go/workshop/breaker"
	"github.com/feynman-go/workshop/client"
	"github.com/feynman-go/workshop/record"
	"github.com/feynman-go/workshop/record/easyrecord"
	"golang.org/x/time/rate"
)

type limiterMiddle struct {
	limiter *rate.Limiter
}

func LimiterMiddle(limiter *rate.Limiter) client.DoMiddle {
	return limiterMiddle{limiter}
}

func (lm limiterMiddle) WrapDo(f func(ctx context.Context, agent client.Agent) error, opt client.ActionOption) func(ctx context.Context, agent client.Agent) error {
	return func(ctx context.Context, agent client.Agent) error {
		err := lm.limiter.Wait(ctx)
		if err != nil {
			return err
		}
		return f(ctx, agent)
	}
}


type breakerMiddle struct {
	bk *breaker.Breaker
}

func NewBreakerMiddle(bk *breaker.Breaker) client.DoMiddle {
	return &breakerMiddle {
		bk: bk,
	}
}
func (manager *breakerMiddle) WrapDo(f func(ctx context.Context, agent client.Agent) error, option client.ActionOption) func(ctx context.Context, agent client.Agent) error {
	return func(ctx context.Context, agent client.Agent) error {
		var err error
		manager.bk.WaitBreakerOn(ctx, func(ctx context.Context) bool {
			err = f(ctx, agent)
			return false
		})
		return err
	}
}

func RetryMiddle(count int, errCheck func(error) bool) client.DoMiddle {
	return retryMiddle{
		canRetry: func(err error) bool {
			if err == nil || count == 0 {
				return false
			}
			if errCheck != nil && !errCheck(err) {
				return false
			}
			if count < 0 {
				return true
			}
			count --
			return true
		},
	}
}

type retryMiddle struct {
	canRetry func(error) bool
}

func (middle retryMiddle) WrapDo(f func(ctx context.Context, agent client.Agent) error, opt client.ActionOption) func(ctx context.Context, agent client.Agent) error {
	return func(ctx context.Context, agent client.Agent) error {
		var err error
		for ctx.Err() == nil {
			err = f(ctx, agent)
			if err == nil {
				break
			}
			if middle.canRetry == nil || !middle.canRetry(err) {
				break
			}
		}
		return err
	}
}

func NewRecorderMiddle(factory record.Factory) client.DoMiddle {
	return &recorderMiddle{
		factory: factory,
	}
}

type recorderMiddle struct {
	factory record.Factory
}

func (md *recorderMiddle) WrapDo(f func(ctx context.Context, agent client.Agent) error, opt client.ActionOption) func(ctx context.Context, agent client.Agent) error {
	return func(ctx context.Context, agent client.Agent) (err error) {
		var records record.Factory
		if md.factory == nil {
			records = easyrecord.ExtraFromContext(ctx)
		}
		if records != nil && opt.Name != "" {
			var recorder record.Recorder
			recorder, ctx = records.ActionRecorder(ctx, opt.Name)
			defer func() {
				recorder.Commit(err)
			}()
		}
		err = f(ctx, agent)
		return err
	}
}
