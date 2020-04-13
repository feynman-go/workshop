package syncrun

import (
	"context"
	"github.com/feynman-go/workshop/randtime"
	"sync"
	"time"
)

func RunAsGroup(ctx context.Context, runner ...func(ctx context.Context)) {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)

	startGroup := &sync.WaitGroup{}
	endGroup := &sync.WaitGroup{}

	for _, f := range runner {
		startGroup.Add(1)
		endGroup.Add(1)
		go func(ctx context.Context, f func(ctx context.Context), cancel func()) {
			startGroup.Done()
			defer cancel()
			defer endGroup.Done()
			if ctx.Err() == nil {
				f(ctx)
			}
		}(ctx, f, cancel)
	}

	startGroup.Wait()
	endGroup.Wait()
}



func FuncWithReStart(runFunc func(ctx context.Context) (canRestart bool), restartWait func() time.Duration) func(ctx context.Context) {
	return func(ctx context.Context) {
		for ctx.Err() == nil {
			if !runFunc(ctx) {
				return
			}
			wait := restartWait()
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <- ctx.Done():
				case <- timer.C:
				}
			}
		}
	}
}

func RandRestart(min, max time.Duration) func() time.Duration {
	return func() time.Duration {
		return randtime.RandDuration(min, max)
	}
}