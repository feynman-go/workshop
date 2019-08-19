package limit

import (
	"context"
	"fmt"
	"github.com/feynman-go/workshop/promise"
	"golang.org/x/time/rate"
)

func WrapWithRateLimit(limiter *rate.Limiter, processFunc promise.ProcessFunc) promise.ProcessFunc {
	return promise.ProcessFunc(func(ctx context.Context, req promise.Request) promise.Result {
		err := limiter.Wait(ctx)
		if err != nil {
			return promise.Result{
				Err: fmt.Errorf("rate limited: %v", err),
			}
		}
		return processFunc(ctx, req)
	})
}
