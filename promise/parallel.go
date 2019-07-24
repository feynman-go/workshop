package promise

import "context"

func Parallel(ctx context.Context, process func(ctx context.Context) error, opt ...Option) error {
	pool := NewPool(1)
	defer pool.Close()

	promise := NewPromise(pool, func(ctx context.Context, request Request) Result {
		err := process(ctx)
		return Result{
			Err: err,
		}
	}, opt...)

	return promise.Wait(ctx, true)
}