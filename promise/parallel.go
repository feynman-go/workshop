package promise

import "context"

func Parallel(ctx context.Context, process func(ctx context.Context) error, middles ...Middle) error {
	pool := NewPool(1)
	defer pool.Close()

	promise := NewPromise(pool, func(ctx context.Context, request Request) Result {
		err := process(ctx)
		return Result{
			Err: err,
		}
	}, middles...)

	return promise.Wait(ctx, true)
}