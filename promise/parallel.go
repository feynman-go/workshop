package promise

import "context"

func Parallel(ctx context.Context, process func(ctx context.Context) error) error {
	pool := NewPool(1)
	defer pool.Close()

	promise := NewPromise(pool, Process{
		Process: func(ctx context.Context, request Request) Result {
			err := process(ctx)
			return Result{
				Err: err,
			}
		},
	})

	return promise.Wait(ctx, true)
}