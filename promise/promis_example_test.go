package promise

import (
	"context"
	"errors"
	"log"
	"time"
)

func ExamplePromise() {
	pool := NewPool(3)
	pms := NewPromise(pool, func(ctx context.Context, req Request) Result {
		// .... Do task
		return Result{
			Err: nil,
			Payload: 25,
		}
	})

	// close promise will close task context
	defer pms.Close()

	ctx, _ := context.WithTimeout(context.Background(), 100 * time.Millisecond)
	_, err := pms.Get(ctx, true)
	if err != nil {
		// ... Handle err
	}
}

func ExamplePromiseChain() {
	pool := NewPool(3)
	pms := NewPromise(pool, func(ctx context.Context, req Request) Result {
		// .... Do task 1
		log.Println("do task one")
		return Result{

		}
	}).Then(func(ctx context.Context, req Request) Result {
		// .... Do task 2
		log.Println("do task two")
		return Result{
			Payload: true,
		}
	})

	// close promise will close task context
	defer pms.Close()

	ctx, _ := context.WithTimeout(context.Background(), 100 * time.Millisecond)
	_, err := pms.Get(ctx, true)
	if err != nil {
		// ... Handle err
	}
}

func ExamplePromiseRetry() {
	pool := NewPool(3)
	pms := NewPromise(pool, func(ctx context.Context, req Request) Result {
		// .... Do task 1
		log.Println("do task one")
		return Result{
			Err: errors.New("do task err"),
		}
	})

	pms.Recover(func(ctx context.Context, req Request) Result {
		// .... try recover
		log.Println("try recover err:", req.LastErr())
		return Result{
			Err: nil, // recover success
		}
	}).HandleException(func(ctx context.Context, req Request) Result {
		// .... handle retry field
		log.Println("recover failed")
		return Result{
			Err: req.LastErr(),
		}
	})

	// close promise will close task context
	defer pms.Close()

	ctx, _ := context.WithTimeout(context.Background(), 100 * time.Millisecond)
	_, err := pms.Get(ctx, true)
	if err != nil {
		// ... Handle err
	}
}

