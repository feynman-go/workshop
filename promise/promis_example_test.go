package promise

import (
	"context"
	"log"
	"time"
)

func ExamplePromise() {
	pool := NewPool(3)
	pms := NewPromise(pool, Process{
		Process: func(ctx context.Context, last interface{}) (interface{}, error) {
			// .... Do task
			return true, nil
		},
	})

	// close promise will close task context
	defer Close()

	ctx, _ := context.WithTimeout(context.Background(), 100 * time.Millisecond)
	_, err := Get(ctx, true)
	if err != nil {
		// ... Handle err
	}
}

func ExamplePromiseChain() {
	pool := NewPool(3)
	pms := Then(Process{
		Process: func(ctx context.Context, last interface{}) (interface{}, error) {
			// .... Do task 2
			log.Println("do task two")
			return true, nil
		},
	})

	// close promise will close task context
	defer Close()

	ctx, _ := context.WithTimeout(context.Background(), 100 * time.Millisecond)
	_, err := Get(ctx, true)
	if err != nil {
		// ... Handle err
	}
}

func ExamplePromiseRetry() {
	pool := NewPool(3)
	pms := RecoverAndRetry(ExceptionProcess{
		Process: func(ctx context.Context, err error, last interface{}) (interface{}, error) {
			// .... try recover
			log.Println("try recover")
			return last, nil
		},
	})

	// close promise will close task context
	defer Close()

	ctx, _ := context.WithTimeout(context.Background(), 100 * time.Millisecond)
	_, err := Get(ctx, true)
	if err != nil {
		// ... Handle err
	}
}

