package middle

import (
	"context"
	"github.com/feynman-go/workshop/promise"
	"log"
	"time"
)

func ExampleBreaker() {
	var failedLimit float64 = 6
	bucketSize := 6
	middle := NewBreakerMiddle(time.Second, failedLimit, bucketSize)

	pool := promise.NewPool(4)
	var err error
	pms := promise.NewPromise(pool, promise.Process{
		Process: func(ctx context.Context, last interface{}) (interface{}, error) {
			return nil, nil
		},
		EventKey: 1,
		Middles: []promise.Middle{middle}, // use breaker middle
	})
	err = pms.Wait(context.Background())
	if err == BreakerOpenErr {
		// rate limit
		log.Println("breaker open")
	}
}

func ExampleRate() {
	pool := promise.NewPool(4)
	var md = NewRateMiddle(6, 6)
	var err error
	pms := promise.NewPromise(pool, promise.Process{
		Process: func(ctx context.Context, last interface{}) (interface{}, error) {
			return nil, nil
		},
		EventKey: 1,
		Middles: []promise.Middle{md}, // use rate middle
	})
	err = pms.Wait(context.Background())
	if err == RateLimitErr {
		// rate limit
		log.Println("rate limit occur")
	}
}
