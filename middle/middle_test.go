package middle

import (
	"context"
	"github.com/feynman-go/workshop"
	"runtime"
	"testing"
	"time"
)

func TestPromiseWithRate(t *testing.T) {

	t.Run("test promise err of rate limit", func(t *testing.T) {
		pool := workshop.NewPool(runtime.GOMAXPROCS(0))
		var md = NewRateMiddle(6, 6)
		var err error
		i := 0
		for {
			pms := workshop.NewPromise(pool, workshop.Process{
				Process: func(ctx context.Context, last interface{}) (interface{}, error) {
					return nil, nil
				},
				EventKey: 1,
				Middles: []workshop.Middle{md},
			})
			err = pms.Wait(context.Background())
			if err != nil {
				break
			}
			i ++
		}

		if err != RateLimitErr {
			t.Fatalf("expect return err is: %v but is %v", RateLimitErr, err)
		}

		if i != 6 {
			t.Fatalf("expect return err is in index: %v", i)
		}
	})

	t.Run("test promise of rate recover", func(t *testing.T) {
		pool := workshop.NewPool(runtime.GOMAXPROCS(0))
		var md = NewRateMiddle(6, 6)
		var err error
		i := 0
		for {
			pms := workshop.NewPromise(pool, workshop.Process{
				Process: func(ctx context.Context, last interface{}) (interface{}, error) {
					return nil, nil
				},
				EventKey: 1,
				Middles: []workshop.Middle{md},
			})
			err = pms.Wait(context.Background())
			if err != nil {
				break
			}
			i ++
		}

		if err != RateLimitErr {
			t.Fatalf("expect return err is: %v but is %v", RateLimitErr, err)
		}

		if i != 6 {
			t.Fatalf("expect return err is in index: %v", i)
		}

		time.Sleep(time.Second)

		pms := workshop.NewPromise(pool, workshop.Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				return nil, nil
			},
			EventKey: 1,
			Middles: []workshop.Middle{md},
		})
		err = pms.Wait(context.Background())
		if err != nil {
			t.Fatalf("promise should recover from rate limit but %v", err)
		}
	})


}