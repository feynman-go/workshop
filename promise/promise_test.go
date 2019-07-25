
package promise

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPromise(t *testing.T) {
	t.Run("basic simple promise", func(t *testing.T) {
		pool := NewPool(3)
		pms := NewPromise(pool, func(ctx context.Context, request Request) Result {
			return Result{
				Payload: true,
			}
		})

		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
		v, err := pms.Get(ctx, true)
		if err != nil {
			t.Fatal("get result with err:", err)
		}

		if v != true {
			t.Errorf("expect return true but return %v", v)
		}
		return
	})

	t.Run("basic simple exception", func(t *testing.T) {
		var reach bool
		var failed bool
		pool := NewPool(3)
		pms := NewPromise(pool, func(ctx context.Context, last Request) Result{
			reach = true
			return Result{
				Payload: true,
				Err: errors.New("expect err"),
			}
		}).OnException(func(ctx context.Context, last Request) Result {
			failed = true
			return Result{
				Payload: false,
				Err: nil,
			}
		})

		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
		v, err := pms.Get(ctx, true)
		if err != nil {
			t.Fatal("get result with err:", err)
		}

		if v != false {
			t.Fatalf("expect return true but return %v", v)
		}

		if !reach {
			t.Fatalf("not reach root Process")
		}

		if !failed {
			t.Errorf("not reach failed Process")
		}

		return
	})

	t.Run("basic simple failed", func(t *testing.T) {
		var failed = errors.New("failed")
		pool := NewPool(3)
		pms := NewPromise(pool, func(ctx context.Context, req Request) Result {
			return Result{
				Err: failed,
				Payload: true,
			}
		})

		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
		_, err := pms.Get(ctx, true)
		if err != failed {
			t.Fatal("expect get failed err but get:", err)
		}
		return
	})

	t.Run("basic simple exception", func(t *testing.T) {
		var reach bool
		var failed bool
		pool := NewPool(3)
		pms := NewPromise(pool, func(ctx context.Context, last Request) Result{
			reach = true
			return Result{
				Payload: true,
				Err: errors.New("expect err"),
			}
		}).OnException(func(ctx context.Context, last Request) Result {
			failed = true
			return Result{
				Payload: false,
				Err: nil,
			}
		})

		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
		v, err := pms.Get(ctx, true)
		if err != nil {
			t.Fatal("get result with err:", err)
		}

		if v != false {
			t.Fatalf("expect return true but return %v", v)
		}

		if !reach {
			t.Fatalf("not reach root Process")
		}

		if !failed {
			t.Errorf("not reach failed Process")
		}

		return
	})

	t.Run("test promise Wait time", func(t *testing.T) {
		pool := NewPool(3)
		var startTime time.Time
		pms := NewPromise(pool, func(ctx context.Context, req Request) Result {
			startTime = time.Now()
			return Result{
				Err: nil,
				Payload: true,
			}
		}, WaitTimeMiddle(100 * time.Millisecond))

		ctx, _ := context.WithTimeout(context.Background(), 200 * time.Millisecond)
		now := time.Now()
		_, err := pms.Get(ctx, true)
		if err != nil {
			t.Fatal("expect get failed err but get:", err)
		}
		if delta := startTime.Sub(now); delta > 110 * time.Millisecond || delta < 100 * time.Millisecond {
			t.Fatalf("expect Wait %v time but %v:", 100 * time.Millisecond, delta)
		}
		return
	})
}

func TestPromiseChain(t *testing.T) {
	t.Run("basic chain promise", func(t *testing.T) {
		var list = []int{}

		pool := NewPool(3)
		pms := NewPromise(pool, func(ctx context.Context, request Request) Result {
			list = append(list, 0)
			return Result{
				Payload: true,
			}
		}).Then(func(ctx context.Context, request Request) Result {
			list = append(list, 1)
			return Result{
				Payload: true,
			}
		}).Then(func(ctx context.Context, last Request) Result {
			list = append(list, 2)
			return Result{
				Payload: true,
			}
		})

		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
		v, err := pms.Get(ctx, true)
		if err != nil {
			t.Fatal("get result with err:", err)
		}

		if v != true {
			t.Errorf("expect return true but return %v", v)
		}

		for i := 0 ; i < 3; i ++ {
			if list[i] != i {
				t.Fatalf("test promise chan not equal id: %v", i)
			}
		}

		return
	})

	t.Run("basic chain exception", func(t *testing.T) {
		var list = []int{}

		pool := NewPool(3)
		pms := NewPromise(pool, func(ctx context.Context, request Request) (Result) {
			list = append(list, 0)
			return Result{
				Payload: true,
			}
		}).Then(func(ctx context.Context, request Request) (Result) {
			list = append(list, 1)
			return Result{
				Payload: true,
			}
		}).Then(func(ctx context.Context, request Request) (Result) {
			list = append(list, 1)
			return Result{
				Payload: true,
				Err: errors.New("1"),
			}
		}).OnException(func(ctx context.Context, request Request) (Result) {
			list[2] = 2
			return Result{
				Payload: true,
			}
		})

		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
		v, err := pms.Get(ctx, true)
		if err != nil {
			t.Fatal("get result with err:", err)
		}

		if v != true {
			t.Errorf("expect return true but return %v", v)
		}

		for i := 0 ; i < 3; i ++ {
			if list[i] != i {
				t.Fatalf("test promise chan not equal id: %v", i)
			}
		}

		return
	})
}

func TestPromiseCloseWait(t *testing.T) {
	t.Run("test Wait timeout", func(t *testing.T) {
		pool := NewPool(3)
		pms := NewPromise(pool, func(ctx context.Context, request Request) Result {
			time.Sleep(100 * time.Millisecond)
			return Result{
				Payload: true,
			}
		})

		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
		_, err := pms.Get(ctx, true)
		if err != context.DeadlineExceeded {
			t.Fatalf("Wait timeout should return err of context.DeadlineExceeded but %v", err)
		}
	})

	t.Run("test Wait timeout", func(t *testing.T) {
		pool := NewPool(3)
		pms := NewPromise(pool, func(ctx context.Context, request Request) Result {
			time.Sleep(100 * time.Millisecond)
			return Result{
				Payload: true,
			}
		})

		ctx, _ := context.WithTimeout(context.Background(), 200 * time.Millisecond)
		_, err := pms.Get(ctx, true)
		if err != nil {
			t.Fatalf("Wait timeout should return err nil but %v", err)
		}
	})
}

func TestRecovery(t *testing.T) {
	t.Run("basic recovery and retry", func(t *testing.T) {
		var recovered bool
		pool := NewPool(3)
		pms := NewPromise(pool, func(ctx context.Context, request Request) Result {
			if !recovered {
				return Result {
					Err: errors.New("Process err"),
				}
			}
			return Result {
				Payload: true,
			}
		}).Recover(func(ctx context.Context, req Request) Result {
			recovered = true
			return Result{}
		}).HandleException(func(ctx context.Context, req Request) Result {
			recovered = false
			return Result{
				Err: errors.New("bad recover"),
			}
		})

		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
		v, err := pms.Get(ctx, true)
		if err != nil {
			t.Fatal("get result with err:", err)
		}

		if v != true {
			t.Errorf("expect return true but return %v", v)
		}
		return
	})

	t.Run("basic recovery multi times", func(t *testing.T) {
		var recovered int
		pool := NewPool(3)
		pms := NewPromise(pool, func(ctx context.Context, request Request) Result {
			if recovered < 100 {
				return Result{
					Err: errors.New("Process err"),
				}
			}
			return Result{
				Payload: true,
			}
		}).Recover(func(ctx context.Context, request Request) Result {
			recovered ++
			return Result{}
		}).HandleException(func(ctx context.Context, req Request) Result {
			return Result{
				Err: errors.New("bad recover"),
			}
		})

		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		v, err := pms.Get(ctx, true)
		if err != nil {
			t.Fatal("get result with err:", err)
		}

		if v != true {
			t.Errorf("expect return true but return %v", v)
		}

		if recovered != 100 {
			t.Errorf("expect recovered times 100 but recover times %v", recovered)
		}
		return
	})

	t.Run("basic recovery field", func(t *testing.T) {
		var recovered int
		var failed bool
		pool := NewPool(3)
		pms := NewPromise(pool, func(ctx context.Context, request Request) Result {
			if recovered < 100 {
				return Result{
					Err: errors.New("Process err"),
				}
			}
			return Result{
				Payload: true,
			}
		}).Recover(func(ctx context.Context, request Request) Result {
			recovered ++
			if recovered >= 100 {
				failed = true
				return Result{
					Err: errors.New("recover err"),
				}
			}
			return Result{}
		}).HandleException(nil)

		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		_, err := pms.Get(ctx, true)
		if err == nil {
			t.Fatal("expect return err")
		}

		if recovered != 100 {
			t.Errorf("expect recovered times 100 but recover times %v", recovered)
		}

		if !failed {
			t.Errorf("expect recoverey failed")
		}
		return
	})
}

func TestPromiseMultiTimeOut(t *testing.T) {
	t.Run("test Wait timeout", func(t *testing.T) {
		gp := &sync.WaitGroup{}
		pool := NewPool(3)
		for i := 0 ; i < 100 ; i ++ {
			gp.Add(1)

			go func() {
				pms := NewPromise(pool, func(ctx context.Context, request Request) Result {
					time.Sleep(100 * time.Millisecond)
					return Result{
						Payload: true,
					}
				})

				ctx, _ := context.WithTimeout(context.Background(), 10 * time.Millisecond)

				err := pms.Wait(ctx, true)
				if err == nil {
					t.Fatalf("Wait timeout should return err")
				}
				gp.Done()
			}()
		}

		time.Sleep(1 * time.Second)
		gp.Wait()

	})
}

func TestPartitionPromise(t *testing.T) {
	t.Run("basic simple promise", func(t *testing.T) {
		pool := NewPool(3)
		pms := NewPromise(pool, func(ctx context.Context, request Request) Result {
			return Result{
				Payload: true,
			}
		}, PartitionMiddle(true), EventKeyMiddle(rand.Int()))

		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
		v, err := pms.Get(ctx, true)
		if err != nil {
			t.Fatal("get result with err:", err)
		}

		if v != true {
			t.Errorf("expect return true but return %v", v)
		}
		return
	})
}


func TestLargeRequest(t *testing.T) {
	var count int32
	var ppppp int32
	t.Run("TestLargeRequest", func(t *testing.T) {
		gp := &sync.WaitGroup{}
		pool := NewPool(2)
		for i := 0 ; i < 10 ; i ++ {
			gp.Add(1)

			go func() {
				pms := NewPromise(pool, func(ctx context.Context, request Request) Result {
					time.Sleep(100 * time.Millisecond)
					atomic.AddInt32(&count, 1)
					return Result{
						Payload: true,
					}
				})

				ctx, _ := context.WithTimeout(context.Background(), 10 * time.Second)
				err := pms.Wait(ctx, true)
				if err != nil {
					t.Fatalf("Wait timeout should return err")
				}
				atomic.AddInt32(&ppppp, 1)
				gp.Done()
			}()
		}
		gp.Wait()
	})
}
