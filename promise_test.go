package workshop

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestPromise(t *testing.T) {
	t.Run("basic simple promise", func(t *testing.T) {
		pool := NewPool(3)
		pms := NewPromise(pool, Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				return true, nil
			},
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
		pms := NewPromise(pool, Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				reach = true
				return true, errors.New("expect err")
			},
		}).OnException(ExceptionProcess{
			Process: func(ctx context.Context, err error, last interface{}) (interface{}, error) {
				failed = true
				return false, nil
			},
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
			t.Fatalf("not reach root process")
		}

		if !failed {
			t.Errorf("not reach failed process")
		}

		return
	})

	t.Run("basic simple failed", func(t *testing.T) {
		var failed = errors.New("failed")
		pool := NewPool(3)
		pms := NewPromise(pool, Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				return true, failed
			},
		})

		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
		_, err := pms.Get(ctx, true)
		if err != failed {
			t.Fatal("expect get failed err but get:", err)
		}
		return
	})
}

func TestPromiseChain(t *testing.T) {
	t.Run("basic chain promise", func(t *testing.T) {
		var list = []int{}

		pool := NewPool(3)
		pms := NewPromise(pool, Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				list = append(list, 0)
				return true, nil
			},
		}).Then(Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				list = append(list, 1)
				return true, nil
			},
		}).Then(Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				list = append(list, 2)
				return true, nil
			},
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
		pms := NewPromise(pool, Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				list = append(list, 0)
				return true, nil
			},
		}).Then(Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				list = append(list, 1)
				return true, nil
			},
		}).Then(Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				list = append(list, 1)
				return true, errors.New("1")
			},
		}).OnException(ExceptionProcess{
			Process: func(ctx context.Context, err error, last interface{}) (interface{}, error) {
				list[2] = 2
				return true, nil
			},
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
	t.Run("test wait timeout", func(t *testing.T) {
		pool := NewPool(3)
		pms := NewPromise(pool, Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				time.Sleep(100 * time.Millisecond)
				return true, nil
			},
		})

		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
		_, err := pms.Get(ctx, true)
		if err != context.DeadlineExceeded {
			t.Fatalf("wait timeout should return err of context.DeadlineExceeded but %v", err)
		}
	})

	t.Run("test wait timeout", func(t *testing.T) {
		pool := NewPool(3)
		pms := NewPromise(pool, Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				time.Sleep(100 * time.Millisecond)
				return true, nil
			},
		})

		ctx, _ := context.WithTimeout(context.Background(), 200 * time.Millisecond)
		_, err := pms.Get(ctx, true)
		if err != nil {
			t.Fatalf("wait timeout should return err nil but %v", err)
		}
	})
}

func TestRecovery(t *testing.T) {
	t.Run("basic recovery and retry", func(t *testing.T) {
		var recovered bool
		pool := NewPool(3)
		pms := NewPromise(pool, Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				if !recovered {
					return nil, errors.New("process err")
				}
				return true, nil
			},
		}).RecoverAndRetry(ExceptionProcess{
			Process: func(ctx context.Context, err error, last interface{}) (interface{}, error) {
				recovered = true
				return nil, nil
			},
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
		pms := NewPromise(pool, Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				if recovered < 100 {
					return nil, errors.New("process err")
				}
				return true, nil
			},
		}).RecoverAndRetry(ExceptionProcess{
			Process: func(ctx context.Context, err error, last interface{}) (interface{}, error) {
				recovered ++
				return nil, nil
			},
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
		pms := NewPromise(pool, Process{
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				if recovered < 100 {
					return nil, errors.New("process err")
				}
				return true, nil
			},
		}).RecoverAndRetry(ExceptionProcess{
			Process: func(ctx context.Context, err error, last interface{}) (interface{}, error) {
				recovered ++
				if recovered >= 100 {
					failed = true
					return nil, errors.New("recover err")
				}
				return nil, nil
			},
		})

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
	t.Run("test wait timeout", func(t *testing.T) {
		gp := &sync.WaitGroup{}
		pool := NewPool(3)
		for i := 0 ; i < 20 ; i ++ {
			gp.Add(1)

			go func() {
				pms := NewPromise(pool, Process{
					Process: func(ctx context.Context, last interface{}) (interface{}, error) {
						time.Sleep(100 * time.Millisecond)
						return true, nil
					},
				})

				ctx, _ := context.WithTimeout(context.Background(), 10 * time.Millisecond)

				err := pms.Wait(ctx, true)
				if err == nil {
					t.Fatalf("wait timeout should return err")
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
		pms := NewPromise(pool, Process{
			EventKey: rand.Int(),
			Partition: true,
			Process: func(ctx context.Context, last interface{}) (interface{}, error) {
				return true, nil
			},
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
}
