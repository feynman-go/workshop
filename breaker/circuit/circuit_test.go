package circuit

import (
	"context"
	"errors"
	"golang.org/x/time/rate"
	"sync/atomic"
	"testing"
	"time"
)

func TestCircuitBasic(t *testing.T) {
	trigger := NewSimpleTrigger(ErrLimiter{
		ErrLimiter: rate.NewLimiter(rate.Every(time.Second), 1),
	}, func(ctx context.Context) Recovery {
		return &HalfOpenRecovery{
			ResetFunc: func(ctx context.Context) *HalfOpener {
				time.Sleep(time.Second)
				return NewHalfOpener(
					rate.NewLimiter(0, 1),
					nil,
					1,
				)
			},
		}
	})

	c := New(trigger)
	if c.Status() != STATUS_OPEN {
		t.Fatal("bad status", c.Status())
	}

	ctx, _ := context.WithTimeout(context.Background(), 100 * time.Millisecond)

	var testErr = errors.New("test err")

	err := c.Do(ctx, func(ctx context.Context) error {
		return testErr
	})
	if err != testErr {
		t.Fatal(err)
	}

	ctx, _ = context.WithTimeout(context.Background(), 100 * time.Millisecond)
	err = c.Do(ctx, func(ctx context.Context) error {
		return testErr
	})
	if err != testErr {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	if c.Status() != STATUS_WAITING_RECOVERY {
		t.Fatal("bad status", c.Status())
	}
	time.Sleep(time.Second)

	var count int32 = 0
	ctx, _ = context.WithTimeout(context.Background(), 100 * time.Millisecond)
	err = c.Do(ctx, func(ctx context.Context) error {
		atomic.AddInt32(&count, 1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	if c.Status() != STATUS_OPEN {
		t.Fatal("bad status", c.Status())
	}
	if count != 1 {
		t.Fatal("bad count")
	}

}
