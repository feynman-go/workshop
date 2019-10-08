package window

import (
	"context"
	"github.com/pkg/errors"
	"sync/atomic"
	"testing"
	"time"
)

var _ Aggregator = (*MockAggregator)(nil)

type MockAggregator struct {
	AggregateFunc func(ctx context.Context, input interface{}, item Whiteboard) (err error)
	OnTriggerFunc func(ctx context.Context) error
}

func (aggregator MockAggregator) Aggregate(ctx context.Context, input interface{}, item Whiteboard, ) (err error) {
	if aggregator.AggregateFunc == nil {
		return nil
	}
	return aggregator.AggregateFunc(ctx, input, item)
}

func (aggregator MockAggregator) OnTrigger(ctx context.Context) (err error) {
	if aggregator.OnTriggerFunc == nil {
		return nil
	}
	return aggregator.OnTriggerFunc(ctx)
}


func TestWindowInputAndTrigger(t *testing.T) {
	var aggregated int32
	var triggered int32
	wd := New(MockAggregator{
		AggregateFunc: func(ctx context.Context, input interface{}, item Whiteboard) error {
			if atomic.AddInt32(&aggregated, 1) > 2 {
				item.Trigger.Trigger()
			}
			return nil
		},
		OnTriggerFunc: func(ctx context.Context) error {
			atomic.AddInt32(&triggered, 1)
			return nil
		},
	})

	err := wd.Accept(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	err = wd.Accept(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	err = wd.Accept(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	if aggregated != 3 {
		t.Fatal("bad aggregated count", aggregated)
	}

	if triggered != 1 {
		t.Fatal("bad triggered count", triggered)
	}
}

func TestWindowInputAndTriggerErr(t *testing.T) {
	var aggregated int32
	var triggered int32
	wd := New(MockAggregator{
		AggregateFunc: func(ctx context.Context, input interface{}, item Whiteboard) error {
			if atomic.AddInt32(&aggregated, 1) > 2 {
				item.Trigger.Trigger()
			}
			return nil
		},
		OnTriggerFunc: func(ctx context.Context) error {
			if atomic.AddInt32(&triggered, 1) == 1 {
				return errors.New("trigger err")
			}
			return nil
		},
	})

	err := wd.Accept(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	err = wd.Accept(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	err = wd.Accept(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	if aggregated != 3 {
		t.Fatal("bad aggregated count", aggregated)
	}

	if triggered != 1 {
		t.Fatal("bad triggered count", triggered)
	}

	err = wd.Accept(context.Background(), nil)
	if err == nil {
		t.Fatal("accept should err after trigger err")
	}

	wd.ClearErr(context.Background())
	time.Sleep(100 * time.Millisecond)

	err = wd.Accept(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	if triggered != 3 {
		t.Fatal("bad triggered count", triggered)
	}
}

func TestWindowInvokeCount(t *testing.T) {
	var triggered int32
	wd := New(MockAggregator{
		OnTriggerFunc: func(ctx context.Context) error {
			atomic.AddInt32(&triggered, 1)
			return nil
		},
	}, CounterWrapper(3))

	err := wd.Accept(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	if triggered != 0 {
		t.Fatal("bad triggered count", triggered)
	}

	err = wd.Accept(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	if triggered != 0 {
		t.Fatal("bad triggered count", triggered)
	}

	err = wd.Accept(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	if triggered != 1 {
		t.Fatal("bad triggered count", triggered)
	}

	err = wd.Accept(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	if triggered != 1 {
		t.Fatal("bad triggered count", triggered)
	}

	err = wd.Accept(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	if triggered != 1 {
		t.Fatal("bad triggered count", triggered)
	}

	err = wd.Accept(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	if triggered != 2 {
		t.Fatal("bad triggered count", triggered)
	}
}



func TestWindowInvokeZeroCount(t *testing.T) {
	var triggered int32
	wd := New(MockAggregator{
		OnTriggerFunc: func(ctx context.Context) error {
			atomic.AddInt32(&triggered, 1)
			return nil
		},
	}, CounterWrapper(0))
	err := wd.Accept(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	if triggered != 1 {
		t.Fatal("bad triggered count", triggered)
	}
}