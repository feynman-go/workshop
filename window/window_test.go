package window

import (
	"context"
	"github.com/pkg/errors"
	"sync/atomic"
	"testing"
	"time"
)

var _ Acceptor = (*MockAggregator)(nil)

type MockAggregator struct {
	AcceptFunc      func(ctx context.Context, input interface{}) (err error)
	ResetFunc       func(whiteboard Whiteboard)
	MaterializeFunc func(ctx context.Context) error
}

func (aggregator MockAggregator) Accept(ctx context.Context, input interface{}) (err error) {
	if aggregator.AcceptFunc == nil {
		return nil
	}
	return aggregator.AcceptFunc(ctx, input)
}

func (aggregator MockAggregator) Reset(whiteboard Whiteboard) {
	if aggregator.ResetFunc == nil {
		return
	}
	aggregator.ResetFunc(whiteboard)
	return
}

func (aggregator MockAggregator) Materialize(ctx context.Context) error {
	if aggregator.MaterializeFunc == nil {
		return nil
	}
	return aggregator.MaterializeFunc(ctx)
}


func TestWindowInputAndTrigger(t *testing.T) {
	var aggregated int32
	var triggered int32
	var wb Whiteboard
	wd := New(MockAggregator{
		ResetFunc: func(whiteboard Whiteboard) {
			wb = whiteboard
			return
		},
		AcceptFunc: func(ctx context.Context, input interface{}) error {
			if atomic.AddInt32(&aggregated, 1) > 2 {
				wb.Trigger.Trigger()
			}
			return nil
		},
	}, MockAggregator{
		MaterializeFunc: func(ctx context.Context) error {
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
	var wb Whiteboard
	wd := New(MockAggregator{
		AcceptFunc: func(ctx context.Context, input interface{}) error {
			if atomic.AddInt32(&aggregated, 1) > 2 {
				wb.Trigger.Trigger()
			}
			return nil
		},
		ResetFunc: func(whiteboard Whiteboard) {
			wb = whiteboard
		},

	}, MockAggregator{
		MaterializeFunc: func(ctx context.Context) error {
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

	err = wd.Accept(context.Background(), nil)
	if err == nil {
		t.Fatal("should return err")
	}

	wb.Trigger.Trigger()
	time.Sleep(100 * time.Millisecond)

	if aggregated != 3 {
		t.Fatal("bad aggregated count", aggregated)
	}

	if triggered != 2 {
		t.Fatal("bad triggered count", triggered)
	}

	err = wd.Accept(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	if triggered != 3 {
		t.Fatal("bad triggered count", triggered)
	}
}

func TestWindowInvokeCount(t *testing.T) {
	var triggered int32
	ag := MockAggregator{
		MaterializeFunc: func(ctx context.Context) error {
			atomic.AddInt32(&triggered, 1)
			return nil
		},
	}
	wd := New(ag, ag, CounterWrapper(3))

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
	wd := New(MockAggregator{},  MockAggregator{
		MaterializeFunc: func(ctx context.Context) error {
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