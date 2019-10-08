package window

import (
	"context"
	"fmt"
	"github.com/feynman-go/workshop/mutex"
	"github.com/feynman-go/workshop/syncrun/prob"
	"github.com/pkg/errors"
	"sync"
	"time"
)

type Aggregator interface {
	Aggregate(ctx context.Context, input interface{}, item Whiteboard) (err error)
	OnTrigger(ctx context.Context) error
}

type EmptyAggregator struct {}
func (agg EmptyAggregator) Aggregate(ctx context.Context, input interface{}, item Whiteboard) (err error){return nil}
func (agg EmptyAggregator) OnTrigger(ctx context.Context) error{return nil}

type Wrapper func(Aggregator) Aggregator

type Trigger struct {
	rw sync.RWMutex
	triggered bool
	cn chan struct{}
}

func (tm *Trigger) Trigger() {
	tm.rw.Lock()
	defer tm.rw.Unlock()

	if !tm.triggered {
		select {
		case tm.cn <- struct{}{}:
		default:
		}
		tm.triggered = true
	}
}

func (tm *Trigger) Triggered() bool {
	tm.rw.RLock()
	defer tm.rw.RUnlock()
	return tm.triggered
}

type Whiteboard struct {
	StartTime    time.Time
	Seq          uint64
	TriggerErr   error
	HasTriggered bool
	Trigger      *Trigger
}

type Window struct {
	current     Whiteboard
	needTrigger bool
	aggregator  Aggregator
	pb          *prob.Prob
	triggerChan chan struct{}
	mx          *mutex.Mutex
}

func New(ag Aggregator, wraps ...Wrapper) *Window {
	triggerChan := make(chan struct{}, 1)

	if ag == nil {
		ag = EmptyAggregator{}
	}

	for _, w := range wraps {
		ag = w(ag)
	}

	wd := &Window{
		aggregator: ag,
		mx: &mutex.Mutex{},
		triggerChan: triggerChan,
		current: Whiteboard {
			StartTime: time.Now(),
			Seq: 0,
			Trigger: &Trigger{
				cn: triggerChan,
			},
		},
	}
	wd.pb = prob.New(wd.runTriggerHandler)
	wd.pb.Start()
	return wd
}

func (w *Window) Accept(ctx context.Context, input interface{}) error {
	var retErr error
	prob.RunSync(ctx, w.pb, func(ctx context.Context) {
		if w.mx.Hold(ctx) {
			defer w.mx.Release()
			if err := w.prepareAccept(ctx); err != nil {
				retErr = err
				return
			}

			err := w.aggregator.Aggregate(ctx, input, w.current)
			if err != nil {
				retErr = err
				return
			}
		} else {
			if ctx.Err() != nil {
				retErr = ctx.Err()
				return
			}
			retErr = errors.New("hold mutex err")
			return
		}
	})

	return retErr

}

func (w *Window) Close(ctx context.Context) error {
	if w.mx.Hold(ctx) {
		defer w.mx.Release()
		w.aggregator.OnTrigger(ctx)
		w.pb.Stop()
	}
	return nil
}

func (w *Window) Closed() <- chan struct{} {
	return w.pb.Stopped()
}

func (w *Window) ClearErr(ctx context.Context) bool {
	if w.mx.Hold(ctx) {
		defer w.mx.Release()
		w.current.TriggerErr = nil
		if w.current.HasTriggered { // re Trigger after clear err
			return w.handleTriggerOn(ctx, w.current.Seq, true)
		}
		return true
	}
	return false
}

func (w *Window) prepareAccept(ctx context.Context) error {
	if w.current.TriggerErr != nil {
		return w.current.TriggerErr
	}
	if w.current.Trigger.Triggered() {
		if w.handleTriggerOn(ctx, w.current.Seq, false) {
			return nil
		}
		return fmt.Errorf("failed to Trigger current %v", w.current.TriggerErr)
	}
	return nil
}

func (w *Window) Current(ctx context.Context) (Whiteboard, bool) {
	if w.mx.Hold(ctx) {
		defer w.mx.Release()
		return w.current, true
	}
	return Whiteboard{}, false
}

func (w *Window) handleTriggerOn(ctx context.Context, seq uint64, canRetry bool) bool {
	var ok bool

	prob.RunSync(ctx, w.pb, func(ctx context.Context) {
		if seq < w.current.Seq {
			ok = false
			return
		}
		if w.current.HasTriggered && !canRetry {
			ok = false
			return
		}

		last := w.current
		newSeq := seq + 1
		newBoard := Whiteboard {
			StartTime: time.Now(),
			Seq:       newSeq,
			Trigger: &Trigger{
				cn: w.triggerChan,
			},
		}

		err := w.aggregator.OnTrigger(ctx)
		last.HasTriggered = true
		if err != nil {
			last.TriggerErr = err
			w.current = last
			ok = false
			return
		}
		w.current = newBoard
		ok = true
		return
	})
	return ok
}

var index = 0

func (w *Window) runTriggerHandler(ctx context.Context) {
	for {
		select {
		case <- ctx.Done():
			return
		case <- w.triggerChan:
			index++
			if w.mx.Hold(ctx) {
				w.handleTriggerOn(ctx, w.current.Seq, false)
				w.mx.Release()
			}
		}
	}
}