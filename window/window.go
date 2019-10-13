package window

import (
	"context"
	"github.com/feynman-go/workshop/mutex"
	"github.com/feynman-go/workshop/syncrun/prob"
	"github.com/pkg/errors"
	"sync"
	"time"
)

type Acceptor interface {
	Accept(ctx context.Context, input interface{}) (err error)
	Reset(whiteboard Whiteboard)
}

type Materializer interface {
	Materialize(ctx context.Context) error
}

type EmptyAggregator struct{}

func (agg EmptyAggregator) Accept(ctx context.Context, input interface{}) (err error) { return nil }
func (agg EmptyAggregator) Reset(whiteboard Whiteboard)                               { return }
func (agg EmptyAggregator) Materialize(ctx context.Context) error                     { return nil }

type Wrapper func(Acceptor) Acceptor
type MaterializerWrapper func(Materializer) Materializer

type Trigger struct {
	rw        sync.RWMutex
	triggered bool
	cn        chan struct{}
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

func (tm *Trigger) ResetTrigger() {
	tm.rw.Lock()
	defer tm.rw.Unlock()
	tm.triggered = false
}

func (tm *Trigger) Triggered() bool {
	tm.rw.RLock()
	defer tm.rw.RUnlock()
	return tm.triggered
}

type Whiteboard struct {
	StartTime time.Time
	Seq       uint64
	Trigger   *Trigger
	Err       error
	triggered chan struct{}
}

type Window struct {
	current       Whiteboard
	needTrigger   bool
	acceptor      Acceptor
	pb            *prob.Prob
	triggerChan   chan struct{}
	mx            *mutex.Mutex
	materializer  Materializer
	errClearChan  chan struct{}
}

func New(ag Acceptor, materializer Materializer, wraps ...Wrapper) *Window {
	triggerChan := make(chan struct{}, 1)

	if ag == nil {
		ag = EmptyAggregator{}
	}

	for _, w := range wraps {
		ag = w(ag)
	}

	wd := &Window{
		acceptor:    ag,
		mx:          &mutex.Mutex{},
		triggerChan: triggerChan,
		current: Whiteboard{
			StartTime: time.Now(),
			Seq:       0,
			Trigger: &Trigger{
				cn: triggerChan,
			},
		},
		materializer: materializer,
	}
	wd.pb = prob.New(wd.runTriggerHandler)
	ag.Reset(wd.current)
	wd.pb.Start()
	return wd
}

func (w *Window) Accept(ctx context.Context, input interface{}) error {
	var retErr error
	runSync := prob.RunSync(ctx, w.pb, func(ctx context.Context) {
		if w.mx.HoldForRead(ctx) {
			defer w.mx.ReleaseForRead()

			if w.current.Err != nil {
				retErr = w.current.Err
				return
			}

			err := w.acceptor.Accept(ctx, input)
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

	if !runSync {
		return errors.New("closed")
	}

	return retErr
}

func (w *Window) CloseWithContext(ctx context.Context) error {
	w.pb.Stop()
	select {
	case <- ctx.Done():
		return ctx.Err()
	case <- w.pb.Stopped():
		return nil
	}
}

func (w *Window) WaitUntilOk(ctx context.Context) error {
	if w.mx.HoldForRead(ctx) {
		clearChan := w.errClearChan
		w.mx.ReleaseForRead()

		if clearChan == nil {
			return nil
		}

		select {
		case <- ctx.Done():
			return ctx.Err()
		case <- clearChan:
			return nil
		}
	}
	return errors.New("hold err")
}

func (w *Window) Closed() <-chan struct{} {
	return w.pb.Stopped()
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

		if w.materializer != nil {
			err := w.materializer.Materialize(ctx)
			if err != nil {
				w.current.Err = err
				if w.errClearChan == nil {
					w.errClearChan = make(chan struct{})
				}
				ok = false
				return
			}
		}

		newSeq := seq + 1
		newBoard := Whiteboard{
			StartTime: time.Now(),
			Seq:       newSeq,
			Trigger: &Trigger{
				cn: w.triggerChan,
			},
		}
		w.acceptor.Reset(newBoard)
		w.current = newBoard
		ok = true

		if w.errClearChan != nil {
			close(w.errClearChan)
		}
		w.errClearChan = nil
		return
	})

	return ok
}

func (w *Window) runTriggerHandler(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.triggerChan:
			if w.mx.Hold(ctx) {
				w.handleTriggerOn(ctx, w.current.Seq, false)
				w.current.Trigger.ResetTrigger()
				w.mx.Release()
			}
		}
	}
}
