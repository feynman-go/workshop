package window

import (
	"context"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
	"sync"
	"time"
)

type Aggregator interface {
	Aggregate(ctx context.Context, item Whiteboard, input interface{}) (err error)
	// return new whiteboard data
	Trigger(ctx context.Context, acceptErr error, nextSeq uint64) error
}

type Whiteboard struct {
	StartTime time.Time
	Seq       uint64
	Count     uint64
	AggregateErr error
	TriggerErr error
	HasTriggered bool
	resetChan chan <- struct{}
}

type Trigger interface {
	Accept(item Whiteboard, input interface{})
	Wait(ctx context.Context) (uint64, bool)
	Reset(nextSeq uint64)
}

type Window struct {
	rw          sync.RWMutex
	current     Whiteboard
	aggregator  Aggregator
	triggers    []Trigger
	pb          *prob.Prob
	triggerChan chan uint64
	triggerErr chan struct{}
}

func New(ag Aggregator, triggers []Trigger) *Window {
	return &Window{
		aggregator: ag,
		triggers: triggers,
		triggerChan: make(chan uint64, 0),
		triggerErr: make(chan struct{}, 1),
		current: Whiteboard {
			StartTime: time.Now(),
			Seq: 0,
			Count: 0,
			resetChan: make(chan struct{}),
		},
	}
}

func (w *Window) Accept(ctx context.Context, input interface{}) error {
	w.rw.Lock()
	defer w.rw.Unlock()

	w.start(ctx)

	if w.current.TriggerErr != nil {
		return w.current.TriggerErr
	}
	if w.current.AggregateErr != nil {
		return w.current.AggregateErr
	}

	err := w.aggregator.Aggregate(ctx, w.current, input)
	if err != nil {
		w.current.AggregateErr = err
		return err
	}
	w.current.Count ++
	w.updateTrigger(w.current, input)
	return nil
}

func (w *Window) Close(ctx context.Context) error {
	var seq uint64
	w.rw.Lock()
	seq = w.current.Seq
	w.rw.Unlock()
	w.triggerChan <- seq
	if w.pb != nil {
		w.pb.Stop()
	}
	return nil
}

func (w *Window) ClearErr(ctx context.Context) {
	w.rw.Lock()
	defer w.rw.Unlock()
	w.current.TriggerErr = nil
	w.current.AggregateErr = nil
	if w.current.HasTriggered { // re trigger after clear err
		w.handleTriggerOn(ctx, w.current.Seq, true)
	}
}

func (w *Window) start(ctx context.Context) bool {
	if w.pb == nil {
		w.pb = prob.New(w.run)
		w.rw.Lock()
		w.handleTriggerOn(ctx, 0, false)
		w.rw.Unlock()
	}
	return w.pb.Start()
}

func (w *Window) Current() Whiteboard {
	w.rw.RLock()
	defer w.rw.RUnlock()
	return w.current
}

/*
	<- w.Trigger()
	cur := w.Current()

	if cur.TriggerErr == err... {
		// handle err
		w.ClearErr(ctx)
	} else {
		w.Close(ctx)
	}
*/
func (w *Window) TriggerErr() <- chan struct{} {
	return w.triggerErr
}

func (w *Window) handleTriggerOn(ctx context.Context, seq uint64, canRetry bool) {
	if seq < w.current.Seq {
		return
	}
	if w.current.HasTriggered && !canRetry {
		return
	}

	last := w.current
	newBoard := Whiteboard {
		StartTime: time.Now(),
		Seq:       seq + 1,
		resetChan: make(chan struct{}),
	}

	err := w.aggregator.Trigger(ctx, last.AggregateErr, newBoard.Seq)
	last.HasTriggered = true
	if err != nil {
		last.TriggerErr = err
		w.current = last
		select {
		case w.triggerErr <- struct{}{}:
		default:
		}
		return
	}
	w.current = newBoard
	for _, tg := range w.triggers{
		tg.Reset(newBoard.Seq)
	}
	close(last.resetChan)

}

func (w *Window) runTriggerHandler(ctx context.Context) {
	for {
		select {
		case <- ctx.Done():
			return
		case seq := <- w.triggerChan:
			w.rw.Lock()
			w.handleTriggerOn(ctx, seq, false)
			w.rw.Unlock()
		}
	}
}

func (w *Window) getResetChan() chan <- struct{}{
	w.rw.Lock()
	defer w.rw.Unlock()
	return w.current.resetChan
}

func (w *Window) run(ctx context.Context) {
	fs := []func(ctx context.Context){w.runTriggerHandler }
	for i := range w.triggers {
		tg := w.triggers[i]
		fs = append(fs, func(ctx context.Context) {
			for ctx.Err() == nil {
				seq, ok := tg.Wait(ctx)
				if ok {
					select {
					case w.triggerChan <- seq:
					}
				}
			}
		})
	}
	syncrun.Run(ctx, fs...)
}

func (w *Window) updateTrigger(item Whiteboard, input interface{}) {
	for _, t := range w.triggers {
		t.Accept(item, input)
	}
}

