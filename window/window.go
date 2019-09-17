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
	ResetData(ctx context.Context, last Whiteboard) interface{}
}

type Window struct {
	rw          sync.RWMutex
	current     Whiteboard
	aggregator  Aggregator
	waitChan    chan Whiteboard
	triggers    []Trigger
	pb          *prob.Prob
	triggerChan chan uint64
}

func New(ag Aggregator, whiteBoardData interface{} , triggers []Trigger) *Window {
	return &Window{
		aggregator: ag,
		triggers: triggers,
		triggerChan: make(chan uint64, 0),
		current: Whiteboard {
			StartTime: time.Now(),
			Seq: 0,
			Count: 0,
			resetChan: make(chan struct{}),
			Data: whiteBoardData,
		},
	}
}

func (w *Window) Accept(ctx context.Context, input interface{}) error {
	w.rw.Lock()
	defer w.rw.Unlock()

	w.start()
	err := w.aggregator.Aggregate(ctx, w.current, input)
	if err != nil {
		return err
	}
	w.current.Count ++
	w.updateTrigger(w.current, input)
	return nil
}

func (w *Window) Close(ctx context.Context, input interface{}) error {
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

func (w *Window) WaitChan() <- chan Whiteboard {
	return w.waitChan
}

type Whiteboard struct {
	StartTime time.Time
	LastData  interface{}
	Data      interface{}
	Seq       uint64
	Count     int32
	resetChan chan <- struct{}
}

type Trigger interface {
	Accept(item Whiteboard, input interface{})
	Wait(ctx context.Context) (uint64, bool)
	Reset(nextSeq uint64)
}

func (w *Window) start() bool {
	if w.pb == nil {
		w.pb = prob.New(w.run)
	}
	return w.pb.Start()
}

func (w *Window) handleTriggerOn(ctx context.Context, seq uint64) {
	w.rw.Lock()
	defer w.rw.Unlock()

	if seq == w.current.Seq {
		last := w.current
		w.current = Whiteboard{
			LastData: last,
			Data: w.aggregator.ResetData(ctx, last),
			StartTime: time.Now(),
			Seq:       last.Seq + 1,
			resetChan: make(chan struct{}),
		}
		for _, tg := range w.triggers{
			tg.Reset(w.current.Seq)
		}
		close(last.resetChan)
		w.waitChan <- last
	}
}

func (w *Window) runTriggerHandler(ctx context.Context) {
	for {
		select {
		case <- ctx.Done():

		case seq := <- w.triggerChan:
			w.handleTriggerOn(ctx, seq)
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
			for ctx.Err() != nil {
				seq, ok := tg.Wait(ctx)
				if ok {
					select {
					case <- w.triggerChan:
					case w.triggerChan	<- seq:
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
