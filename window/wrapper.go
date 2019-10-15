package window

import (
	"context"
	"sync"
	"time"
)

var _ Acceptor = (*DurationTrigger)(nil)
type DurationTrigger struct {
	rw        sync.RWMutex
	startTime time.Time
	tm        *time.Timer
	trigger   *Trigger
	seq       uint64
	duration  time.Duration
	inner     Acceptor
}

func DurationWrapper(duration time.Duration) Wrapper {
	return func(in Acceptor) Acceptor {
		trigger := &DurationTrigger {
			duration: duration,
			inner: in,
		}
		return trigger
	}
}

func (trigger *DurationTrigger) refreshTimer() {
	if trigger.tm != nil {
		if !trigger.tm.Stop() {
			select {
			case <- trigger.tm.C:
			default:
			}
		}
	}

	trigger.tm = time.AfterFunc(trigger.duration, func() {
		trigger.rw.RLock()
		defer trigger.rw.RUnlock()
		if t := trigger.trigger; t != nil && !t.Triggered() {
			t.Trigger()
		}
		trigger.tm.Reset(trigger.duration)
	})
	return
}

func (trigger *DurationTrigger) Accept(ctx context.Context, input interface{}) (err error) {
	trigger.rw.Lock()
	if trigger.tm == nil {
		trigger.refreshTimer()
	}
	trigger.rw.Unlock()

	if trigger.inner != nil {
		err = trigger.inner.Accept(ctx, input)
	}
	return err
}

func (trigger *DurationTrigger) Reset(whiteboard Whiteboard) {
	if trigger.inner != nil {
		trigger.inner.Reset(whiteboard)
	}

	trigger.rw.Lock()
	defer trigger.rw.Unlock()

	if trigger.tm != nil {
		trigger.tm.Stop()
		trigger.tm = nil
	}
	trigger.seq = whiteboard.Seq
	trigger.trigger = whiteboard.Trigger
	return
}

var _ Acceptor = (*CountAggregator)(nil)
type CountAggregator struct {
	rw       sync.RWMutex
	maxCount uint64
	seq      uint64
	count    uint64
	inner    Acceptor
	trigger *Trigger
}

func CounterWrapper(maxCount uint64) Wrapper {
	return func(in Acceptor) Acceptor {
		return &CountAggregator{
			maxCount: maxCount,
			inner: in,
		}
	}
}

func (ca *CountAggregator) Accept(ctx context.Context, input interface{}) (err error) {
	ca.rw.Lock()
	defer ca.rw.Unlock()

	if ca.inner != nil {
		err = ca.inner.Accept(ctx, input)
		if err != nil {
			return err
		}
	}

	ca.count ++
	ct := ca.count
	if ca.maxCount <= ct {
		ca.trigger.Trigger()
	}
	return nil
}

func (ca *CountAggregator) Reset(whiteboard Whiteboard) {
	ca.rw.Lock()
	defer ca.rw.Unlock()
	defer func() {
		ca.count = 0
	}()

	if ca.inner != nil {
		ca.inner.Reset(whiteboard)
	}
	ca.trigger = whiteboard.Trigger
	ca.seq = whiteboard.Seq
}
