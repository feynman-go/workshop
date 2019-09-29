package window

import (
	"context"
	"sync"
	"time"
)

var _ Aggregator = (*DurationTrigger)(nil)
type DurationTrigger struct {
	rw sync.RWMutex
	startTime time.Time
	tm *time.Timer
	trigger *Trigger
	seq uint64
	duration time.Duration
	inner Aggregator
}

func DurationWrapper(duration time.Duration) Wrapper {
	return func(in Aggregator) Aggregator {
		return &DurationTrigger {
			duration: duration,
			inner: in,
		}
	}
}

func (trigger *DurationTrigger) Aggregate(ctx context.Context, input interface{}, item Whiteboard) (err error) {
	if trigger.inner != nil {
		return trigger.inner.Aggregate(ctx, input, item)
	}

	trigger.rw.Lock()
	defer trigger.rw.Unlock()

	trigger.seq = item.Seq
	trigger.trigger = item.Trigger

	if trigger.tm == nil {
		trigger.tm = time.AfterFunc(trigger.duration, func() {
			trigger.rw.RLock()
			defer trigger.rw.RUnlock()
			if t := trigger.trigger; t != nil && !t.Triggered(){
				t.Trigger()
			}
		})
	}

	return nil
}

func (trigger *DurationTrigger) OnTrigger(ctx context.Context) error {
	if trigger.inner != nil {
		err := trigger.inner.OnTrigger(ctx)
		if err != nil {
			return err
		}
	}

	trigger.rw.Lock()
	defer trigger.rw.Unlock()

	if trigger.tm != nil {
		if !trigger.tm.Stop() {
			select {
			case <- trigger.tm.C:
			default:
			}
		}
		trigger.tm.Reset(trigger.duration)
		trigger.trigger = nil
	}
	return nil
}


var _ Aggregator = (*CountAggregator)(nil)
type CountAggregator struct {
	rw sync.RWMutex
	maxCount uint64
	seq uint64
	count uint64
	inner Aggregator
}

func CounterWrapper(maxCount uint64) Wrapper {
	return func(in Aggregator) Aggregator {
		return &CountAggregator{
			maxCount: maxCount,
			inner: in,
		}
	}
}

func (ca *CountAggregator) Aggregate(ctx context.Context, input interface{}, item Whiteboard) (err error) {
	ca.rw.Lock()
	defer ca.rw.Unlock()

	if ca.inner != nil {
		err = ca.inner.Aggregate(ctx, input, item)
		if err != nil {
			return err
		}
	}

	ca.seq = item.Seq
	ca.count ++
	ct := ca.count
	if ca.maxCount <= ct {
		item.Trigger.Trigger()
	}
	return nil
}

func (ca *CountAggregator) OnTrigger(ctx context.Context) error {
	ca.rw.Lock()
	defer ca.rw.Unlock()

	if ca.inner != nil {
		return ca.inner.OnTrigger(ctx)
	}


	ca.count = 0
	return nil
}
