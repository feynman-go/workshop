package window

import (
	"context"
	"sync"
	"time"
)

var _ Trigger = (*DurationTrigger)(nil)
type DurationTrigger struct {
	rw sync.RWMutex
	startTime time.Time
	tm *time.Timer
	seq uint64
	duration time.Duration
}

func NewDurationTrigger(duration time.Duration) *DurationTrigger {
	return &DurationTrigger {
		duration: duration,
	}
}

func (trigger *DurationTrigger) Accept(item Whiteboard, input interface{}) {}

func (trigger *DurationTrigger) Wait(ctx context.Context) (uint64, bool) {
	select {
	case <- ctx.Done():
		return 0, false
	case <- trigger.tm.C:
		trigger.rw.Lock()
		defer trigger.rw.Unlock()
		return trigger.seq, true
	}
}

func (trigger *DurationTrigger) Reset(nextSeq uint64) {
	trigger.rw.Lock()
	defer trigger.rw.Unlock()

	if trigger.tm != nil {
		if !trigger.tm.Stop() {
			select {
			case <- trigger.tm.C:
			default:
			}
		}
		trigger.seq = nextSeq
		trigger.tm.Reset(trigger.duration)
	}
	return

}

var _ Trigger = (*CountTrigger)(nil)
type CountTrigger struct {
	rw sync.RWMutex
	maxCount uint64
	seq uint64
	cn chan uint64
	count uint64
}

func NewCounterTrigger(maxCount uint64) *CountTrigger {
	return &CountTrigger {
		maxCount: maxCount,
		cn: make(chan uint64, 2),
	}
}


func (trigger *CountTrigger) Accept(item Whiteboard, input interface{}) {
	trigger.rw.Lock()
	defer trigger.rw.Unlock()
	if trigger.seq > item.Seq {
		return
	}

	trigger.count ++
	if trigger.maxCount < trigger.count {
		select {
		case trigger.cn <- trigger.seq:
		default:
		}
	}
}

func (trigger *CountTrigger) IsCountOverMax() (bool, uint64) {
	trigger.rw.Lock()
	defer trigger.rw.Unlock()
	return trigger.maxCount < trigger.count, trigger.seq
}

func (trigger *CountTrigger) Wait(ctx context.Context) (uint64, bool) {
	if ok, seq := trigger.IsCountOverMax(); ok {
		return seq, true
	}
	select {
	case <- ctx.Done():
		return 0, false
	case seq := <- trigger.cn:
		if ok, seq := trigger.IsCountOverMax(); ok {
			return seq, true
		}
		return seq, true
	}
}

func (trigger *CountTrigger) Reset(nextSeq uint64) {
	trigger.rw.Lock()
	defer trigger.rw.Unlock()
	trigger.seq = nextSeq
	trigger.count = 0
}