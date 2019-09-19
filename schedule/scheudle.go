package schedule

import (
	"context"
	"github.com/feynman-go/workshop/syncrun/prob"
	"log"
	"sync"
	"time"
)

type List interface {
	Front() Element
	Insert(*Schedule) Element
}

type Element interface {
	Next() Element
	IsHead() bool
	Schedule() Schedule
}

type Scheduler struct {
	rw      sync.RWMutex
	pb 		*prob.Prob
	newChan chan struct{}
	l List
}

func New(list List) *Scheduler {
	return &Scheduler{
		l:       list,
		newChan: make(chan struct{}, 2),
	}
}

func (sr *Scheduler) Start() bool {
	sr.rw.Lock()
	defer sr.rw.Unlock()
	if sr.pb == nil {
		sr.pb = prob.New(func(ctx context.Context) {
			err := sr.run(ctx)
			if err != nil {
				log.Println("run err")
			}
		})
		return sr.pb.Start()
	}
	return sr.pb.Start()
}

func (sr *Scheduler) Close(ctx context.Context) error {
	sr.rw.Lock()
	if sr.pb == nil {
		sr.rw.Unlock()
		return nil
	}
	sr.pb.Stop()
	sr.rw.Unlock()
	select {
	case <- sr.pb.Stopped():
		return nil
	case <- ctx.Done():
		return ctx.Err()
	}
}

func (sr *Scheduler) AddPlan(startTime time.Time, endTime time.Time) *Schedule {
	var node = &Schedule{
		expectStart: startTime,
		endTime:     endTime,
	}
	sr.rw.Lock()
	defer sr.rw.Unlock()

	sr.l.Insert(node)

	go func() {
		select {
		case sr.newChan <- struct{}{}:
		default:
		}
	}()
	return node
}

func (sr *Scheduler) Remove(schedule *Schedule) {
	sr.rw.Lock()
	defer sr.rw.Unlock()

	sr.l.Remove(schedule.e)
	sr.m.Delete(schedule)
}


func (sr *Scheduler) peak() *Schedule {
	sr.rw.Lock()
	defer sr.rw.Unlock()

	head := sr.l.Front()
	if head == nil {
		return nil
	}

	//sr.l.Remove(head)
	node, ok := head.Value.(*Schedule)
	if !ok {
		return nil
	}

	return node
}

func (sr *Scheduler) pop() {
	sr.rw.Lock()
	defer sr.rw.Unlock()

	head := sr.l.Front()
	if head == nil {
		return
	}
	sr.l.Remove(head)
}

func (sr *Scheduler) run(ctx context.Context) error {
	for ctx.Err() == nil {
		var (
			headNode = sr.peak()
			tm       *time.Timer
		)
		if headNode != nil {
			now := time.Now()
			tm = time.NewTimer(headNode.expectStart.Sub(now))
			if !headNode.endTime.IsZero() && headNode.endTime.Before(now) {
				sr.pop()
				continue
			}
		}
		if tm == nil {
			tm = time.NewTimer(time.Second)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sr.newChan:
		case cur := <-tm.C:
			sr.pop()

			_, ok := sr.m.Load(headNode)
			if headNode != nil && headNode.action != nil && ok {
				runCtx, _ := context.WithTimeout(ctx, headNode.endTime.Sub(cur))
				sr.startAction(runCtx, headNode)
			}
		}
	}
	return ctx.Err()
}

type Schedule struct {
	expectStart time.Time
	endTime     time.Time
	element     Element
}

type comparer struct {}

func (c comparer) Descending() bool {
	return false
}

func (c comparer) Compare(lhs, rhs interface{}) bool {
	l := lhs.(*Schedule)
	r := lhs.(*Schedule)
	return l.endTime.After(r.endTime)
}