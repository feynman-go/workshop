package schedule

import (
	"container/list"
	"context"
	"github.com/feynman-go/workshop/promise"
	"sync"
	"time"
)

type Scheduler struct {
	rw      sync.RWMutex
	l       *list.List
	pool    *promise.Pool
	newChan chan struct{}
	m *sync.Map
}

func New(pool *promise.Pool) *Scheduler {
	return &Scheduler{
		l:       list.New(),
		pool:    pool,
		newChan: make(chan struct{}, 2),
		m: &sync.Map{},
	}
}

func (sr *Scheduler) AddPlan(startTime time.Time, endTime time.Time, action func(ctx context.Context) error) *Schedule {
	var node = &Schedule{
		action:      action,
		expectStart: startTime,
		endTime:     endTime,
	}
	sr.rw.Lock()
	defer sr.rw.Unlock()

	head := sr.l.Front()
	for ; head != nil; head = head.Next() {
		in := head.Value.(*Schedule)
		if node.expectStart.Before(in.expectStart) {
			break
		}
	}

	if head != nil {
		node.e = sr.l.InsertBefore(node, head)
	} else {
		node.e = sr.l.PushFront(node)
	}

	sr.m.Store(node, true)

	go func() {
		select {
		case sr.newChan <- struct{}{}:
		default:
		}
	}()
	return node
}

func (sr *Scheduler) Remove(schedule Schedule) {
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

func (sr *Scheduler) Run(ctx context.Context) error {
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

func (sr *Scheduler) startAction(ctx context.Context, node *Schedule) {
	if node.action != nil {
		p := promise.NewPromise(sr.pool, func(ctx context.Context, req promise.Request) promise.Result {
			runCtx, _ := context.WithTimeout(ctx, node.endTime.Sub(time.Now()))
			err := node.action(runCtx)
			return promise.Result{
				Err: err,
			}
		})
		p.Start(ctx)
	}
}

type Schedule struct {
	e *list.Element
	action      func(ctx context.Context) error
	expectStart time.Time
	endTime     time.Time
}