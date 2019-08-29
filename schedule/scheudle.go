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
}

func New(pool *promise.Pool) *Scheduler {
	return &Scheduler{
		l:       list.New(),
		pool:    pool,
		newChan: make(chan struct{}),
	}
}

func (schedule *Scheduler) AddPlan(name string, startTime time.Time, endTime time.Time, action func(ctx context.Context) error) {
	var node = &innerNode{
		key:         name,
		action:      action,
		expectStart: startTime,
		endTime:     endTime,
	}
	schedule.rw.Lock()
	defer schedule.rw.Unlock()

	head := schedule.l.Front()
	for ; head != nil; head = head.Next() {
		in := head.Value.(*innerNode)
		if node.expectStart.Before(in.expectStart) {
			break
		}
	}
	if head != nil {
		schedule.l.InsertBefore(node, head)
	} else {
		schedule.l.PushFront(node)
	}

	select {
	case schedule.newChan <- struct{}{}:
	default:
	}
}

func (schedule *Scheduler) peak() *innerNode {
	schedule.rw.Lock()
	defer schedule.rw.Unlock()

	head := schedule.l.Front()
	if head == nil {
		return nil
	}

	//schedule.l.Remove(head)
	node, ok := head.Value.(*innerNode)
	if !ok {
		return nil
	}

	return node
}

func (schedule *Scheduler) pop() {
	schedule.rw.Lock()
	defer schedule.rw.Unlock()

	head := schedule.l.Front()
	if head == nil {
		return
	}
	schedule.l.Remove(head)
}

func (schedule *Scheduler) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		var (
			headNode = schedule.peak()
			tm       *time.Timer
		)
		if headNode != nil {
			now := time.Now()
			tm = time.NewTimer(headNode.expectStart.Sub(now))
			if !headNode.endTime.IsZero() && headNode.endTime.Before(now) {
				schedule.pop()
				continue
			}
		}
		if tm == nil {
			tm = time.NewTimer(time.Second)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-schedule.newChan:
		case cur := <-tm.C:
			schedule.pop()
			if headNode != nil && headNode.action != nil {
				runCtx, _ := context.WithTimeout(ctx, headNode.endTime.Sub(cur))
				schedule.startAction(runCtx, headNode)
			}
		}
	}
	return ctx.Err()
}

func (schedule *Scheduler) startAction(ctx context.Context, node *innerNode) {
	if node.action != nil {
		p := promise.NewPromise(schedule.pool, func(ctx context.Context, req promise.Request) promise.Result {
			runCtx, _ := context.WithTimeout(ctx, node.endTime.Sub(time.Now()))
			err := node.action(runCtx)
			return promise.Result{
				Err: err,
			}
		})
		p.Start(ctx)
	}
}

type innerNode struct {
	key         string
	action      func(ctx context.Context) error
	expectStart time.Time
	endTime     time.Time
}
