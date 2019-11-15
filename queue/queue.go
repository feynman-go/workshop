package queue

import (
	"container/list"
	"context"
	"github.com/pkg/errors"
	"sync"
)

type Queue struct {
	mx sync.RWMutex
	closed bool
	l *list.List
	peeked *list.Element
	queueFull chan struct{}
	addNotify chan struct{}
	max int
}

func NewQueue(max int) *Queue {
	return &Queue{
		addNotify: make(chan struct{}, 1),
		l: list.New(),
		max: max,
	}
}

func (queue *Queue) Push(ctx context.Context, data interface{}) error {
	for ctx.Err() == nil {
		queue.mx.Lock()
		closed := queue.closed
		queueFull := queue.queueFull
		if closed {
			queue.mx.Unlock()
			return errors.New("closed")
		}
		if queueFull == nil {
			queue.l.PushFront(data)
			select {
			case queue.addNotify <- struct{}{}:
			default:
			}
			queue.mx.Unlock()
			return nil
		}
		queue.mx.Unlock()

		select {
		case <- queueFull:
		case <- ctx.Done():
			return ctx.Err()
		}
	}
	return ctx.Err()
}

func (queue *Queue) WaitOk(ctx context.Context) error {
	queue.mx.RLock()
	defer queue.mx.RUnlock()
	cn := queue.queueFull
	if cn == nil {
		return ctx.Err()
	}
	select {
	case <- ctx.Done():
		return ctx.Err()
	case <- cn:
		return nil
	}
}

func (queue *Queue) Peek(ctx context.Context) (interface{}, error) {
	for ctx.Err() == nil {
		nf, err := queue.tryPeek(ctx)
		if err != nil {
			return nil, err
		}
		if nf != nil {
			return nf, nil
		}
	}
	return nil, nil
}

func (queue *Queue) tryPeek(ctx context.Context) (interface{}, error) {
	queue.mx.Lock()
	defer  queue.mx.Unlock()

	closed := queue.closed
	peeked := queue.peeked
	if closed {
		return nil, errors.New("closed")
	}

	var ret *list.Element
	if peeked == nil {
		ret = queue.l.Back()
	} else {
		ret = peeked.Prev()
	}
	if ret != nil {
		queue.peeked = ret
	}

	if ret == nil {
		select {
		case <- queue.addNotify:
		case <- ctx.Done():
		}
		return nil, nil
	} else {
		nf := ret.Value
		return nf, nil
	}
}

func (queue *Queue) AckPeeked(ctx context.Context) error {
	queue.mx.Lock()
	defer  queue.mx.Unlock()

	if queue.closed {
		return nil
	}

	queueFull := queue.queueFull
	if queue.peeked == nil {
		return nil
	}
	for back := queue.l.Back(); back != nil ; back = queue.l.Back(){
		queue.l.Remove(back)
		if back == queue.peeked {
			break
		}
	}
	if queue.isQueueFull() {
		if queueFull == nil {
			queue.queueFull = make(chan struct{})
		}
	} else {
		if queueFull != nil {
			close(queueFull)
			queue.queueFull = nil
		}
	}
	return nil
}

func (queue *Queue) ResetPeeked() {
	queue.mx.Lock()
	defer  queue.mx.Unlock()
	queue.peeked = nil
}

func (queue *Queue) CloseWithContext(ctx context.Context) error {
	queue.mx.Lock()
	defer queue.mx.Unlock()
	if !queue.closed {
		queue.closed = true
	}
	return nil
}

func (queue *Queue) LastPeeked() interface{} {
	nf := queue.peeked.Value
	return &nf
}

func (queue *Queue) isQueueFull() bool {
	if queue.l.Len()  >= queue.max {
		return true
	}
	return false
}