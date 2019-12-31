package queue

import (
	"container/list"
	"context"
	"github.com/pkg/errors"
	"sync"
)

type Queue struct {
	mx sync.RWMutex
	closed chan struct{}
	l *list.List
	peeked *list.Element
	queueFull chan struct{}
	addNotify chan struct{}
	max int
}

func NewQueue(max int) *Queue {
	return &Queue{
		addNotify: make(chan struct{}, 1),
		closed: make(chan struct{}),
		l: list.New(),
		max: max,
	}
}

func (queue *Queue) Push(ctx context.Context, data interface{}) error {
	for ctx.Err() == nil {
		queue.mx.Lock()
		queueFull := queue.queueFull
		select {
		case <- queue.closed:
			queue.mx.Unlock()
			return errors.New("closed")
		default:
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
		case <- ctx.Done():
		case <- queueFull:
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
		it, ok, err := queue.tryPeek(ctx)
		if err != nil {
			return nil, err
		}
		if ok {
			return it, nil
		}
		select {
		case <- ctx.Done():
			return nil, nil
		case <- queue.closed:
		case <- queue.addNotify:
		}
	}
	return nil, nil
}

func (queue *Queue) tryPeek(ctx context.Context) (interface{}, bool, error) {
	queue.mx.Lock()
	peeked := queue.peeked

	select {
	case <- queue.closed: // not need lock, only for read
		queue.mx.Unlock()
		return nil, false, errors.New("closed")
	default:
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

	queue.mx.Unlock()

	if ret != nil {
		nf := ret.Value
		return nf, true, nil
	}

	return nil, false, nil
}

func (queue *Queue) AckPeeked(ctx context.Context) error {
	queue.mx.Lock()
	defer queue.mx.Unlock()

	select {
	case <- queue.closed:
		return errors.New("closed")
	default:
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

	select {
	case <- queue.closed:
		return errors.New("closed")
	default:
		close(queue.closed)
	}
	return nil
}

func (queue *Queue) LastPeeked() interface{} {
	queue.mx.RLock()
	defer queue.mx.RUnlock()

	nf := queue.peeked.Value
	return &nf
}

func (queue *Queue) isQueueFull() bool {
	if queue.l.Len()  >= queue.max {
		return true
	}
	return false
}