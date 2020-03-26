package notify

import (
	"container/list"
	"context"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
	"github.com/pkg/errors"
	"log"
	"sync"
	"time"
)

const (
	DefaultMaxQueueCount = 100
)
/*type Publisher interface {
	Publish(ctx context.Context, notifications []Notification) (err error)
}*/

// not multi goroutine safe
type Iterator struct {
	pb        *routine.Prob
	stream    OutputStream
	option    Option
	queue 	*Queue
}

type Option struct {
	MaxQueueCount int
	StreamMiddles []OutputStreamMiddle
	CloseTimeOut  time.Duration
}

// init iterator is empty
func NewIterator(stream OutputStream, option Option) *Iterator {
	if option.MaxQueueCount == 0 {
		option.MaxQueueCount = DefaultMaxQueueCount
	}
	for _, mid := range option.StreamMiddles {
		stream = mid.WrapStream(stream)
	}
	ret := &Iterator{
		stream:    stream,
		queue: NewQueue(option.MaxQueueCount),
	}
	ret.pb = routine.New(ret.run)
	ret.pb.Start()
	return ret
}

func (iterator *Iterator) Peek(ctx context.Context) (*Notification, error) {
	return iterator.queue.Peek(ctx)
}

func (iterator *Iterator) ResetPeeked() {
	iterator.queue.ResetPeeked()
}

func (iterator *Iterator) PullPeeked(ctx context.Context) error {
	return iterator.queue.PullPeeked(ctx)
}

func (iterator *Iterator) start() {
	iterator.pb.Start()
}

func (iterator *Iterator) commit(ctx context.Context, batch []Notification) error {
	return iterator.stream.CommitOutput(ctx, batch)
}

func (iterator *Iterator) run(ctx context.Context) {
	syncrun.FuncWithReStart(func(ctx context.Context) bool {
		err := iterator.queue.WaitOk(ctx)
		if err != nil {
			log.Println("wait window ok err:", err)
			return true
		}

		stream := iterator.stream
		cursor, err := stream.FetchOutputCursor(ctx)
		if err != nil {
			log.Println("fetch output cursor")
			return true
		}

		defer func() {
			closeCtx := context.Background()
			if iterator.option.CloseTimeOut != 0 {
				closeCtx, _ = context.WithTimeout(closeCtx, iterator.option.CloseTimeOut)
			}
			err := cursor.CloseWithContext(closeCtx)
			if err != nil {
				log.Println("cursor close err:", err)
			}
		}()


		for msg := cursor.Next(ctx); msg != nil; msg = cursor.Next(ctx) {
			err = iterator.queue.Push(ctx, *msg)
			if err != nil {
				log.Println("push message err:", err)
				break
			}
		}

		if cursor.Err() != nil {
			log.Println("notify get cursor err:", err)
		}

		return true
	}, syncrun.RandRestart(time.Second, 3 * time.Second))(ctx)
}

func (iterator *Iterator) CloseWithContext(ctx context.Context) error {
	iterator.pb.Stop()
	err := iterator.queue.CloseWithContext(ctx)
	if err != nil {
		return err
	}
	select {
	case <- iterator.pb.Stopped():
	case <- ctx.Done():
		return ctx.Err()
	}
	return nil
}

type Notification struct {
	CreateTime time.Time
	OffsetToken string
	Data interface{}
}

type OutputCursor interface {
	Next(ctx context.Context) *Notification
	CloseWithContext(ctx context.Context) error
	Err() error
}

type OutputStream interface {
	FetchOutputCursor(ctx context.Context) (OutputCursor, error)
	CommitOutput(ctx context.Context, notifications []Notification) error
}

type OutputStreamMiddle interface {
	WrapStream(stream OutputStream) OutputStream
}

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

func (queue *Queue) Push(ctx context.Context, notification Notification) error {
	for ctx.Err() == nil {
		queue.mx.Lock()
		closed := queue.closed
		queueFull := queue.queueFull
		if closed {
			queue.mx.Unlock()
			return errors.New("closed")
		}
		if queueFull == nil {
			queue.l.PushFront(notification)
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

func (queue *Queue) Peek(ctx context.Context) (*Notification, error) {
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

func (queue *Queue) tryPeek(ctx context.Context) (*Notification, error) {
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
		nf := ret.Value.(Notification)
		return &nf, nil
	}
}

func (queue *Queue) PullPeeked(ctx context.Context) error {
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

func (queue *Queue) LastPeeked() *Notification {
	nf := queue.peeked.Value.(Notification)
	return &nf
}

func (queue *Queue) isQueueFull() bool {
	if queue.l.Len()  >= queue.max {
		return true
	}
	return false
}