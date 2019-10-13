package notify

import (
	"context"
	"fmt"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
	"github.com/feynman-go/workshop/window"
	"github.com/pkg/errors"
	"log"
	"sync"
	"time"
)

/*type Publisher interface {
	Publish(ctx context.Context, notifications []Notification) (err error)
}*/

type Iterator struct {
	b []Notification
	notifier *notifier
}

// init iterator is empty
func NewIterator(stream OutputStream, option Option) *Iterator {
	nf := newNotifier(stream, option)
	return &Iterator{
		b: nil,
		notifier: nf,
	}
}

func (iterator *Iterator) Batch() []Notification {
	return iterator.b
}

func (iterator *Iterator) CommitAndWaitNext(ctx context.Context) (*Iterator, error) {
	err := iterator.notifier.commit(ctx, iterator.b)
	if err != nil {
		return nil, err
	}
	select {
	case ns := <- iterator.notifier.cn:
		return &Iterator{
			b:        ns,
			notifier: iterator.notifier,
		}, nil
	case <- ctx.Done():
		return nil, ctx.Err()
	}
}

func (iterator *Iterator) Close(ctx context.Context) error {
	return iterator.notifier.Close(ctx)
}

type notifier struct {
	pb        *prob.Prob
	stream    OutputStream
	option    Option
	cn chan []Notification
	wd 		*window.Window
}

type Option struct {
	MaxBlockCount    int64
	MaxBlockDuration time.Duration
	FailedWait       time.Duration
	StreamMiddles    []OutputStreamMiddle
	CloseTimeOut     time.Duration
}

func newNotifier(stream OutputStream, option Option) *notifier {
	for _, mid := range option.StreamMiddles {
		stream = mid.WrapStream(stream)
	}
	ret := &notifier{
		stream:    stream,
		cn: make(chan []Notification, 6),
	}
	ret.pb = prob.New(ret.run)
	var wrappers []window.Wrapper
	if option.MaxBlockCount <= 0 {
		option.MaxBlockCount = 1
	}
	if option.MaxBlockDuration <= 0 {
		option.MaxBlockDuration = time.Second
	}

	wrappers = append(wrappers, window.CounterWrapper(uint64(option.MaxBlockCount)))
	if option.MaxBlockDuration > 0 {
		wrappers = append(wrappers, window.DurationWrapper(option.MaxBlockDuration))
	}

	ret.wd = ret.newPublishWindow(wrappers)
	ret.pb.Start()
	return ret
}

func (notifier *notifier) start() {
	notifier.pb.Start()
}

func (notifier *notifier) commit(ctx context.Context, batch []Notification) error {
	return notifier.stream.CommitOutput(ctx, batch)
}

func (notifier *notifier) newPublishWindow(wrappers []window.Wrapper) *window.Window {
	ret := &publishWindow{
		batchChan: notifier.cn,
	}
	wd := window.New(ret, ret, wrappers...)
	return wd
}

func (notifier *notifier) run(ctx context.Context) {
	syncrun.FuncWithRandomStart(func(ctx context.Context) bool {
		ok, err := notifier.wd.WaitUntilOk(ctx)
		if err != nil || !ok {
			log.Println("wait window ok err:", err)
			return true
		}

		stream := notifier.stream
		cursor, err := stream.FetchOutputCursor(ctx)
		if err != nil {
			log.Println("fetch output cursor")
			return true
		}

		defer func() {
			closeCtx := context.Background()
			if notifier.option.CloseTimeOut != 0 {
				closeCtx, _ = context.WithTimeout(closeCtx, notifier.option.CloseTimeOut)
			}
			err := cursor.Close(closeCtx)
			if err != nil {
				log.Println("cursor close err:", err)
			}
		}()


		for msg := cursor.Next(ctx); msg != nil; msg = cursor.Next(ctx) {
			err = notifier.wd.Accept(ctx, *msg)
			if err != nil {
				log.Println("push message err:", err)
				break
			}
		}
		return true
	}, syncrun.RandRestart(time.Second, 3 * time.Second))(ctx)
}

func (notifier *notifier) Close(ctx context.Context) error {
	notifier.pb.Stop()
	err := notifier.wd.Close(ctx)
	if err != nil {
		return err
	}

	select {
	case <- notifier.pb.Stopped():
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
	Close(ctx context.Context) error
	Err() error
}

type OutputStream interface {
	FetchOutputCursor(ctx context.Context) (OutputCursor, error)
	CommitOutput(ctx context.Context, notifications []Notification) error
}

type publishWindow struct {
	rw        sync.RWMutex
	msgs      []Notification
	whiteboard window.Whiteboard
	batchChan chan []Notification
}

func (agg *publishWindow) Reset(whiteboard window.Whiteboard) {
	agg.whiteboard = whiteboard
}

// imply interface for window
func (agg *publishWindow) Accept(ctx context.Context, input interface{}) (err error) {
	agg.rw.Lock()
	defer agg.rw.Unlock()

	msg, ok := input.(Notification)
	if !ok {
		log.Println("input is not message")
		return errors.New("input must be message")
	}

	agg.msgs = append(agg.msgs, msg)
	return nil
}

// imply interface for window
func (agg *publishWindow) Materialize(ctx context.Context) error {
	agg.rw.Lock()
	defer agg.rw.Unlock()

	if len(agg.msgs) != 0 {
		select {
		case agg.batchChan <- agg.msgs:
			agg.msgs = nil
		default:
			return fmt.Errorf("publisher push message block")
		}
		return nil
	}

	return nil
}

type OutputStreamMiddle interface {
	WrapStream(stream OutputStream) OutputStream
}
