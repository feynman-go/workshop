package message

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

type Publisher interface {
	Publish(ctx context.Context, message []OutputMessage) (err error)
}

type Notifier struct {
	pb        *prob.Prob
	stream    OutputStream
	publisher Publisher
	wrappers  []window.Wrapper
	option    Option
}

type Option struct {
	MaxBlockCount    int64
	MaxBlockDuration time.Duration
	FailedWait       time.Duration
	StreamMiddles    []OutputStreamMiddle
	PublishMiddles   []PublisherMiddle
	CloseTimeOut     time.Duration
}

func New(stream OutputStream, publisher Publisher, option Option) *Notifier {
	for _, mid := range option.StreamMiddles {
		stream = mid.WrapStream(stream)
	}

	ret := &Notifier{
		stream:    stream,
		publisher: publisher,
	}
	ret.pb = prob.New(ret.run)

	var wrappers []window.Wrapper
	if option.MaxBlockCount <= 0 {
		option.MaxBlockCount = 1
	}

	wrappers = append(wrappers, window.CounterWrapper(uint64(option.MaxBlockCount)))
	if option.MaxBlockDuration > 0 {
		wrappers = append(wrappers, window.DurationWrapper(option.MaxBlockDuration))
	}

	ret.wrappers = wrappers
	ret.pb.Start()
	return ret
}

func (notifier *Notifier) Start() {
	notifier.pb.Start()
}

func (notifier *Notifier) newPublishWindow(cursor OutputCursor) *publishWindow {
	pub := notifier.publisher
	for _, mid := range notifier.option.PublishMiddles {
		pub = mid.WrapPublisher(pub)
	}

	ret := &publishWindow{
		publisher: pub,
		stream:    notifier.stream,
		cursor: 	cursor,
	}

	ret.wd = window.New(ret, notifier.wrappers...)
	return ret
}

func (notifier *Notifier) run(ctx context.Context) {
	syncrun.FuncWithRandomStart(func(ctx context.Context) bool {
		stream := notifier.stream
		cursor, err := stream.FetchOutputCursor(ctx)
		if err != nil {
			log.Println("fetch output cursor")
			return true
		}

		pw := notifier.newPublishWindow(cursor)

		defer func() {
			closeCtx := context.Background()
			if notifier.option.CloseTimeOut != 0 {
				closeCtx, _ = context.WithTimeout(closeCtx, notifier.option.CloseTimeOut)
			}
			err := pw.close(closeCtx)
			if err != nil {
				log.Println("cursor close err:", err)
			}
		}()

		for msg := cursor.Next(ctx); msg != nil; msg = cursor.Next(ctx) {
			err := pw.addToPush(ctx, *msg)
			if err != nil {
				log.Println("push message err:", err)
				break
			}
		}

		return true
	}, syncrun.RandRestart(time.Second, 5*time.Second))(ctx)
}

func (notifier *Notifier) Close() error {
	notifier.pb.Stop()
	return nil
}

func (notifier *Notifier) Closed() chan<- struct{} {
	return notifier.pb.Stopped()
}

type Message struct {
	UID       string
	Partition int64
	PayLoad   []byte
	Head      map[string]string
}

type OutputMessage struct {
	OffsetToken string
	Topic       string
	Message
}

type Notify struct {
	CursorID string
}

type OutputCursor interface {
	Next(ctx context.Context) *OutputMessage
	Close(ctx context.Context) error
	Err() error
}

type OutputStream interface {
	FetchOutputCursor(ctx context.Context) (OutputCursor, error)
	CommitOutput(ctx context.Context, messages []OutputMessage) error
}

type publishWindow struct {
	rw        sync.RWMutex
	msgs      []OutputMessage
	publisher Publisher
	stream    OutputStream
	cursor    OutputCursor
	wd        *window.Window
}

func (agg *publishWindow) addToPush(ctx context.Context, message OutputMessage) error {
	return agg.wd.Accept(ctx, message)
}

func (agg *publishWindow) close(ctx context.Context) error {
	select {
	case <-agg.wd.Closed():
		return errors.New("closed")
	default:
		return agg.wd.Close(ctx)
	}
}

// imply interface for window
func (agg *publishWindow) Aggregate(ctx context.Context, input interface{}, item window.Whiteboard) (err error) {
	agg.rw.Lock()
	defer agg.rw.Unlock()

	msg, ok := input.(OutputMessage)
	if !ok {
		log.Println("input is not message")
		return errors.New("input must be message")
	}

	agg.msgs = append(agg.msgs, msg)
	return nil
}

// imply interface for window
func (agg *publishWindow) OnTrigger(ctx context.Context) error {
	agg.rw.Lock()
	defer agg.rw.Unlock()

	if len(agg.msgs) != 0 {
		err := agg.publisher.Publish(ctx, agg.msgs)
		if err != nil {
			log.Println("publisher push message err:", err)
			return err
		}
		err = agg.stream.CommitOutput(ctx, agg.msgs)
		if err != nil {
			return fmt.Errorf("store resume token err: %v", err)
		}
		if agg.msgs != nil {
			agg.msgs = agg.msgs[:0]
		}
		return nil
	}

	return nil
}

type OutputStreamMiddle interface {
	WrapStream(stream OutputStream) OutputStream
}

type PublisherMiddle interface {
	WrapPublisher(publisher Publisher) Publisher
}
