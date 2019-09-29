package notify

import (
	"context"
	"fmt"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
	"github.com/feynman-go/workshop/window"
	"github.com/pkg/errors"
	"log"
	"net/textproto"
	"sync"
	"time"
)

type Publisher interface {
	Publish(ctx context.Context, message []Message) (lastToken string)
}

type Notifier struct {
	pb         *prob.Prob
	stream     MessageStream
	whiteBoard WhiteBoard
	publisher  Publisher
	ag         *aggregator
	wrappers   []window.Wrapper
}

type Option struct {
	MaxCount int64
	MaxDuration time.Duration
	FailedWait time.Duration
}

func New(stream MessageStream, whiteBoard WhiteBoard, publisher Publisher, option Option) *Notifier {
	agg := &aggregator{
		wb: whiteBoard,
		publisher: publisher,
	}

	ret := &Notifier {
		stream: stream,
		whiteBoard: whiteBoard,
		ag: agg,
	}
	ret.pb = prob.New(ret.run)

	var wrappers []window.Wrapper
	if option.MaxCount <= 0 {
		option.MaxCount = 1
	}

	wrappers = append(wrappers, window.CounterWrapper(uint64(option.MaxCount)))
	if option.MaxDuration > 0 {
		wrappers = append(wrappers, window.DurationWrapper(option.MaxDuration))
	}

	ret.wrappers = wrappers
	ret.pb.Start()
	return ret
}


func (notifier *Notifier) run(ctx context.Context) {
	syncrun.FuncWithRandomStart(func(ctx context.Context) bool {
		token, err := notifier.whiteBoard.GetResumeToken(ctx)
		if err != nil {
			return true
		}

		cursor, err := notifier.stream.ResumeFromToken(ctx, token)
		if err != nil {
			return true
		}

		defer func() {
			closeCtx, _ := context.WithCancel(context.Background())
			err = cursor.Close(closeCtx)
			if err != nil {
				log.Println("cursor close err:", err)
			}
		}()

		wd := window.New(notifier.ag, notifier.wrappers...)

		defer func() {
			closeCtx, _ := context.WithTimeout(context.Background(), 10 * time.Second)
			wd.Close(closeCtx)
		}()
		for msg := cursor.Next(ctx); msg != nil; msg = cursor.Next(ctx) {
			err = wd.Accept(ctx, *msg)
			if err != nil {
				log.Println("push message err:", err)
				break
			}
		}

		return true
	}, syncrun.RandRestart(time.Second, 5 * time.Second))(ctx)
}

func (notifier *Notifier) Close() error {
	notifier.pb.Stop()
	return nil
}

func (notifier *Notifier) Closed() chan <- struct{} {
	return notifier.pb.Stopped()
}

type Message struct {
	ID string
	PayLoad interface{}
	Head textproto.MIMEHeader
	Token string
}

type Notify struct {
	CursorID string
}

type WhiteBoard interface {
	StoreResumeToken(ctx context.Context, token string) error
	GetResumeToken(ctx context.Context) (token string, err error)
}

type Cursor interface {
	Next(ctx context.Context) *Message
	Close(ctx context.Context) error
	Err() error
	ResumeToken() string
}

type MessageStream interface {
	ResumeFromToken(ctx context.Context, resumeToken string) (Cursor, error)
}

type aggregator struct {
	rw sync.RWMutex
	msgs []Message
	publisher Publisher
	wb WhiteBoard
}

func (agg *aggregator) Aggregate(ctx context.Context, input interface{}, item window.Whiteboard) (err error) {
	agg.rw.Lock()
	defer agg.rw.Unlock()

	msg, ok := input.(Message)
	if !ok {
		return errors.New("input must be message")
	}
	agg.msgs = append(agg.msgs, msg)
	return nil
}

func (agg *aggregator) OnTrigger(ctx context.Context) error {
	agg.rw.Lock()
	defer agg.rw.Unlock()

	if len(agg.msgs) != 0 {
		lastToken := agg.publisher.Publish(ctx, agg.msgs)
		if lastToken == "" {
			return errors.New("publisher no valid token")
		} else {
			err := agg.wb.StoreResumeToken(ctx, lastToken)
			if err == nil {
				if lastToken != agg.msgs[len(agg.msgs) - 1].Token {
					return errors.New("bad message notify")
				}
			} else {
				return fmt.Errorf("store resume token err: %v", err)
			}
		}
	}
	if agg.msgs != nil {
		agg.msgs = agg.msgs[:0]
	}
	return nil
}