package notify

import (
	"context"
	"github.com/feynman-go/workshop/syncrun/prob"
	"log"
	"net/textproto"
	"time"
)

type Publisher interface {
	Publish(ctx context.Context, message *Message) error
}

type Notifier struct {
	pb         *prob.Prob
	stream     MessageStream
	whiteBoard WhiteBoard
	publisher  Publisher
	trigger    Trigger
}

func New(stream MessageStream, whiteBoard WhiteBoard, publisher Publisher, trigger Trigger) *Notifier {
	ret := &Notifier {
		stream: stream,
		whiteBoard: whiteBoard,
		publisher: publisher,
		trigger: trigger,
	}
	ret.pb = prob.New(ret.run)
	return ret
}

func (notifier *Notifier) Start(ctx context.Context, restartMax time.Duration, restartMin time.Duration) error {
	notifier.pb.Start()
	return nil
}

func (notifier *Notifier) run(ctx context.Context) {
	for {
		select {
		case <- ctx.Done():
			return
		case <- notifier.trigger.Trigger():
			notifier.trySendMessage(ctx)
		}
	}
}

func (notifier *Notifier) trySendMessage(ctx context.Context) error {
	token, err := notifier.whiteBoard.GetResumeToken(ctx)
	if err != nil {
		return err
	}

	cursor, err := notifier.stream.ResumeFromToken(ctx, token)
	if err != nil {
		return err
	}

	defer func() {
		closeCtx, _ := context.WithCancel(context.Background())
		err = cursor.Close(closeCtx)
		if err != nil {
			log.Println("cursor close err:", err)
		}
	}()

	for msg := cursor.Next(ctx); msg != nil; msg = cursor.Next(ctx) {
		err = notifier.publisher.Publish(ctx, msg)
		if err != nil {
			return err
		}
		err = notifier.whiteBoard.StoreResumeToken(ctx, cursor.ResumeToken())
		if err != nil {
			return err
		}
	}
	return nil
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

type Trigger interface {
	Trigger() <- chan struct{}
}