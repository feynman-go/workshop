package notify

import (
	"context"
	"github.com/feynman-go/workshop/parallel/prob"
	"net/textproto"
	"time"
)

type Publisher interface {
	Publish(ctx context.Context, message *Message) error
}

type Notifier struct {
	pb *prob.Prob
	steam MessageStream
	whiteBoard WhiteBoard
	publisher Publisher
}

func (notifier *Notifier) Start(ctx context.Context, restartMax time.Duration, restartMin time.Duration) error {
	notifier.pb.Start()
	go prob.SyncRunWithRestart(notifier.pb, notifier.run, prob.RandomRestart(restartMin, restartMax))
	return nil
}

func (notifier *Notifier) run(ctx context.Context) (canRestart bool) {
	token, err := notifier.whiteBoard.GetResumeToken(ctx)
	if err != nil {
		return true
	}

	cursor, err := notifier.steam.ResumeFromToken(ctx, token)
	if err != nil {
		return true
	}

	for msg := cursor.Next(ctx); msg != nil; msg = cursor.Next(ctx) {
		err = notifier.publisher.Publish(ctx, msg)
		if err != nil {
			return true
		}
		err = notifier.whiteBoard.StoreResumeToken(ctx, cursor.ResumeToken())
		if err != nil {
			return true
		}
	}
}

func (notifier *Notifier) Close() error {
	notifier.pb.Close()
	return nil
}

func (notifier *Notifier) Closed() chan <- struct{} {
	return notifier.pb.Closed()
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
