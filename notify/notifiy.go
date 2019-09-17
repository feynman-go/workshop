package notify

import (
	"context"
	"github.com/feynman-go/workshop/syncrun/prob"
	"github.com/feynman-go/workshop/window"
	"log"
	"net/textproto"
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
	wd 		   *window.Window
}

func New(stream MessageStream, whiteBoard WhiteBoard, publisher Publisher) *Notifier {
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
	for ctx.Err() != nil {
		notifier.trySendMessage(ctx)
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
		err = notifier.push(ctx, *msg)
		if err != nil {
			return err
		}

		msgs, err := notifier.pull()
		if err != nil {
			return err
		}

		if len(msgs) != 0 {
			lastToken := notifier.publisher.Publish(ctx, msgs)
			if lastToken != "" {
				err = notifier.whiteBoard.StoreResumeToken(ctx, lastToken)
				if err != nil {
					return err
				}
			}

			if lastToken != msgs[len(msgs) - 1].Token {
				return nil
			}
		}
	}
	return nil
}

func (notifier *Notifier) push(ctx context.Context, message Message) error {

}

func (notifier *Notifier) pull() ([]Message, error){

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