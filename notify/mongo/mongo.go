package mongo

import (
	"context"
	"github.com/feynman-go/workshop/notify"
	"github.com/feynman-go/workshop/syncrun/prob"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

func FetchMessage(raw bson.Raw) (notify.Message, error) {

}

type document struct {
	Seq int64 `bson:"_seq"`
}

func ReadMessage() {

}

func InsertMessage() {

}

type Cursor struct {

}

func(c *Cursor) Next(ctx context.Context) *notify.Message {

}

func(c *Cursor) Close(ctx context.Context) error {

}

func(c *Cursor) Err() error {

}

func(c *Cursor) ResumeToken() string {

}

type MessageStream struct {
	query bson.D
	trigger chan struct{}
	pb *prob.Prob
	database *mongo.Database
	col string
	streamCol string
}

func NewMessageSteam() {

}

func (stream *MessageStream) start() {
	stream.pb.Start()
}

func (stream *MessageStream) Close() error {
	stream.pb.Stop()
	return nil
}

func (stream *MessageStream) run(ctx context.Context) {
	for ctx.Err() == nil {
		runCtx, _ := context.WithTimeout(ctx, 10 * time.Second)
		pipeline := mongo.Pipeline{bson.D{{"$match", stream.query}}}

		cs, err := stream.database.Collection(stream.col).Watch(runCtx, pipeline,
			options.ChangeStream().SetFullDocument(options.UpdateLookup),
			options.ChangeStream().SetMaxAwaitTime(5 * time.Second),
			options.ChangeStream().SetResumeAfter(),
		)

		if err != nil {
			timer := time.NewTimer(2 * time.Second)
			select {
			case <- timer.C:
			case <- ctx.Done():
				return
			}
			continue
		}

		var doc = document{}
		for cs.Next(runCtx) {
			err = cs.Decode(&doc)
			if err != nil {
				cs.Close(ctx)
				streamErr = err
				return
			}
			lastTime := doc.FullDocument.LastTime
			if lastTime.Add(mgo.docTooLaterDuration).Before(time.Now()) {
				continue
			}

			mgo.cn <- doc
			mgo.setResumeToken(cs.ResumeToken())
		}


		err = cs.Err()
		if err == nil || err == mongo.ErrNilCursor || err == context.Canceled || err == context.DeadlineExceeded {
			cs.Close(runCtx)
			continue
		} else {
			cs.Close(runCtx)
			streamErr = err
			return
		}
	}
}

func (stream *MessageStream) ResumeFromToken(ctx context.Context, resumeToken string) (Cursor, error){

}

func (stream *MessageStream) Trigger() <- chan struct{} {

}

type Trigger interface {

}

