package mongo

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/feynman-go/workshop/notify"
	"github.com/feynman-go/workshop/syncrun/prob"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
)

type Cursor struct {
	rw sync.RWMutex
	err error
	cs *mongo.ChangeStream
	parser Parser
}

func(c *Cursor) Next(ctx context.Context) *notify.Message {
	if c.err != nil {
		return nil
	}

	if !c.cs.Next(ctx) {
		return nil
	}

	msg, err := c.parser(c.cs.Current)
	if err != nil {
		c.err = err
		return nil
	}

	return &msg
}

func(c *Cursor) Close(ctx context.Context) error {
	return c.cs.Close(ctx)
}

func(c *Cursor) Err() error {
	c.rw.RLock()
	defer c.rw.RUnlock()
	if c.err != nil {
		return c.err
	}
	return nil
}

func (c *Cursor) setErr(err error) {
	c.rw.Lock()
	defer c.rw.Unlock()
	if err != nil {
		c.err = err
	}
}

func (c *Cursor) ResumeToken() string {
	raw := c.cs.ResumeToken()
	return base64.RawStdEncoding.EncodeToString(raw)
}

type Parser func(raw bson.Raw) (notify.Message, error)

type MessageStream struct {
	query bson.D
	pb *prob.Prob
	database *mongo.Database
	col string
	parser Parser
}

func NewMessageSteam(database *mongo.Database, col string, query bson.D, parser Parser) *MessageStream {
	return &MessageStream{
		query: query,
		database: database,
		col: col,
		parser: parser,
	}
}

func (stream *MessageStream) start() {
	stream.pb.Start()
}

func (stream *MessageStream) Close() error {
	stream.pb.Stop()
	return nil
}

func (stream *MessageStream) ResumeFromToken(ctx context.Context, resumeToken string) (notify.Cursor, error){
	runCtx, _ := context.WithTimeout(ctx, 10 * time.Second)
	pipeline := mongo.Pipeline{bson.D{{"$match", stream.query}}}

	opt := options.ChangeStream().SetFullDocument(options.UpdateLookup).SetMaxAwaitTime(5 * time.Second)
	if resumeToken != "" {
		bs, err  := base64.RawStdEncoding.DecodeString(resumeToken)
		if err != nil {
			return nil, fmt.Errorf("bad cursor token err: %v", err)
		}
		opt.SetResumeAfter(bson.Raw(bs))
	}

	cs, err := stream.database.Collection(stream.col).Watch(runCtx, pipeline, opt)
	if err != nil {
		return nil, err
	}
	return &Cursor{
		cs: cs,
		parser: stream.parser,
	}, nil
}