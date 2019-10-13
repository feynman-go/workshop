package mongo

import (
	"context"
	"fmt"
	"github.com/feynman-go/workshop/database/mgo"
	"github.com/feynman-go/workshop/notify"
	"github.com/feynman-go/workshop/syncrun/prob"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Resume struct {
	Ts primitive.Timestamp
}

func (rm Resume) Encode() string {
	return strconv.FormatInt(int64(rm.Ts.T), 16) + ":" + strconv.FormatInt(int64(rm.Ts.I), 16)
}

func (rm *Resume) Decode(s string) error {
	ls := strings.Split(s, ":")
	if len(ls) != 2 {
		return fmt.Errorf("bad resume string %v", s)
	}

	t, err := strconv.ParseInt(ls[0], 16, 64)
	if err != nil {
		return err
	}

	i, err := strconv.ParseInt(ls[0], 16, 64)
	if err != nil {
		return err
	}

	rm.Ts.T = uint32(t)
	rm.Ts.I = uint32(i)
	return nil
}


type ResumeStore interface {
	StoreResume(ctx context.Context, resume Resume) error
	GetResume(ctx context.Context) (resume Resume, err error)
}

type Parser func(ctx context.Context, raw bson.Raw) ([]*notify.Notification, error)

type MessageStream struct {
	query bson.D
	pb *prob.Prob
	dbClient *mgo.DbClient
	col string
	parser Parser
	resumeStore ResumeStore
}

/** query target is mongo change event data
	example:
		{
			"fullDocument.type": "type-A",
		}
 */
func NewMessageStream(dbClient *mgo.DbClient, col string, resumeStore ResumeStore, query Query, parser Parser) *MessageStream {

	var q bson.D
	for f, v := range query.Fields {
		q = append(q, bson.E{
			"fullDocument." + f, v,
		})
	}

	return &MessageStream{
		query: q,
		dbClient: dbClient,
		col: col,
		parser: parser,
		resumeStore: resumeStore,
	}
}

func (stream *MessageStream) start() {
	stream.pb.Start()
}

func (stream *MessageStream) Close() error {
	stream.pb.Stop()
	return nil
}

func (stream *MessageStream) FetchOutputCursor(ctx context.Context) (notify.OutputCursor, error) {
	rm, err := stream.resumeStore.GetResume(ctx)
	if err != nil {
		return nil, err
	}
	return stream.cursorByTimestamp(ctx, rm.Ts)
}

func (stream *MessageStream) CommitOutput(ctx context.Context, messages []notify.Notification) error {
	var max *Resume
	for _, m := range messages {
		var rm = &Resume{}
		err := rm.Decode(m.OffsetToken)
		if err != nil {
			return err
		}
		if max == nil || max.Ts.T < rm.Ts.T || (max.Ts.T == rm.Ts.T && max.Ts.I < rm.Ts.I) {
			max = rm
		}
	}
	if max != nil {
		return stream.resumeStore.StoreResume(ctx, *max)
	}
	return nil
}

func (stream *MessageStream) cursorByTimestamp(ctx context.Context, ts primitive.Timestamp) (notify.OutputCursor, error) {
	return &Cursor{
		resetStream: stream.getChangeStream,
		parser: stream.parser,
	}, nil

}

func (stream *MessageStream) prepareBasicWatchOption() *options.ChangeStreamOptions {
	opt := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	return opt
}

func (stream *MessageStream) getChangeStream(ctx context.Context, ts primitive.Timestamp) (*mongo.ChangeStream, error) {
	if ts.T == 0 {
		ts.T = uint32(time.Now().Unix())
	} else {
		ts.I ++
	}

	opt := stream.prepareBasicWatchOption()
	opt = opt.SetStartAtOperationTime(&ts)

	pipeline := mongo.Pipeline{bson.D{{"$match", bson.M{
		"$or": bson.A{
			bson.M{
				"$and": bson.A{
					bson.M{"operationType": bson.M{"$in": bson.A{"insert", "replace", "delete", "update"}}},
					stream.query,
				},
			},
			bson.M{
				"operationType": bson.M{"$in": bson.A{"invalidate"}},
			},
		},
	}}}}

	var cs *mongo.ChangeStream

	err := stream.dbClient.Do(ctx, func(ctx context.Context, db *mongo.Database) error {
		var err error
		cs, err = db.Collection(stream.col).Watch(ctx, pipeline, opt)
		return err
	})
	if err != nil {
		return nil, err
	}
	return cs, nil
}


type Cursor struct {
	rw sync.RWMutex
	err error
	cs *mongo.ChangeStream
	resetStream func(ctx context.Context, timestamp primitive.Timestamp) (*mongo.ChangeStream, error)
	parser Parser
	msgs []*notify.Notification

	curTimestamp primitive.Timestamp
	curToken string
}

func(c *Cursor) Next(ctx context.Context) *notify.Notification {
	for c.Err() == nil && ctx.Err() == nil {
		var err error
		if c.cs == nil {
			c.cs, err = c.resetStream(ctx, c.curTimestamp)
			if err != nil {
				c.setErr(fmt.Errorf("reset stream err: %v", err))
				continue
			}
		}

		if len(c.msgs) > 0 {
			ret := c.msgs[0]
			c.msgs = c.msgs[1:]
			ret.OffsetToken = c.curToken
			return ret
		}

		if !c.cs.Next(ctx) {
			if ctx.Err() == nil {
				c.setErr(c.cs.Err())
			}
			continue
		}

		var doc changeDoc
		err = c.cs.Decode(&doc)
		if err != nil {
			c.setErr(err)
			continue
		}

		c.curTimestamp = doc.ClusterTime
		c.curToken = Resume{c.curTimestamp}.Encode()

		if doc.OperationType == "invalidate" {
			c.cs = nil
			continue
		}

		msgs, err := c.parser(ctx, doc.FullDoc)
		if err != nil {
			c.setErr(err)
			continue
		}

		c.msgs = msgs
	}
	return nil
}

func(c *Cursor) CloseWithContext(ctx context.Context) error {
	if c.cs != nil {
		return c.cs.Close(ctx)
	}
	return nil
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

type changeDoc struct {
	ID bson.Raw `bson:"_id"`
	OperationType string `bson:"operationType"`
	FullDoc bson.Raw `bson:"fullDocument"`
	Ns struct {
		DB string `bson:"ns"`
		Coll string `bson:"coll"`
	} `bson:"ns"`
	To struct {
		DB string `bson:"ns"`
		Coll string `bson:"coll"`
	} `bson:"to"`
	ClusterTime primitive.Timestamp `bson:"clusterTime"`
}

type Query struct {
	Fields map[string]interface{}
}
