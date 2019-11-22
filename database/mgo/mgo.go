package mgo

import (
	"context"
	"fmt"
	"github.com/feynman-go/workshop/client"
	"github.com/feynman-go/workshop/mutex"
	"github.com/feynman-go/workshop/record"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/time/rate"
	"runtime"
	"time"
)

const (
)

type DbAgent interface {
	GetDB(ctx context.Context) (db *mongo.Database, err error)
	client.Recoverable
	client.Agent
}

type DbClient struct {
	clt *client.Client
	dbAgent DbAgent
	mx *mutex.Mutex
	inited bool
}

type Option struct {
	Parallel *int
	Record *record.Factory
	Breaker *BreakerOption
	ClientMiddles []client.DoMiddle
}

type BreakerOption struct {
	AbnormalLimiter *rate.Limiter
	RecoveryWaitMax time.Duration
	RecoveryWaitMin time.Duration
}

func (opt Option) SetParallel(parallel int) Option {
	opt.Parallel = &parallel
	return opt
}

func (opt Option) SetRecorder(recorders record.Factory) Option {
	opt.Record = &recorders
	return opt
}

func (opt Option) SetBreaker(abnormalLimiter *rate.Limiter, recoveryWaitMax time.Duration, recoveryWaitMin time.Duration) Option {
	opt.Breaker = &BreakerOption{
		AbnormalLimiter: abnormalLimiter,
		RecoveryWaitMax: recoveryWaitMax,
		RecoveryWaitMin: recoveryWaitMin,
	}
	return opt
}

func (opt Option) AddClientMid(mid client.DoMiddle) Option {
	opt.ClientMiddles = append(opt.ClientMiddles, mid)
	return opt
}

func New(agent DbAgent, option Option) *DbClient {
	return &DbClient{
		clt: client.New(agent, buildClientOption(option, agent)),
		dbAgent: agent,
		mx: &mutex.Mutex{},
	}
}

func buildClientOption(opt Option, agent DbAgent) client.Option {
	cltOpt := client.Option{}
	mids := opt.ClientMiddles
	if opt.Parallel == nil {
		cltOpt = cltOpt.SetParallelCount(runtime.GOMAXPROCS(0))
	} else {
		cltOpt = cltOpt.SetParallelCount(*opt.Parallel)
	}

	if opt.Record != nil {
		mids = append(mids, client.NewRecorderMiddle(*opt.Record))
	}

	if opt.Breaker != nil {
		breakerMid := client.NewBasicBreakerMiddle(
			opt.Breaker.AbnormalLimiter,
			agent,
			opt.Breaker.RecoveryWaitMin,
			opt.Breaker.RecoveryWaitMax,
			)
		mids = append(mids, breakerMid)
	}

	cltOpt = cltOpt.AddMiddle(mids...)
	return cltOpt
}

type DoOption struct {
	PartID *int
}

func (option *DoOption) SetPartition(partID int) {
	option.PartID = &partID
}

func (mgo *DbClient) LongTimeDo(ctx context.Context, action func(ctx context.Context, db *mongo.Database) error) error {
	return mgo.clt.Do(ctx, func(ctx context.Context, agent client.Agent) error {
		ag := agent.(DbAgent)
		db, err := ag.GetDB(ctx)
		if err != nil {
			return err
		}
		return action(ctx, db)
	}, client.ActionOption{}.SetAlone(true))
}

func (mgo *DbClient) Do(ctx context.Context, action func(ctx context.Context, db *mongo.Database) error) error {
	return mgo.clt.Do(ctx, func(ctx context.Context, agent client.Agent) error {
		ag := agent.(DbAgent)
		if !mgo.mx.HoldForRead(ctx) {
			return ctx.Err()
		}
		inited := mgo.inited
		mgo.mx.ReleaseForRead()
		if !inited {
			if mgo.mx.Hold(ctx) {
				if !mgo.inited {
					err := mgo.dbAgent.TryRecovery(ctx)
					if err != nil {
						mgo.mx.Release()
						return fmt.Errorf("init db agent err: %v", err)
					}
				}
				mgo.inited = true
				mgo.mx.Release()
			}
		}
		db, err := ag.GetDB(ctx)
		if err != nil {
			return err
		}
		return action(ctx, db)
	}, )
}

func (mgo *DbClient) DoWithPartition(ctx context.Context, part int, action func(ctx context.Context, db *mongo.Database) error) error {
	actionOpt := client.ActionOption{}.SetPartition(part)
	return mgo.clt.Do(ctx, func(ctx context.Context, agent client.Agent) error {
		ag := agent.(DbAgent)
		db, err := ag.GetDB(ctx)
		if err != nil {
			return err
		}
		return action(ctx, db)
	}, actionOpt)
}

func (mgo *DbClient) CloseWithContext(ctx context.Context) error {
	return mgo.clt.CloseWithContext(ctx)
}


const (
	OperationTypeInsert OperationType = "insert"
	OperationTypeDelete OperationType =  "delete"
	OperationTypeReplace OperationType = "replace"
	OperationTypeUpdate OperationType = "update"
	OperationTypeDrop OperationType = "drop"
	OperationTypeRename OperationType = "rename"
	OperationTypeDropDatabase OperationType = "dropDatabase"
	OperationTypeInvalidate	 OperationType = "invalidate"
)

type OperationType string

type ChangeData struct {
	OperationType OperationType
	FullDocument bson.RawValue
	ResumeToken interface{}
}

// only for help, not safe
type ChangeStreamQuery struct {
	Col string
	Match bson.D // fields should start with 'fullDocument.'
	Project bson.D // fields should start with prefix 'fullDocument.'
	ResumeToken interface{} // for first stream start, privilege is greater than ResumeTimestamp.
	ResumeTimestamp primitive.Timestamp // for first stream start, is ResumeTimestamp T is 0, then t is current time
}

// only a help function, if operation return err is not nil, close cursor and return
func (mgo *DbClient) HandleColChangeStream(ctx context.Context, query ChangeStreamQuery, operation func(ctx context.Context, data ChangeData) error) error {
	if query.Col == "" {
		return errors.New("empty collection")
	}
	if query.Match == nil {
		query.Match = bson.D{}
	}

	pipeline := mongo.Pipeline{
		{
			{"$match", bson.M{
			"$or": bson.A{
				bson.M{
					"operationType": bson.M{"$in": bson.A{"insert", "replace", "delete", "update"}},
					"fullDocument": query.Match,
				},
				bson.M{
					"operationType": bson.M{"$in": bson.A{"invalidate"}},
				},
			},
		}},
	}}

	if query.Project != nil {
		pipeline = append(pipeline, bson.D{
			{
				"$project", query.Project,
			},
		})
	}


	type Document struct {
		ResumeToken   bson.RawValue `bson:"_id"`
		OperationType string        `bson:"operationType"`
		FullDocument  bson.RawValue `bson:"fullDocument,omitempty"`
		DocumentKey   struct {
			ID bson.RawValue `bson:"_id"`
		} `bson:"documentKey,omitempty"`
	}


	return mgo.LongTimeDo(ctx, func(ctx context.Context, db *mongo.Database) error {
		var resumeToken = query.ResumeToken

		for ctx.Err() == nil {

			opts := options.ChangeStream().SetFullDocument(options.UpdateLookup).
				SetMaxAwaitTime(10 * time.Second)

			if query.ResumeToken != nil {
				opts = opts.SetResumeAfter(resumeToken)
			} else {
				if query.ResumeTimestamp.T == 0 {
					query.ResumeTimestamp.T = uint32(time.Now().Unix())
				}
				opts = opts.SetStartAtOperationTime(&query.ResumeTimestamp)
			}

			cs, err := db.Collection(query.Col).Watch(ctx, pipeline, opts)
			if err != nil {
				return fmt.Errorf("watch change stream: %v", err)
			}

			var doc = Document{}
			for cs.Next(ctx) {
				err = cs.Decode(&doc)
				if err != nil {
					break
				}

				resumeToken = cs.ResumeToken()
				if doc.OperationType == "invalidate" {
					break
				}

				err = operation(ctx, ChangeData{
					OperationType: OperationType(doc.OperationType),
					FullDocument: doc.FullDocument,
					ResumeToken: doc.ResumeToken,
				})
			}
			if cs.Err() != nil {
				err = cs.Err()
			}

			closeCtx, _ := context.WithTimeout(context.Background(), 5 * time.Second)
			cs.Close(closeCtx)

			if err != nil {
				break
			}
		}
		return err
	})
}



type majorAgent struct {
	db *mongo.Database
	option MajorOption
	cltOpt client.Option
}

type MajorOption struct {
	*options.ClientOptions
	Parallel int
	Database string
}

func NewMajorAgent(option MajorOption) (DbAgent, error) {
	clt, err := mongo.NewClient(option.ClientOptions)
	if err != nil {
		return nil, err
	}

	dbOpt := options.Database()
	return &majorAgent{
		db: clt.Database(option.Database, dbOpt),
	}, nil
}

func (agent *majorAgent) setUpClientOption(opt MajorOption) {
	cltOpt := &client.Option{}
	if opt.Parallel != 0 {
		cltOpt.SetParallelCount(opt.Parallel)
	}
}

func (agent *majorAgent) GetDB(ctx context.Context) (db *mongo.Database, err error) {
	return agent.db, nil
}

func (agent *majorAgent) TryRecovery(ctx context.Context) error {
	clt := agent.db.Client()
	clt.Disconnect(ctx)
	err := clt.Connect(ctx)
	if err != nil {
		return err
	}
	err = clt.Ping(ctx, nil)
	if err != nil {
		return err
	}
	return nil
}

func (agent *majorAgent) CloseWithContext(ctx context.Context) error {
	clt := agent.db.Client()
	err := clt.Disconnect(ctx)
	return err
}

func (agent *majorAgent) ClientOption() client.Option {
	return agent.cltOpt
}