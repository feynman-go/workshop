package mgo

import (
	"context"
	"fmt"
	"github.com/feynman-go/workshop/breaker"
	"github.com/feynman-go/workshop/client"
	"github.com/feynman-go/workshop/client/richclient"
	"github.com/feynman-go/workshop/health"
	"github.com/feynman-go/workshop/health/easyhealth"
	"github.com/feynman-go/workshop/mutex"
	"github.com/feynman-go/workshop/record"
	"github.com/feynman-go/workshop/closes"
	"github.com/feynman-go/workshop/syncrun/routine"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.uber.org/zap"
	"runtime"
	"sync"
	"time"
)

const (
)

type DbAgent interface {
	GetDB(ctx context.Context) (db *mongo.Database, err error)
	GetBreaker() *breaker.Breaker
	client.Agent
}

type DbClient struct {
	clt            *client.Client
	dbAgent        DbAgent
	mx             *mutex.Mutex
	reporterCloser closes.WithContextCloser
}

type Option struct {
	Parallel *int
	Record *record.Factory
	StatusReporter *health.StatusReporter
	ClientMiddles []client.DoMiddle
}


func (opt Option) SetParallel(parallel int) Option {
	opt.Parallel = &parallel
	return opt
}

// if not set, use context record
func (opt Option) SetRecorder(recorders record.Factory) Option {
	opt.Record = &recorders
	return opt
}

func (opt Option) SetStatusReporter(reporter *health.StatusReporter) Option {
	opt.StatusReporter = reporter
	return opt
}


func (opt Option) AddClientMid(mid client.DoMiddle) Option {
	opt.ClientMiddles = append(opt.ClientMiddles, mid)
	return opt
}

func New(agent DbAgent, option Option) *DbClient {
	var closer closes.WithContextCloser
	if bk := agent.GetBreaker(); bk != nil && option.StatusReporter != nil {
		closer = easyhealth.StartBreakerReport(bk, option.StatusReporter)
	}
	return &DbClient{
		clt: client.New(agent, buildClientOption(option, agent)),
		dbAgent: agent,
		mx: &mutex.Mutex{},
		reporterCloser: closer,
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
		mids = append(mids, richclient.NewRecorderMiddle(*opt.Record))
	} else {
		mids = append(mids, richclient.NewRecorderMiddle(nil))
	}

	bk := agent.GetBreaker()
	if bk != nil {
		mids = append(mids, richclient.NewBreakerMiddle(bk))
	}

	cltOpt = cltOpt.AddMiddle(mids...)
	return cltOpt
}

type DoOption struct {
	Name string
	PartID *int
}

func (option DoOption) SetPartition(partID int) DoOption {
	option.PartID = &partID
	return option
}

func (option DoOption) SetName(name string) DoOption {
	option.Name = name
	return option
}

func (option DoOption) buildClientOption() client.ActionOption {
	opt := client.ActionOption{}
	if name := option.Name; name != "" {
		opt = opt.SetName(name)
	}

	if pp := option.PartID; pp != nil {
		opt = opt.SetPartition(*pp)
	}
	return opt
}

func (mgo *DbClient) LongTimeDo(ctx context.Context, action func(ctx context.Context, db *mongo.Database) error, options ...DoOption) error {
	opts := mgo.buildClientOption(options)
	opts = append(opts, client.ActionOption{}.SetAlone(true))
	return mgo.clt.Do(ctx, func(ctx context.Context, agent client.Agent) error {
		ag := agent.(DbAgent)
		db, err := ag.GetDB(ctx)
		if err != nil {
			return err
		}
		return action(ctx, db)
	}, opts...)
}

func (mgo *DbClient) Do(ctx context.Context, action func(ctx context.Context, db *mongo.Database) error, options ...DoOption) error {

	return mgo.clt.Do(ctx, func(ctx context.Context, agent client.Agent) error {
		ag := agent.(DbAgent)
		db, err := ag.GetDB(ctx)
		if err != nil {
			return err
		}
		if db == nil {
			return errors.New("no db")
		}
		return action(ctx, db)
	}, mgo.buildClientOption(options)...)
}

func (mgo *DbClient) CloseWithContext(ctx context.Context) error {
	err := mgo.clt.CloseWithContext(ctx)
	if err == nil {
		err = mgo.reporterCloser.CloseWithContext(ctx)
	}
	return err
}

func (mgo *DbClient) buildClientOption(options []DoOption) []client.ActionOption {
	var opts = make([]client.ActionOption, 0, len(options))
	for _, o := range options {
		opts = append(opts, o.buildClientOption())
	}
	return opts
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

type ChangeCursor struct {
	query ChangeStreamQuery
	dbClient *DbClient
	rw sync.RWMutex
	cs *mongo.ChangeStream
	resumeToken bson.Raw
}

func (c *ChangeCursor) Next(ctx context.Context) (*ChangeData, error) {
	cs, err := c.readyChangeStream(ctx)
	if err != nil {
		return nil, err
	}

	var doc = changeDocument{}

	if !cs.Next(ctx) {
		err := cs.Err()
		if err == context.Canceled || err == context.DeadlineExceeded && ctx.Err() != nil {
			err = nil
		}
		return nil, err
	}

	err = cs.Decode(&doc)
	if err != nil {
		return nil, err
	}

	c.rw.Lock()
	c.resumeToken = cs.ResumeToken()
	if doc.OperationType == "invalidate" {
		c.cs = nil
		go func(cs *mongo.ChangeStream) {
			cs.Close(context.Background())
		}(cs)
		c.rw.Unlock()
		return nil, nil
	}
	c.rw.Unlock()

	data := ChangeData{
		OperationType: OperationType(doc.OperationType),
		FullDocument: doc.FullDocument,
		ResumeToken: doc.ResumeToken,
	}
	return &data, nil
}

func (c *ChangeCursor) readyChangeStream(ctx context.Context) (*mongo.ChangeStream, error){
	c.rw.RLock()
	var cs = c.cs
	c.rw.RUnlock()

	if cs != nil {
		return cs, nil
	}

	c.rw.Lock()
	defer c.rw.Unlock()
	if c.cs != nil {
		return cs, nil

	}
	var err error
	cs, err = c.createChangeStream(ctx)
	if err != nil {
		return nil, err
	}

	c.cs = cs
	return cs, nil
}

type changeDocument struct {
	ResumeToken   bson.RawValue `bson:"_id"`
	OperationType string        `bson:"operationType"`
	FullDocument  bson.RawValue `bson:"fullDocument,omitempty"`
	DocumentKey   struct {
		ID bson.RawValue `bson:"_id"`
	} `bson:"documentKey,omitempty"`
}



func (c *ChangeCursor) buildPipeLine() interface{} {
	query := c.query
	pipeline := mongo.Pipeline{
		{
			{"$match", bson.M{
				"$or": bson.A{
					bson.M{
						"$and": bson.A {
							bson.M{
								"operationType": bson.M{"$in": bson.A{"insert", "replace", "delete", "update"}},
							},
							query.Match,
						},
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

	return pipeline
}

func (c *ChangeCursor) createChangeStream(ctx context.Context) (*mongo.ChangeStream, error) {
	var (
		opts = options.ChangeStream().SetFullDocument(options.UpdateLookup).
			SetMaxAwaitTime(10 * time.Second)
		query = c.query
		resumeToken = query.ResumeToken
		pipeline = c.buildPipeLine()
	)

	if c.resumeToken != nil {
		resumeToken = c.resumeToken
	}

	if resumeToken != nil {
		opts = opts.SetResumeAfter(resumeToken)
	} else {
		if query.ResumeTimestamp.T == 0 {
			query.ResumeTimestamp.T = uint32(time.Now().Unix())
		}
		opts = opts.SetStartAtOperationTime(&query.ResumeTimestamp)
	}

	var changeStream *mongo.ChangeStream
	err := c.dbClient.Do(ctx, func(ctx context.Context, db *mongo.Database) error {
		var err error
		changeStream, err = db.Collection(query.Col).Watch(ctx, pipeline, opts)
		if err != nil {
			return fmt.Errorf("watch change stream: %v", err)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return changeStream, nil
}

func (c *ChangeCursor) CloseWithContext(ctx context.Context) error {
	c.rw.Lock()
	defer c.rw.Unlock()
	if c.cs != nil {
		err := c.cs.Close(ctx)
		if err != nil {
			return err
		}
		c.cs = nil
	}
	return nil
}

func (mgo *DbClient) ChangeStreamCursor(ctx context.Context, query ChangeStreamQuery) (*ChangeCursor, error) {
	if query.Col == "" {
		return nil, errors.New("empty collection")
	}
	if query.Match == nil {
		query.Match = bson.D{}
	}

	return &ChangeCursor{
		query:       query,
		dbClient:    mgo,
		rw:          sync.RWMutex{},
		cs:          nil,
		resumeToken: nil,
	}, nil
}


// only a help function, if operation return err is not nil, close cursor and return
func (mgo *DbClient) HandleColChangeStream(ctx context.Context, query ChangeStreamQuery, operation func(ctx context.Context, data ChangeData) error) error {
	cc, err := mgo.ChangeStreamCursor(ctx, query)
	if err != nil {
		return err
	}

	defer cc.CloseWithContext(context.Background())

	for ctx.Err() == nil {

		cd, err := cc.Next(ctx)
		if err != nil {
			return err
		}

		if cd == nil {
			continue
		}

		err = operation(ctx, *cd)
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}



type majorAgent struct {
	client *mongo.Client
	db     *mongo.Database
	dbOpt *options.DatabaseOptions
	option SingleOption
	cltOpt client.Option
	pb *routine.Routine
	bk *breaker.Breaker
	logger *zap.Logger
	rw sync.RWMutex
}

type SingleOption struct {
	*options.ClientOptions
	Parallel int
	Database string
}

func NewMajorAgent(option SingleOption, logger *zap.Logger) (DbAgent, error) {
	_, err := mongo.NewClient(option.ClientOptions) // try new client
	if err != nil {
		return nil, err
	}

	dbOpt := options.Database().
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority()))

	agent := &majorAgent{
		dbOpt: dbOpt,
		option: option,
		bk: breaker.New(false),
		logger: logger,
	}
	agent.pb = routine.New(agent.run)
	agent.pb.Start()
	return agent, nil
}


func (agent *majorAgent) GetDB(ctx context.Context) (db *mongo.Database, err error) {
	agent.rw.RLock()
	defer agent.rw.RUnlock()
	return agent.db, nil
}

func (agent *majorAgent) GetBreaker() *breaker.Breaker {
	return agent.bk
}


func (agent *majorAgent) CloseWithContext(ctx context.Context) error {
	clt := agent.db.Client()
	err := clt.Disconnect(ctx)
	return err
}

func (agent *majorAgent) ClientOption() client.Option {
	return agent.cltOpt
}

func (agent *majorAgent) run(ctx context.Context) {
	timer := time.NewTimer(0)
	for ctx.Err() == nil {
		select {
		case <- timer.C:
			if agent.client == nil {
				clt, err := mongo.NewClient(agent.option.ClientOptions)
				if err != nil {
					agent.logger.Error("new client", zap.Error(err))
					agent.bk.Off(err.Error())
					timer.Reset(3 * time.Second)
					continue
				}
				err = clt.Connect(ctx)
				if err != nil {
					agent.logger.Error("db connect", zap.Error(err))
					agent.bk.Off(err.Error())
					timer.Reset(3 * time.Second)
					continue
				}
				agent.rw.Lock()
				agent.client = clt
				dbOpt := agent.dbOpt
				if dbOpt == nil {
					dbOpt = options.Database()
				}
				agent.db = agent.client.Database(agent.option.Database, dbOpt)
				agent.rw.Unlock()
			}

			pingCtx , _ := context.WithTimeout(ctx, 3 * time.Second)
			err := agent.client.Ping(pingCtx, readpref.Primary())
			if err != nil {
				agent.logger.Error("ping", zap.Error(err))
				agent.bk.Off(err.Error())
				agent.rw.Lock()
				agent.client.Disconnect(ctx)
				agent.client = nil
				agent.db = nil
				agent.rw.Unlock()
				timer.Reset(3 * time.Second)
				continue
			}
			agent.bk.On("ping success")
			timer.Reset(30 * time.Second)
		case <- ctx.Done():
		}
	}

	if agent.client != nil {
		closeCtx , _ := context.WithTimeout(context.Background(), 3 * time.Second)
		agent.client.Disconnect(closeCtx)
	}
}