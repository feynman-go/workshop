package mgo

import (
	"context"
	"fmt"
	"github.com/feynman-go/workshop/client"
	"github.com/feynman-go/workshop/mutex"
	"github.com/feynman-go/workshop/record"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
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

func (mgo *DbClient) Close(ctx context.Context) error {
	return mgo.clt.Close(ctx)
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

	dbOpt := options.Database().
		SetWriteConcern(writeconcern.New(writeconcern.WMajority(), writeconcern.J(true))).
		SetReadConcern(readconcern.Majority())

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

func (agent *majorAgent) Close(ctx context.Context) error {
	clt := agent.db.Client()
	err := clt.Disconnect(ctx)
	return err
}

func (agent *majorAgent) ClientOption() client.Option {
	return agent.cltOpt
}