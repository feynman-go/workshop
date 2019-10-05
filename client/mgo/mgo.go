package mgo

import (
	"context"
	"github.com/feynman-go/workshop/client"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)


type DbAgent interface {
	GetDB(ctx context.Context) (db *mongo.Database, err error)
	ClientOption() client.Option
	client.Agent
}

type DbClient struct {
	clt *client.Client
	dbAgent DbAgent
}

func New(agent DbAgent) *DbClient {
	return &DbClient{
		clt: client.New(agent, agent.ClientOption()),
		dbAgent: agent,
	}
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

type majorAgent struct {
	db *mongo.Database
	option MajorOption
	cltOpt client.Option
}

type MajorOption struct {
	options.ClientOptions
	Parallel int
	Queue int
	DataBase string
}

func NewMajorAgent(option MajorOption) (DbAgent, error) {
	clt, err := mongo.NewClient(&option.ClientOptions)
	if err != nil {
		return nil, err
	}

	dbOpt := options.Database().
		SetWriteConcern(writeconcern.New(writeconcern.WMajority(), writeconcern.J(true))).
		SetReadConcern(readconcern.Majority())

	return &majorAgent{
		db: clt.Database(option.DataBase, dbOpt),
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

func (agent *majorAgent) Reset(ctx context.Context) error {
	clt := agent.db.Client()
	err := clt.Disconnect(ctx)
	if err != nil {
		return err
	}
	return clt.Connect(ctx)
}

func (agent *majorAgent) Close(ctx context.Context) error {
	clt := agent.db.Client()
	err := clt.Disconnect(ctx)
	return err
}

func (agent *majorAgent) ClientOption() client.Option {
	return agent.cltOpt
}