package mongo

import (
	"context"
	"fmt"
	"github.com/feynman-go/workshop/cap/cluster"
	"github.com/feynman-go/workshop/database/mgo"
	"github.com/feynman-go/workshop/syncrun/prob"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"time"
)

const (
	_NodeCollection      = "cluster_node"
	_NodeGroupCollection = "cluster_group"
)

const (
	_MaxMessageBucketSize = 10
)

var _ cluster.NodeGroupStore = (*NodeGroupStore)(nil)
var _ cluster.NodeMessageQueue = (*NodeMessageQueue)(nil)
var _ cluster.ScheduleEventQueue = (*ScheduleQueue)(nil)

type NodeGroupStore struct {
	client     *mgo.DbClient
	clusterKey interface{}
}

func NewNodeGroupStore(client *mgo.DbClient, clusterKey interface{}) *NodeGroupStore {
	return &NodeGroupStore{
		client:     client,
		clusterKey: clusterKey,
	}
}

func (store NodeGroupStore) ReadNodeGroup(ctx context.Context) (*cluster.NodeGroup, error) {
	var res *mongo.SingleResult
	err := store.client.Do(ctx, func(ctx context.Context, database *mongo.Database) error {
		c := database.Collection(_NodeGroupCollection)
		res = c.FindOne(ctx, bson.M{
			"cluster": store.clusterKey,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	if res == nil {
		return nil, errors.New("empty res")
	}
	if res.Err() != nil {
		return nil, res.Err()
	}

	var doc clusterDoc
	err = res.Decode(&doc)
	if err != nil {
		return nil, err
	}

	var ret []cluster.Node
	for _, n := range doc.Nodes {
		ret = append(ret, n)
	}

	return cluster.NewNodeGroup(ret), nil
}

func (store NodeGroupStore) SaveNodes(ctx context.Context, group *cluster.NodeGroup) error {
	var setNodes = bson.M{}
	for _, n := range group.GetNodes() {
		// TODO node id has invalid charset ?
		setNodes["nodes."+n.ID] = n
	}

	err := store.client.Do(ctx, func(ctx context.Context, database *mongo.Database) error {
		if len(setNodes) == 0 {
			return nil
		}
		c := database.Collection(_NodeGroupCollection)
		res := c.FindOneAndUpdate(ctx, bson.M{
			"cluster": store.clusterKey,
		}, bson.M{
			"$set": setNodes,
		})
		return res.Err()
	})
	if err != nil {
		return err
	}
	return nil
}

type NodeMessageQueue struct {
	messages chan cluster.NodeMessage
	pb       *routine.Prob
	client   *mgo.DbClient
}

func NewNodeEventQueue(client *mgo.DbClient) *NodeMessageQueue {
	queue := &NodeMessageQueue{
		messages: make(chan cluster.NodeMessage, 1),
		client: client,
	}
	queue.pb = routine.New(func(ctx context.Context) {
		for ctx.Err() == nil {
			err := queue.runWatcher(ctx)
			if err != nil {
				zap.L().Error("event queue runt watcher", zap.Error(err))
			}
		}
	})
	return queue
}

func (store *NodeMessageQueue) WaitMessages(ctx context.Context) ([]cluster.NodeMessage, error) {
	var messages []cluster.NodeMessage
	for {
		if len(messages) >= _MaxMessageBucketSize {
			return messages, nil
		}

		select {
		case <- ctx.Done():
			return messages, nil
		case msg := <- store.messages:
			messages = append(messages, msg)
			continue
		default:
			if len(messages) != 0 {
				return messages, nil
			}
			select {
			case <- ctx.Done():
			case msg := <- store.messages:
				messages = append(messages, msg)
			}
		}
	}
}

func (store *NodeMessageQueue) PushNodeTransaction(ctx context.Context, transaction cluster.Transaction) error {
	var (
		err          error
		writeModels = []mongo.WriteModel{}
	)

	if len(writeModels) == 0 {
		return nil
	}

	for _, e := range transaction.Schedules {
		writeModels = append(writeModels,
			mongo.NewUpdateOneModel().
			SetUpsert(false).
			SetFilter(bson.D{
				{
					"_id", e.NodeID,
				},
			}).SetUpdate(bson.D{
				{
					"schedule", cluster.NodeScheduleEvent{
							Seq: transaction.Seq,
							Schedule: e,
						},
				},
			}),
		)
	}

	err = store.client.Do(ctx, func(ctx context.Context, database *mongo.Database) error {
		c := database.Collection(_NodeCollection)
		_, err := c.BulkWrite(ctx, writeModels)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (store *NodeMessageQueue) Close() error {
	store.pb.Stop()
	return nil
}

func (store *NodeMessageQueue) runWatcher(ctx context.Context) error {
	return store.client.HandleColChangeStream(ctx, mgo.ChangeStreamQuery{
		Col: _NodeCollection,
		Project: bson.D{
			{
				"fullDocument._id", true,
			},
			{
				"fullDocument.records",
				bson.D{
					{
						"$slice", bson.A{"$fullDocument.records", 1},
					},
				},
			},
		},
	}, func(ctx context.Context, changeData mgo.ChangeData) error {
		var doc = &nodeDoc{}
		err := changeData.FullDocument.Unmarshal(doc)
		if err != nil {
			return fmt.Errorf("unmarshal doc: %v", err)
		}

		record := doc.Records[0]

		sendContext, _ := context.WithTimeout(ctx, time.Second)

		select { // TODO improve if block
		case <- sendContext.Done():
			zap.L().Warn("send context timestamp context done", zap.Error(sendContext.Err()))
		case store.messages <- cluster.NodeMessage{Timestamp: record.Timestamp, Node: record.Node}:
		}

		return nil
	})
}



type ScheduleQueue struct {
	nodeID string
	client *mgo.DbClient
	pb *routine.Prob
	events chan *cluster.NodeScheduleEvent
}

func NewScheduleQueue(nodeID string, client *mgo.DbClient) *ScheduleQueue {
	queue := &ScheduleQueue{
		client: client,
		nodeID: nodeID,
		events: make(chan *cluster.NodeScheduleEvent, 1),
	}
	queue.pb = routine.New(func(ctx context.Context) {
		for ctx.Err() == nil {
			err := queue.runWatcher(ctx)
			if err != nil {
				zap.L().Error("schedule watcher", zap.Error(err))
			}
		}
	})
	return queue
}

func (queue *ScheduleQueue) WaitEvent(ctx context.Context) (*cluster.NodeScheduleEvent, error) {
	queue.pb.Start()
	select {
	case <-ctx.Done():
		return nil, nil
	case evt := <-queue.events:
		return evt, nil
	}
}

func (queue *ScheduleQueue) Ack(ctx context.Context, node cluster.Node) error {
	var (
		updateResult *mongo.UpdateResult
		err          error
	)
	err = queue.client.Do(ctx, func(ctx context.Context, database *mongo.Database) error {
		c := database.Collection(_NodeCollection)
		updateResult, err = c.UpdateOne(ctx, bson.M{
			"_id": queue.nodeID,
		}, bson.M{
			"$push": bson.D{
				{
					"records", bson.D{
					{
						"$each",
						bson.A{
							nodeRecord{
								Node:      node,
								Timestamp: time.Now(),
							},
						},
					},
					{
						"$position", 0,
					},
					{
						"$slice", 5,
					},
				},
				},
			},
		}, options.Update().SetUpsert(true))

		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (queue *ScheduleQueue) Close() error {
	queue.pb.Stop()
	return nil
}

func (queue *ScheduleQueue) runWatcher(ctx context.Context) error {
	var lastSeq int64
	return queue.client.HandleColChangeStream(ctx, mgo.ChangeStreamQuery{
		Col: _NodeCollection,
		Match: bson.D{
			{
				"fullDocument._id", queue.nodeID,
			},
			{
				"fullDocument.schedule", bson.D{
					{
						"$exists", true,
					},
				},
			},
		},
		Project: bson.D{
			{
				"fullDocument._id", true,
			},
			{
				"fullDocument.schedule", true,

			},
		},
	}, func(ctx context.Context, changeData mgo.ChangeData) error {
		var doc = &nodeDoc{}
		err := changeData.FullDocument.Unmarshal(doc)
		if err != nil {
			return fmt.Errorf("unmarshal doc: %v", err)
		}

		if doc.Schedule.Seq <= lastSeq {
			return nil
		}

		lastSeq = doc.Schedule.Seq
		sendContext, _ := context.WithTimeout(ctx, time.Second)

		select { // TODO improve if block
		case <- sendContext.Done():
			zap.L().Warn("send context timestamp context done", zap.Error(sendContext.Err()))
		case queue.events <- doc.Schedule:
		}

		return nil
	})
}


type clusterDoc struct {
	ClusterKey interface{}             `bson:"_id"`
	Nodes      map[string]cluster.Node `bson:"nodes"`
}

type nodeDoc struct {
	NodeID  string       `bson:"_id"`
	Cluster interface{}  `bson:"cluster"`
	Records []nodeRecord `json:"records"`
	Schedule *cluster.NodeScheduleEvent `json:"schedule,omitempty"`
}

type nodeRecord struct {
	Node      cluster.Node `json:"node"`
	Timestamp time.Time    `json:"time"`
}
