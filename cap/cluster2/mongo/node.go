package mongo

import (
	"context"
	"github.com/feynman-go/workshop/cap/cluster2"
	"github.com/feynman-go/workshop/database/mgo"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)


const (
	_NodeCollection = "cluster_node"
	_NodeGroupCollection = "cluster_group"
)

var _ cluster.NodeGroupStore = (*NodeGroupStore)(nil)
var _ cluster.NodeStore = (*NodeStore)(nil)

type NodeGroupStore struct {
	client *mgo.DbClient
	clusterKey interface{}
}

func NewNodeGroupStore(client *mgo.DbClient, clusterKey interface{}) *NodeGroupStore {
	return &NodeGroupStore{
		client: client,
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
		setNodes["nodes." + n.ID] = n
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

type NodeStore struct {
	client *mgo.DbClient
	nodeID string
}

func NewNodeStore(client *mgo.DbClient, nodeID string) *NodeStore {
	return &NodeStore{
		client: client,
		nodeID: nodeID,
	}
}

func (store *NodeStore) UpdateNode(ctx context.Context, node cluster.Node) error {
	var (
		updateResult *mongo.UpdateResult
		err error
	)
	err = store.client.Do(ctx, func(ctx context.Context, database *mongo.Database) error {
		c := database.Collection(_NodeCollection)
		updateResult, err = c.UpdateOne(ctx, bson.M{
			"_id": store.nodeID,
		}, bson.M{
			"$set": node,
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

func (store *NodeStore) GetNode(ctx context.Context) (cluster.Node, error) {
	var res *mongo.SingleResult
	err := store.client.Do(ctx, func(ctx context.Context, database *mongo.Database) error {
		c := database.Collection(_NodeCollection)
		res = c.FindOne(ctx, bson.M{
			"_id": store.nodeID,
		})
		return nil
	})
	if err != nil {
		return cluster.Node{}, err
	}

	if res == nil {
		return cluster.Node{}, errors.New("empty res")
	}
	if res.Err() != nil {
		return cluster.Node{}, res.Err()
	}

	var doc cluster.Node
	err = res.Decode(&doc)
	if err != nil {
		return doc, err
	}

	return doc, nil
}

type clusterDoc struct {
	ClusterKey interface{} `bson:"_id"`
	Nodes map[string]cluster.Node `bson:"nodes"`
}
