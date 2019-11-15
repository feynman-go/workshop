package mongo

import (
	"context"
	"github.com/feynman-go/workshop/cap/cluster"
	"github.com/feynman-go/workshop/database/mgo"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var _ cluster.NodeGroupStore = (*NodeStore)(nil)

type NodeStore struct {
	client *mgo.DbClient
	collection string
	clusterKey interface{}
}

func (store NodeStore) ReadNodeGroup(ctx context.Context) (*cluster.NodeGroup, error) {
	var res *mongo.SingleResult
	err := store.client.Do(ctx, func(ctx context.Context, database *mongo.Database) error {
		c := database.Collection(store.collection)
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

func (store NodeStore) SaveNodes(ctx context.Context, nodes []cluster.Node) error {
	var setNodes = bson.M{}
	for _, n := range nodes{
		// TODO node id has invalid charset ?
		setNodes["nodes." + n.ID] = n
	}

	err := store.client.Do(ctx, func(ctx context.Context, database *mongo.Database) error {
		if len(setNodes) == 0 {
			return nil
		}
		c := database.Collection(store.collection)
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

type clusterDoc struct {
	ClusterKey interface{} `bson:"_id"`
	Nodes map[string]cluster.Node `bson:"nodes"`
}
