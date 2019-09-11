package mongo

import (
	"context"
	"github.com/feynman-go/workshop/cap/leader"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
	"time"
)

func TestElector_PostAnWatch(t *testing.T) {
	c, err := mongo.Connect(context.Background(),
		options.Client().SetReplicaSet("rs0").
		SetHosts([]string{"localhost:27017"}).
		SetConnectTimeout(time.Second).
		SetSocketTimeout(time.Second).
		SetServerSelectionTimeout(3 * time.Second).SetDirect(true),
	)
	if err != nil {
		t.Fatal(err)
	}

	err = c.Database("test").Collection("leader").Drop(context.Background())
	err = c.Database("test").Drop(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	ec := NewElector(1, "leader", c.Database("test"), 3 * time.Second)
	go func() {
		time.Sleep(time.Second)
		err = ec.PostElection(context.Background(), leader.Election{
			Sequence: 1,
			ElectID: "elect",
		})

		if err != nil {
			t.Fatal(err)
		}
	}()

	elect, err := ec.WaitElectionNotify(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if elect.ElectID != "elect" {
		t.Fatal("unexpect elect id")
	}
	if elect.Sequence != 1 {
		t.Fatal("unexpect elect id")
	}
}

func TestElector_KeelAnchor(t *testing.T) {
	c, err := mongo.Connect(context.Background(),
		options.Client().SetReplicaSet("rs0").
			SetHosts([]string{"localhost:27017"}).
			SetConnectTimeout(time.Second).
			SetSocketTimeout(time.Second).
			SetServerSelectionTimeout(3 * time.Second).SetDirect(true),
	)
	if err != nil {
		t.Fatal(err)
	}

	err = c.Database("test").Collection("leader").Drop(context.Background())

	err = c.Database("test").Drop(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	ec := NewElector(1, "leader", c.Database("test"), 3 * time.Second)
	go func() {
		time.Sleep(time.Second)
		err = ec.PostElection(context.Background(), leader.Election{
			Sequence: 1,
			ElectID: "elect",
		})

		if err != nil {
			t.Fatal(err)
		}
	}()

	elect, err := ec.WaitElectionNotify(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if elect.ElectID != "elect" {
		t.Fatal("unexpect elect id")
	}
	if elect.Sequence != 1 {
		t.Fatal("unexpect elect id")
	}

	err = ec.KeepLive(context.Background(), leader.KeepLive{
		Sequence: 1,
		ElectID: "elect",
	})

	if elect.ElectID != "elect" {
		t.Fatal("unexpect elect id")
	}
	if elect.Sequence != 1 {
		t.Fatal("unexpect elect id")
	}
}