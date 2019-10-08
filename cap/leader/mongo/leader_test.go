package mongo

import (
	"context"
	"github.com/feynman-go/workshop/cap/leader"
	"github.com/feynman-go/workshop/database/mgo"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
	"time"
)

func TestElector_PostAnWatch(t *testing.T) {

	agent, err := mgo.NewMajorAgent(mgo.MajorOption{
		Database: "elector-test",
		ClientOptions: options.Client().SetHosts([]string{"localhost:27017"}).SetDirect(true),
	})

	if err != nil {
		t.Fatal(err)
	}

	dbClt := mgo.New(agent, mgo.Option{})

	defer dbClt.Close(context.Background())

	err = dbClt.Do(context.Background(), func(ctx context.Context, db *mongo.Database) error {
		return db.Drop(ctx)
	})

	if err != nil {
		t.Fatal(err)
	}

	ec := NewElector(1, "elector", dbClt, 3 * time.Second)

	go func() {
		err = ec.PostElection(context.Background(), leader.Election{
			Sequence: 1,
			ElectID: "elect1",
		})

		if err != nil {
			t.Fatal(err)
		}
	}()

	time.Sleep(time.Second)
	var elect leader.Election
	if !checkInDuration(200 * time.Millisecond, 0, func() {
		var err error
		elect, err = ec.WaitElectionNotify(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}) {
		t.Fatal("cost too much time")
	}


	if elect.ElectID != "elect1" {
		t.Fatal("unexpect elect id")
	}
	if elect.Sequence != 1 {
		t.Fatal("unexpect elect id")
	}
}

func TestElector_KeelAnchor(t *testing.T) {

	agent, err := mgo.NewMajorAgent(mgo.MajorOption{
		Database: "elector-test",
		ClientOptions: options.Client().SetHosts([]string{"localhost:27017"}).SetDirect(true),
	})

	if err != nil {
		t.Fatal(err)
	}

	dbClt := mgo.New(agent, mgo.Option{})

	defer dbClt.Close(context.Background())

	err = dbClt.Do(context.Background(), func(ctx context.Context, db *mongo.Database) error {
		return db.Drop(ctx)
	})

	if err != nil {
		t.Fatal(err)
	}

	ec := NewElector(2, "elector", dbClt, 3 * time.Second)
	go func() {
		err = ec.PostElection(context.Background(), leader.Election{
			Sequence: 1,
			ElectID: "elect1",
		})

		if err != nil {
			t.Fatal(err)
		}
	}()

	time.Sleep(time.Second)
	var elect leader.Election
	checkInDuration(100 * time.Millisecond, 0, func() {
		elect, err = ec.WaitElectionNotify(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	})

	if elect.ElectID != "elect1" {
		t.Fatal("unexpect elect id")
	}
	if elect.Sequence != 1 {
		t.Fatal("unexpect elect id")
	}

	err = ec.KeepLive(context.Background(), leader.KeepLive{
		Sequence: 1,
		ElectID: "elect1",
	})

}

func checkInDuration(maxDuration, minDuration time.Duration, f func()) bool {
	var wait = make(chan struct{})
	start := time.Now()
	go func() {
		f()
		close(wait)
	}()

	select {
	case <- wait:
	case <- time.After(maxDuration):
		return false
	}
	delta := time.Now().Sub(start)
	return delta <= maxDuration && delta >= minDuration
}
