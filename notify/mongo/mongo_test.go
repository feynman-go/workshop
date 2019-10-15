package mongo

import (
	"context"
	"github.com/feynman-go/workshop/database/mgo"
	"github.com/feynman-go/workshop/notify"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"testing"
	"time"
)

type MockResumStore struct {
	rs *Resume
}

func (store *MockResumStore) StoreResume(ctx context.Context, resume Resume) error {
	store.rs = &resume
	return nil
}

func (store *MockResumStore) GetResume(ctx context.Context) (resume Resume, err error) {
	if store.rs == nil {
		return Resume{}, nil
	}
	return *store.rs, nil
}


func TestBasicStream(t *testing.T) {
	agent, err := mgo.NewMajorAgent(mgo.MajorOption{
		Database: "message-test",
		ClientOptions: options.Client().SetHosts([]string{"localhost:27017"}).SetDirect(true),
	})

	if err != nil {
		t.Fatal(err)
	}

	dbClt := mgo.New(agent, mgo.Option{})

	defer dbClt.CloseWithContext(context.Background())

	err = dbClt.Do(context.Background(), func(ctx context.Context, db *mongo.Database) error {
		return db.Drop(ctx)
	})

	if err != nil {
		t.Fatal(err)
	}

	type Doc struct {
		Message string `bson:"message"`
	}

	ms := NewMessageStream(dbClt, "messages", &MockResumStore{}, Query{
		Fields: map[string]interface {}{
			"message": bson.M{"$exists": true},
		},
	}, func(ctx context.Context, d bson.Raw) (NotifyInfo, error) {
		var doc Doc
		err := bson.Unmarshal(d, &doc)
		if err != nil {
			return NotifyInfo{}, err
		}
		return NotifyInfo{
			CreateTime: time.Now(),
			Payload: doc,
		}, nil
	})

	cur, err := ms.FetchOutputCursor(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	err = dbClt.Do(context.Background(), func(ctx context.Context, db *mongo.Database) error {
		_, err := db.Collection("messages").InsertMany(ctx, []interface{}{
			Doc {
				Message: "a",
			}, Doc{
				Message: "b",
			}, Doc{
				Message: "c",
			}},
		)
		return err
	})

	if err != nil {
		t.Fatal(err)
	}

	var msg *notify.Notification

	if !checkInDuration(100 * time.Millisecond, 0, func() {
		msg = cur.Next(context.Background())
	}) {
		log.Println("next cost too much time")
	}

	if msg.Data.(Doc).Message != "a" {
		t.Fatal("first message uid should be a")
	}

	if !checkInDuration(100 * time.Millisecond, 0, func() {
		msg = cur.Next(context.Background())
	}) {
		log.Println("next cost too much time")
	}

	if msg.Data.(Doc).Message != "b" {
		t.Fatal("first message uid should be a")
	}

	if !checkInDuration(100 * time.Millisecond, 0, func() {
		msg = cur.Next(context.Background())
	}) {
		log.Println("next cost too much time")
	}

	if msg.Data.(Doc).Message != "c" {
		t.Fatal("first message uid should be a")
	}
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
