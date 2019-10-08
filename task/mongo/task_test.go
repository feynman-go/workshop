package mongo

import (
	"context"
	"github.com/feynman-go/workshop/database/mgo"
	"github.com/feynman-go/workshop/task"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
	"time"
)

func TestTaskScheduler(t *testing.T) {

	agent, err := mgo.NewMajorAgent(mgo.MajorOption{
		Database: "task-test",
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

	testSuit := task.ScheduleTestSuit{
		New: func() task.Scheduler{
			scheduler := NewTaskScheduler(dbClt, "tasks", 0, func(taskKey string) int16 {
				return 0
			})
			time.Sleep(time.Second)
			return scheduler
		},
	}

	testSuit.TestSchedulerBasic(t)
	//testSuit.TestSchedulerOverWriteTime(t)
}

func TestTaskSchedulerOverWriteTime(t *testing.T) {
	agent, err := mgo.NewMajorAgent(mgo.MajorOption{
		Database: "task-test",
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

	testSuit := task.ScheduleTestSuit{
		New: func() task.Scheduler{
			scheduler := NewTaskScheduler(dbClt, "tasks", 0, func(taskKey string) int16 {
				return 0
			})
			time.Sleep(time.Second)
			return scheduler
		},
	}

	testSuit.TestSchedulerOverWriteTime(t)
}