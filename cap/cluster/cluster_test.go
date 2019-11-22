package cluster

import (
	"context"
	"log"
	"testing"
	"time"
)

type MockSchedulerFactory struct {
	StartSchedulerFunc func(ctx context.Context, observer *NodeObserver) (ScheduleTrigger, error)
}

func (factory MockSchedulerFactory) StartSchedule(ctx context.Context, observer *NodeObserver) (ScheduleTrigger, error) {
	return factory.StartSchedulerFunc(ctx, observer)
}

func TestClusterBasic(t *testing.T) {
	queue := NewMemoMessageQueue()
	scheduleQueue1 := queue.NewScheduleQueue("1")
	//scheduleQueue2 := queue.NewScheduleQueue("2")
	//scheduleQueue3 := queue.NewScheduleQueue("3")

	groupStore := &MemoGroupStore{}

	schedulerFactory := MockSchedulerFactory{
		func(ctx context.Context, observer *NodeObserver) (ScheduleTrigger, error) {
			return SchedulerFunc(func(ctx context.Context) ([]NodeSchedule, error) {
				log.Println("did get node schedule")
				queue := observer.GetEventQueue(ctx)
				_, err := queue.Peek(ctx)
				if err != nil {
					return nil, nil
				}
				return []NodeSchedule{
					{
						NodeID: "1",
						UpdateUnit: []Unit{
							{
								Key: "a",
							},
						},
					},
				}, nil
			}), nil
		},
	}

	c := New(groupStore, queue, schedulerFactory, nil)
	defer c.Close()


	agent := NewNodeAgent(Node{
		ID: "1",
	}, scheduleQueue1)

	err := agent.SetLiveInfo(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}

	err = agent.SendCurrentInfo(context.Background())
	if err != nil {
		log.Println("send ack", err)
	}

	time.Sleep(2 * time.Second)

	set, err := agent.WaitSchedule(context.Background())
	if err != nil {
		log.Println("WaitSchedule", err)
	}

	t.Log("unit key", set.List()[0].Key)



}