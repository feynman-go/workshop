package leader

import (
	"context"
	"github.com/feynman-go/workshop/cap/cluster"
	"testing"
	"time"
)

func TestBalanceBasic(t *testing.T) {

	queue := cluster.NewMemoMessageQueue()
	scheduleQueue1 := queue.NewScheduleQueue("1")
	//scheduleQueue2 := queue.NewScheduleQueue("2")
	//scheduleQueue3 := queue.NewScheduleQueue("3")

	groupStore := &cluster.MemoGroupStore{}

	c := cluster.New(groupStore, queue, nil)
	defer c.Close()

	decider := NewMemoDecider()

	agent := cluster.NewNodeAgent(cluster.Node{
		ID: "1",
	}, scheduleQueue1)

	elector, factory := NewBalanceElectors(1, decider.CreateElector(), agent, time.Second)

	var parts = []*Partition{NewPartition(1, NewMember(elector, factory, Option{}), nil)}

	leaders := NewLeaders(parts...)
	leaders.SyncLeader(context.Background(), PartitionExecutor(func(ctx context.Context, part *Partition ) {

	}))

}