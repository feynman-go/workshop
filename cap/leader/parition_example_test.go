package leader

import (
	"context"
	"time"
)

func ExampleLeaderBasicUse() {
	var parts []*Partition
	for i := 0 ; i < 10; i ++ {
		member := newSimpleTestMember("node192.168.0.1")
		partition := NewPartition(PartitionID(i), member, nil)
		parts = append(parts, partition)
	}

	leaders := NewLeaders(parts...)

	// SyncLeader 是同步leader, 当member 成为某一个分区leader的时候，会调用func匿名函数，如果失去leader 资格，context 会被关闭
	leaders.SyncLeader(context.Background(), func(ctx context.Context, part *Partition) {
		// start notify kafka topic
		// start task manager
		// start push part message
	})
}

func newSimpleTestMember(nodeID string) *Member {
	// Elector 是一个选举裁决者的接口
	var electors = NewMemoDecider()
	var elector Elector = electors.CreateElector()

	var electionFactory ElectionFactory = &ConstElectionFactory{
		ConstID: nodeID, // 唯一表示当前节点的唯一id, 可以用ip container_id 随机数等等。
	}

	member := NewMember(elector, electionFactory, Option{
		MaxElectDuration: 2 * time.Second,
		MaxKeepLiveDuration: time.Second,
	})

	member.Start()

	// SyncLeader 是同步leader, 当member 成为leader的时候，会调用func匿名函数，如果失去leader 资格，context 会被关闭
	/*member.SyncLeader(context.Background(), func(ctx context.Context) {

	})*/
	return member
}
