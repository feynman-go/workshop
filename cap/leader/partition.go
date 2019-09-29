package leader

import (
	"context"
	"github.com/feynman-go/workshop/syncrun"
	"sync"
	"time"
)

type PartitionExecutor func(ctx context.Context, partitionID PartitionID)

type PartitionID int64

type Leaders struct {
	rw        sync.RWMutex
	mb        map[PartitionID]*Member
}

func NewLeaders(members map[PartitionID]*Member) *Leaders {
	leaders := &Leaders{
		mb: map[PartitionID]*Member{},
	}

	for p, m := range members {
		leaders.mb[p] = m
	}

	return leaders
}

func (pl *Leaders) AllPartitions() []PartitionID {
	pl.rw.RLock()
	defer pl.rw.RUnlock()

	var ids []PartitionID
	for id := range pl.mb {
		ids = append(ids, id)
	}
	return ids
}

func (pl *Leaders) SyncLeader(ctx context.Context, executor PartitionExecutor) {
	var rs []func(ctx context.Context)
	pl.rw.RLock()
	for key := range pl.mb {
		k := key
		m := pl.mb[k]
		rs = append(rs, syncrun.FuncWithRandomStart(func(ctx context.Context) bool {
			for ctx.Err() == nil {
				m.SyncLeader(ctx, func(ctx context.Context) {
					executor(ctx, k)
				})
				return true
			}
			return false
		}, syncrun.RandRestart(time.Second, 3 * time.Second)))
	}
	pl.rw.RUnlock()
	syncrun.Run(ctx, rs...)
}