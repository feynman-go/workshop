package leader

import (
	"context"
	"github.com/feynman-go/workshop/parallel"
	"sync"
)

type PartitionID int64

type PartitionLeaders struct {
	rw        sync.RWMutex
	mb        map[PartitionID]*Member
}

func NewPartitionLeaders(members map[PartitionID]*Member) *PartitionLeaders {
	return &PartitionLeaders{
		mb: map[PartitionID]*Member{},
	}
}


func (pl *PartitionLeaders) SyncLeader(ctx context.Context, partitionLeaded func(ctx context.Context, part PartitionID)) {
	var rs []func(ctx context.Context)
	pl.rw.RLock()
	for key := range pl.mb {
		k := key
		m := pl.mb[k]
		rs = append(rs, func(ctx context.Context) {
			for ctx.Err() == nil {
				m.SyncLeader(ctx, func(ctx context.Context) {
					partitionLeaded(k, ctx)
				})
			}
		})
	}
	pl.rw.RUnlock()
	parallel.RunByContext(ctx, rs...)
}