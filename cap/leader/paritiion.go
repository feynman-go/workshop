package leader

import (
	"context"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
	"sync"
	"time"
)

type PartExecutor func(ctx context.Context, part PartitionID)

type PartitionID int64

type PartitionLeaders struct {
	rw        sync.RWMutex
	mb        map[PartitionID]*Member
	pb        *prob.Prob
	partExecutor PartExecutor
}

func NewPartitionLeaders(members map[PartitionID]*Member, partExecutor PartExecutor) *PartitionLeaders {
	leaders := &PartitionLeaders{
		mb: map[PartitionID]*Member{},
		partExecutor: partExecutor,
	}

	for p, m := range members {
		leaders.mb[p] = m
	}

	leaders.pb = prob.New(leaders.run)

	return leaders
}


func (pl *PartitionLeaders) Start() {
	pl.pb.Start()
}

func (pl *PartitionLeaders) Close() error {
	pl.pb.Stop()
	return nil
}

func (pl *PartitionLeaders) Closed() <- chan struct{} {
	return pl.pb.Stopped()
}


func (pl *PartitionLeaders) run(ctx context.Context) {
	var rs []func(ctx context.Context)
	pl.rw.RLock()
	for key := range pl.mb {
		k := key
		m := pl.mb[k]
		rs = append(rs, syncrun.FuncWithRandomStart(func(ctx context.Context) bool {
			for ctx.Err() == nil {
				m.SyncLeader(ctx, func(ctx context.Context) {
					pl.partExecutor(ctx, k)
				})
				return true
			}
			return false
		}, syncrun.RandRestart(time.Second, 3 * time.Second)))
	}
	pl.rw.RUnlock()

	syncrun.Run(ctx, rs...)
}