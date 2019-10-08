package leader

import (
	"context"
	"github.com/feynman-go/workshop/syncrun"
	"sync"
	"time"
)

type PartitionExecutor func(ctx context.Context, partition *Partition)

type PartitionID int64

type Leaders struct {
	rw        sync.RWMutex
	mb        map[PartitionID]*Partition
}

type Partition struct {
	id PartitionID
	member *Member
	vars map[string]string
}

func NewPartition(id PartitionID, member *Member, vars map[string]string) *Partition {
	vs := make(map[string]string, len(vars))
	for k, v := range vars {
		vs[k] = v
	}
	return &Partition{
		id, member, vs,
	}
}

func (part *Partition) ID() PartitionID {
	return part.id
}

func (part *Partition) GetValue(key string) string {
	if part.vars == nil {
		return ""
	}
	return part.vars[key]
}


func NewLeaders(partitions ...*Partition) *Leaders {
	leaders := &Leaders{
		mb: map[PartitionID]*Partition{},
	}

	for _, m := range partitions {
		leaders.mb[m.id] = m
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
		part := pl.mb[k]
		rs = append(rs, syncrun.FuncWithRandomStart(func(ctx context.Context) bool {
			for ctx.Err() == nil {
				part.member.SyncLeader(ctx, func(ctx context.Context) {
					executor(ctx, part)
				})
				return true
			}
			return false
		}, syncrun.RandRestart(time.Second, 3 * time.Second)))
	}
	pl.rw.RUnlock()
	syncrun.RunAsGroup(ctx, rs...)
}