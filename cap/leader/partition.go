package leader

import (
	"context"
	cluster2 "github.com/feynman-go/workshop/cap/cluster"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
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

func (pl *Leaders) GetPartitionMember(ctx context.Context, id PartitionID) *Member {
	p := pl.mb[id]
	if p == nil {
		return nil
	}
	return p.member
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

type LeadersReBalancer struct {
	c *cluster2.Follower
	leaders *Leaders
	pb *prob.Prob
	nodeID int64
}

func Rebalancer(cluster *cluster2.Follower, leaders *Leaders) *LeadersReBalancer {
	balancer :=  &LeadersReBalancer{
		leaders: leaders,
	}
	balancer.pb = prob.New(balancer.run)
	return balancer
}

func (bls *LeadersReBalancer) Start() {
	bls.pb.Start()
}

func (bls *LeadersReBalancer) Close() error {
	bls.pb.Stop()
	return nil
}

func (bls *LeadersReBalancer) run(ctx context.Context) {
	for ctx.Err() == nil {
		bls.c.RunTransfer(ctx, func(ctx context.Context, ring *cluster2.Ring, transaction *cluster2.Transaction) error {
			if transaction != nil {
				ring = ring.WithTransaction(*transaction)
			}
			for _, p := range bls.leaders.AllPartitions() {
				ringNode := ring.MapNode(int64(p))
				if ringNode == nil {
					continue
				}
				member := bls.leaders.GetPartitionMember(ctx, p)
				if member != nil {
					if ringNode.Node.ID == bls.nodeID {
						// 开始延时执行 elector
						if !member.GetInfo().IsLeader {
							member.StartElect(ctx)
						}
					} else {
						// 如果是leader 则延时KeepLive
						member.DoAsLeader(ctx, func(ctx context.Context) {
							member.DelayKeepLive(ctx)
						})
					}
				}
			}
			return nil
		})
	}
}
