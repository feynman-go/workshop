package leader

import (
	"context"
	"fmt"
	"github.com/feynman-go/workshop/cap/cluster2"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"
)

var _ Elector = (*rebalancerElector)(nil)

func NewRebalancerElectors(
	partitionID PartitionID,
	elector Elector,
	nodeAgent *cluster.NodeAgent,
	delayDuration time.Duration) (Elector, ElectionFactory) {
	balancerElector := &rebalancerElector{
		partitionID: partitionID,
		elector: elector,
		node: nodeAgent,
		delayDuration: delayDuration,
	}
	return balancerElector, balancerElector
}

type rebalancerElector struct {
	partitionID PartitionID
	elector Elector
	node *cluster.NodeAgent
	delayDuration time.Duration
	delayTimer *time.Timer
	rw sync.RWMutex
}

func (ba *rebalancerElector) NewElection(ctx context.Context, sequence int64) (Election, error) {
	node, err := ba.node.GetCurrent(ctx)
	if err != nil {
		return Election{}, fmt.Errorf("get current node: %v", err)
	}
	return Election{
		Sequence: sequence + 1,
		ElectID: node.ID,
	}, nil
}

func (ba *rebalancerElector) PostElection(ctx context.Context, election Election) error {
	// delay post election
	if ba.expectLeaderPartition(ctx) {
		return ba.elector.PostElection(ctx, election)
	}

	ba.rw.Lock()
	defer ba.rw.Unlock()

	if ba.delayTimer != nil {
		ba.delayTimer.Stop()
	}
	ba.delayTimer = time.AfterFunc(ba.delayDuration, func() {
		err := ba.elector.PostElection(ctx, election)
		if err != nil {
			log.Println("err:", err)
		}
	})
	return nil
}

func (ba *rebalancerElector) KeepLive(ctx context.Context, keepLive KeepLive) error {
	if ba.expectLeaderPartition(ctx) {
		return ba.elector.KeepLive(ctx, keepLive)
	}

	ba.rw.Lock()
	defer ba.rw.Unlock()

	if ba.delayTimer != nil {
		ba.delayTimer.Stop()
	}
	ba.delayTimer = time.AfterFunc(ba.delayDuration, func() {
		err := ba.elector.KeepLive(ctx, keepLive)
		if err != nil {
			log.Println("err:", err)
		}
	})

	ba.node.SendAck(ctx)
	return nil
}

func (ba *rebalancerElector) WaitElectionNotify(ctx context.Context) (Election, error) {
	e, err := ba.elector.WaitElectionNotify(ctx)
	if err != nil {
		return e, err
	}

	node, err := ba.node.GetCurrent(ctx)
	if err != nil {
		return e, fmt.Errorf("node get current: %v", err)
	}
	if node.ID == e.ElectID {
		err = ba.node.AddUnits(ctx, cluster.Unit{
			Key: strconv.FormatInt(int64(ba.partitionID), 16),
		})
		if err != nil {
			ba.node.SendAck(ctx)
		}
	}

	if err != nil {
		return e, fmt.Errorf("change node range: %v", err)
	}
	return e, nil
}

func (ba *rebalancerElector) expectLeaderPartition(ctx context.Context) bool {
	node, err := ba.node.GetCurrent(ctx)
	if err != nil {
		return true
	}

	_, ok := node.Units.Get(ba.getPartitionUnitID())
	return ok
}

func (ba *rebalancerElector) getPartitionUnitID() string {
	return strconv.FormatInt(int64(ba.partitionID), 16)
}

var _ cluster.SchedulerFactory = (*PartitionSchedulerFactory)(nil)

type PartitionSchedulerFactory struct {
	partitions          []PartitionID
	maxKeepLiveDuration time.Duration
}

func (factory *PartitionSchedulerFactory) StartSchedule(ctx context.Context, observer *cluster.NodeObserver) (cluster.Scheduler, error) {
	return &PartitionScheduler{
		partitions: factory.partitions,
		maxKeepLiveDuration: factory.maxKeepLiveDuration,
		observer: observer,
	}, nil
}

const (
	_scheduleTickDuration = time.Minute
)

type PartitionScheduler struct {
	partitions          []PartitionID
	maxKeepLiveDuration time.Duration
	observer            *cluster.NodeObserver
}

func (scheduler *PartitionScheduler) WaitSchedulerTrigger(ctx context.Context) ([]cluster.NodeScheduleEvent, error) {
	tk := time.NewTicker(_scheduleTickDuration)
	for ctx.Err() == nil {
		select {
		case <- ctx.Done():
		case <- tk.C:
			group, err := scheduler.observer.GetNodeGroup(ctx)
			if err != nil {
				return nil, fmt.Errorf("observer get node group: %v", err)
			}
			return scheduler.schedule(ctx, group)
		}
	}
	return nil, nil
}

func (scheduler *PartitionScheduler) isHealthNode(n cluster.Node) bool {
	return n.LastAvailable > 0 && time.Now().Sub(n.LastKeepLive) < scheduler.maxKeepLiveDuration
}

type nodeSchedule struct {
	cluster.Node
	ExpectUnits []cluster.Unit
	Event       *cluster.NodeScheduleEvent
	IsHealth    bool
}


func (scheduler *PartitionScheduler) schedule(ctx context.Context, group *cluster.NodeGroup) ([]cluster.NodeScheduleEvent, error) {
	//TODO need improve

	parts := scheduler.partitions
	nodes := group.GetNodes()
	if len(nodes) == 0 {
		return nil, nil
	}

	var (
		partNodeMap = map[PartitionID]string{} // value is node id
		healthNode int
		schedules = make([]*nodeSchedule, 0, len(nodes))
	)

	for _, n := range nodes {
		schedule := &nodeSchedule{
			Node:     n,
			IsHealth: scheduler.isHealthNode(n),
		}
		if schedule.IsHealth {
			healthNode ++
		}
		schedules = append(schedules, schedule)
	}

	average := len(parts) / healthNode
	if average == 0 {
		return nil, nil
	}

	mod := len(parts) % healthNode

	for _, s := range schedules {
		if !s.IsHealth {
			continue
		}
		i := 0
		for _, u := range s.Units.List() {
			if i > average {
				break
			}
			pid := parsePartitionID(u)
			if pid < 0 {
				continue
			}
			if i < average || (i == average && mod > 0) {
				partNodeMap[pid] = s.ID
				s.ExpectUnits = append(s.ExpectUnits, u)
				if i == average {
					mod --
				}
			}
			i ++
		}
	}

	sort.SliceStable(schedules, func(i, j int) bool {
		return len(schedules[i].ExpectUnits) < len(schedules[j].ExpectUnits)
	})

	for _, p := range parts {
		id := partNodeMap[p]
		if id != "" {
			continue
		}
		for _, s := range schedules {
			if len(s.ExpectUnits) < average + 1 {
				s.ExpectUnits = append(s.ExpectUnits, cluster.Unit{
					Key: buildPartitionID(p),
				})
			}
		}
	}

	var events []cluster.NodeScheduleEvent

	for _, s := range schedules {
		if len(s.ExpectUnits) == s.Units.Count() {
			var notEqual bool
			for _, u := range s.ExpectUnits {
				if _, ok := s.Units.Get(u.Key); !ok {
					notEqual = true
					break
				}
			}
			if !notEqual {
				continue
			}
		}

		events = append(events, cluster.NodeScheduleEvent{
			Schedule: &cluster.NodeSchedule{
				UpdateUnit: s.ExpectUnits,
			},
		})
	}

	return events, nil
}

func buildPartitionID(id PartitionID) string {
	return strconv.FormatInt(int64(id), 16)
}

func parsePartitionID(unit cluster.Unit) PartitionID {
	pid, err := strconv.ParseInt(unit.Key, 16, 64)
	if err != nil {
		return -1
	}
	return PartitionID(pid)
}
