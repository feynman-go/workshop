package cluster

import (
	"context"
	"errors"
	"github.com/feynman-go/workshop/queue"
	"github.com/feynman-go/workshop/record"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
	"log"
	"net/textproto"
	"sync"
	"time"
)

const (
	_maxNodeEventQueueCount = 16
)

type NodeMessageQueue interface {
	WaitMessages(ctx context.Context) ([]NodeMessage, error)
	PushNodeTransaction(ctx context.Context, transaction Transaction) error
	Close() error
}

type NodeMessage struct {
	Timestamp time.Time
	Node Node
}

type Cluster struct {
	nodeNotifyQueue NodeMessageQueue
	nodeGroupStore  NodeGroupStore
	records         record.Factory
	schedulers      Scheduler
	seq             int64
	pb              *routine.Prob
	rw              sync.RWMutex
	observer        *NodeObserver
}

func New(
	nodeStore NodeGroupStore,
	nodeNotifyQueue NodeMessageQueue,
	schedulers Scheduler,
	records record.Factory) *Cluster {

	cluster := &Cluster{
		records:         records,
		nodeGroupStore:  nodeStore,
		nodeNotifyQueue: nodeNotifyQueue,
		schedulers: schedulers,
		observer: &NodeObserver {
			nodeStore: nodeStore,
			q: &NodeEventQueue{
				q: queue.NewQueue(_maxNodeEventQueueCount),
			},
		},
	}

	cluster.pb = routine.New(cluster.run)
	cluster.pb.Start()
	return cluster
}

func (cluster *Cluster) Close() {
	cluster.pb.Stop()
}

func (cluster *Cluster) waitNodeNotify(ctx context.Context) error {
	msgs, err := cluster.nodeNotifyQueue.WaitMessages(ctx)
	log.Println("did receive msgs", msgs, err)
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		return nil
	}

	group, err := cluster.nodeGroupStore.ReadNodeGroup(ctx)
	if err != nil {
		return err
	}

	events, err := cluster.updateGroupByNodeMessage(ctx, msgs, group)
	if err != nil {
		return err
	}

	err = cluster.nodeGroupStore.SaveNodes(ctx, group)
	if err != nil {
		return err
	}

	err = cluster.observer.pushEvent(ctx, events)
	if err != nil {
		return err
	}
	return nil
}

func (cluster *Cluster) updateGroupByNodeMessage(ctx context.Context, msgs []NodeMessage, group *NodeGroup) ([]NodeEvent, error) {
	var events []NodeEvent
	for _, msg := range msgs {
		origin, ext := group.GetNodeByID(msg.Node.ID)
		e := NodeEvent{
			Seq: 0,
			Node: msg.Node,
		}
		if ext {
			e.Origin = &origin
		}
		events = append(events, e)
		group.UpdateNode(msg.Node)
	}
	return events, nil
}


func (cluster *Cluster) run(ctx context.Context) {
	syncrun.RunAsGroup(ctx,
		syncrun.FuncWithReStart(func(ctx context.Context) bool {
			cluster.waitNodeNotify(ctx)
			return true
		}, syncrun.RandRestart(time.Second, 2 * time.Second)),

		syncrun.FuncWithReStart(func(ctx context.Context) bool {
			cluster.waitSchedule(ctx)
			return true
		}, syncrun.RandRestart(time.Second, 2 * time.Second)),
	)
}


func (cluster *Cluster) waitSchedule(ctx context.Context) error {
	var err error
	if cluster.records != nil {
		var recorder record.Recorder
		recorder, ctx = cluster.records.ActionRecorder(ctx, "waitSchedule")
		defer func() {
			recorder.Commit(err)
		}()
	}

	scheduler, err := cluster.schedulers.StartSchedule(ctx, cluster.observer)
	if err != nil {
		return err
	}


	for ctx.Err() == nil {
		var schedules []NodeSchedule
		schedules, err = scheduler.WaitSchedulerTrigger(ctx)
		if err != nil {
			return err
		}

		if len(schedules) != 0 {
			err = cluster.nodeNotifyQueue.PushNodeTransaction(ctx, Transaction{
				Seq:       cluster.incSeq(),
				Schedules: schedules,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (cluster *Cluster) incSeq() int64 {
	cluster.rw.Lock()
	defer cluster.rw.Unlock()

	cluster.seq ++
	return cluster.seq
}

type NodeGroupStore interface {
	ReadNodeGroup(ctx context.Context) (*NodeGroup, error)
	SaveNodes(ctx context.Context, group *NodeGroup) error
}

type NodeGroup struct {
	nodes map[string]Node
}

func NewNodeGroup(nodes []Node) *NodeGroup {
	m := map[string]Node{}
	for _, n := range nodes {
		if n.ID == "" {
			continue
		}
		m[n.ID] = n
	}
	return &NodeGroup{
		nodes: m,
	}
}

func (group *NodeGroup) Size() int {
	return len(group.nodes)
}

func (group *NodeGroup) GetNodeByID(id string) (Node, bool) {
	n, ext :=  group.nodes[id]
	return n, ext
}

func (group *NodeGroup) UpdateNode(node Node) {
	if node.ID == "" {
		return
	}
	group.nodes[node.ID] = node
}

func (group *NodeGroup) GetNodes() []Node {
	ret := make([]Node, 0, len(group.nodes))
	for _, n := range group.nodes {
		ret = append(ret, n)
	}
	return ret
}


type Node struct {
	ID            string    `bson:"id"`
	Seq           int64     `bson:"seq"`
	LastKeepLive  time.Time `bson:"keepLive"`
	LastAvailable float64   `bson:"available"`
	Units     	  UnitSet   `bson:"units"`
	Labels		  Labels `bson:"labels"`
}

type Unit struct {
	Key string
	Labels Labels
	Status int32 // for customer change
}

type Labels textproto.MIMEHeader

type UnitSet struct {
	units map[string]Unit
}

func NewUnitSet() *UnitSet {
	return &UnitSet{
		units: map[string]Unit{},
	}
}

func (set *UnitSet) Update(unit Unit) {
	set.units[unit.Key] = unit
}

func (set *UnitSet) Remove(key string) {
	delete(set.units, key)
}

func (set *UnitSet) Count() int {
	return len(set.units)
}

func (set *UnitSet) Get(key string) (Unit, bool) {
	u, ext :=  set.units[key]
	return u, ext
}

func (set *UnitSet) Copy() *UnitSet {
	newSet := NewUnitSet()
	for _, u := range set.units {
		newSet.Update(u)
	}
	return newSet
}

func (set *UnitSet) List() []Unit {
	var ret = make([]Unit, 0, len(set.units))
	for _, u := range set.units {
		ret = append(ret, u)
	}
	return ret
}

type NotificationAcceptor interface {
	Accept(ctx context.Context, notification NodeMessage) error
	WaitScheduleTask(ctx context.Context) ([]NodeSchedule, error)
}

type NodeSchedule struct {
	NodeID string
	UpdateUnit []Unit
	RemoveUnit []string
}

type NodeScheduleEvent struct {
	Schedule NodeSchedule
	Seq int64
}

type Scheduler interface {
	StartSchedule(ctx context.Context, observer *NodeObserver) (ScheduleTrigger, error)
}

type ScheduleTrigger interface {
	WaitSchedulerTrigger(ctx context.Context) ([]NodeSchedule, error)
}

type SchedulerFunc func(ctx context.Context) ([]NodeSchedule, error)

func (f SchedulerFunc) WaitSchedulerTrigger(ctx context.Context) ([]NodeSchedule, error) {return f(ctx)}

type NodeObserver struct {
	nodeStore NodeGroupStore
	q *NodeEventQueue
}

func (observer *NodeObserver) GetNodeGroup(ctx context.Context) (*NodeGroup, error) {
	return observer.nodeStore.ReadNodeGroup(ctx)
}

func (observer *NodeObserver) GetEventQueue(ctx context.Context) *NodeEventQueue {
	return observer.q
}

func (observer *NodeObserver) pushEvent(ctx context.Context, events []NodeEvent) error {
	for _, e := range events {
		err := observer.q.push(ctx, e)
		if err != nil {
			return err
		}
	}
	return nil
}

type NodeEventQueue struct {
	q *queue.Queue
}

func (q *NodeEventQueue) push(ctx context.Context, e NodeEvent) error {
	return q.q.Push(ctx, e)
}

func (q *NodeEventQueue) Reset() {
	q.q.ResetPeeked()
}

func (q *NodeEventQueue) Peek(ctx context.Context) (*NodeEvent, error) {
	d, err := q.q.Peek(ctx)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, nil
	}
	node := d.(NodeEvent)
	return &node, nil
}



type Transaction struct {
	Seq       int64
	Schedules []NodeSchedule
}

type NodeEvent struct {
	Seq int64
	Node Node
	Origin *Node
}


type ScheduleEventQueue interface {
	WaitEvent(ctx context.Context) (*NodeScheduleEvent, error)
	Ack(ctx context.Context, node Node) error
	Close() error
}

type NodeAgent struct {
	rw          sync.RWMutex
	node        Node
	expectSet   *UnitSet
	eventStream ScheduleEventQueue
	cn chan *UnitSet
}

func NewNodeAgent(store Node, eventStream ScheduleEventQueue) *NodeAgent {
	manager := &NodeAgent{
		node:        store,
		eventStream: eventStream,
		cn: make(chan *UnitSet, 1),
	}
	return manager
}

func (manager *NodeAgent) SetLiveInfo(ctx context.Context, available float64) error {
	manager.rw.Lock()
	defer manager.rw.Unlock()

	if available <= 0 {
		return errors.New("available can not small & equal than zero")
	}

	node := &manager.node

	if node.LastAvailable < 0 {
		return errors.New("left cluster")
	}
	node.LastAvailable = available
	node.LastKeepLive = time.Now()
	node.Seq ++

	return nil
}

func (manager *NodeAgent) AddUnits(ctx context.Context, units ...Unit) error {
	manager.rw.Lock()
	defer manager.rw.Unlock()

	if manager.node.LastAvailable < 0 {
		return errors.New("left cluster")
	}

	for _, u := range units {
		manager.node.Units.Update(u)
	}
	return nil
}

func (manager *NodeAgent) RemoveUnits(ctx context.Context, keys []string) error {
	manager.rw.Lock()
	defer manager.rw.Unlock()

	if manager.node.LastAvailable < 0 {
		return errors.New("left cluster")
	}

	for _, key := range keys {
		manager.node.Units.Remove(key)
	}
	return nil
}

func (manager *NodeAgent) SendCurrentInfo(ctx context.Context) error {
	nd, err := manager.GetCurrentNode(ctx)
	if err != nil {
		return err
	}

	if nd.LastAvailable < 0 {
		return errors.New("left cluster")
	}

	return manager.eventStream.Ack(ctx, nd)
}

func (manager *NodeAgent) GetCurrentNode(ctx context.Context) (Node, error) {
	manager.rw.RLock()
	defer manager.rw.RUnlock()
	return manager.node, nil
}

func (manager *NodeAgent) GetCurrentSchedule(ctx context.Context) (expect *UnitSet, err error) {
	manager.rw.RLock()
	defer manager.rw.RUnlock()
	return manager.expectSet.Copy(), nil
}

func (manager *NodeAgent) SetLeaveCluster(ctx context.Context) error {
	manager.rw.Lock()
	defer manager.rw.Unlock()
	return manager.SetLiveInfo(ctx, -1)
}

func (manager *NodeAgent) run(ctx context.Context) {
	for ctx.Err() == nil {
		evt, err := manager.eventStream.WaitEvent(ctx)
		if err != nil {
			log.Println("wait event err:", err)
			continue
		}
		manager.rw.Lock()
		if manager.expectSet == nil {
			manager.expectSet = NewUnitSet()
		}
		for _, key := range evt.Schedule.RemoveUnit {
			manager.expectSet.Remove(key)
		}
		for _, u := range evt.Schedule.UpdateUnit {
			manager.expectSet.Update(u)
		}
		cp := manager.expectSet.Copy()
		select {
		case manager.cn <- cp:
		default:
		}
		manager.rw.Unlock()
	}
}

func (manager *NodeAgent) WaitSchedule(ctx context.Context) (expect *UnitSet, err error) {
	select {
	case set :=  <- manager.cn:
		return set, nil
	case <- ctx.Done():
		return nil, nil
	}
}