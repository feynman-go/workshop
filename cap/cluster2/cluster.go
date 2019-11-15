package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/feynman-go/workshop/queue"
	"github.com/feynman-go/workshop/record"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
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
	schedulers      SchedulerFactory
	seq             int64
	pb              *prob.Prob
	rw              sync.RWMutex
	observer 		*NodeObserver
}

func New(
	nodeStore NodeGroupStore,
	nodeNotifyQueue NodeMessageQueue,
	records record.Factory) *Cluster {

	cluster := &Cluster{
		records:         records,
		nodeGroupStore:  nodeStore,
		nodeNotifyQueue: nodeNotifyQueue,
		observer: &NodeObserver {
			nodeStore: nodeStore,
			q: &NodeEventQueue{
				q: queue.NewQueue(_maxNodeEventQueueCount),
			},
		},
	}

	cluster.pb = prob.New(cluster.run)
	cluster.pb.Start()
	return cluster
}

func (cluster *Cluster) waitNodeNotify(ctx context.Context) error {
	msgs, err := cluster.nodeNotifyQueue.WaitMessages(ctx)
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

	var events []NodeScheduleEvent
	events, err = scheduler.WaitSchedulerTrigger(ctx)
	if err != nil {
		return err
	}

	if len(events) != 0 {
		err = cluster.nodeNotifyQueue.PushNodeTransaction(ctx, Transaction{
			Seq:   cluster.incSeq(),
			Events: events,
		})
		if err != nil {
			return err
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
}

type NodeScheduleEvent struct {
	Schedule *NodeSchedule
}

type SchedulerFactory interface {
	StartSchedule(ctx context.Context, observer *NodeObserver) (Scheduler, error)
}

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


type Scheduler interface {
	WaitSchedulerTrigger(ctx context.Context) ([]NodeScheduleEvent, error)
}

type Transaction struct {
	Seq int64
	Events []NodeScheduleEvent
}

type NodeEvent struct {
	Seq int64
	Node Node
	Origin *Node
}


type ScheduleEventQueue interface {
	WaitEvent(ctx context.Context) (*NodeScheduleEvent, error)
	Ack(ctx context.Context, node Node) error
}

type NodeAgent struct {
	rw           sync.RWMutex
	store        NodeStore
	expectSet  	 *UnitSet
	eventStream ScheduleEventQueue
}

type NodeStore interface {
	UpdateNode(ctx context.Context, node Node) error
	GetNode(ctx context.Context) (Node, error)
}

func NewNodeAgent(store NodeStore, eventStream ScheduleEventQueue) *NodeAgent {
	manager := &NodeAgent{
		store: store,
		eventStream: eventStream,
	}
	return manager
}

func (manager *NodeAgent) KeepLive(ctx context.Context, available float64) error {
	manager.rw.Lock()
	defer manager.rw.Unlock()

	if available <= 0 {
		return errors.New("available can not small & equal than zero")
	}

	node, err := manager.store.GetNode(ctx)
	if err != nil {
		return nil
	}

	if node.LastAvailable < 0 {
		return errors.New("left cluster")
	}
	node.LastAvailable = available
	node.LastKeepLive = time.Now()
	node.Seq ++

	return manager.store.UpdateNode(ctx, node)
}

func (manager *NodeAgent) AddUnits(ctx context.Context, units ...Unit) error {
	nd, err := manager.store.GetNode(ctx)
	if err != nil {
		return fmt.Errorf("store get: %v", err)
	}
	for _, u := range units {
		nd.Units.Update(u)
	}

	err = manager.store.UpdateNode(ctx, nd)
	if err != nil {
		return fmt.Errorf("store update: %v", err)
	}
	return err
}

func (manager *NodeAgent) RemoveUnits(ctx context.Context, keys []string) error {
	nd, err := manager.store.GetNode(ctx)
	if err != nil {
		return fmt.Errorf("store get: %v", err)
	}
	for _, key := range keys {
		nd.Units.Remove(key)
	}

	err = manager.store.UpdateNode(ctx, nd)
	if err != nil {
		return fmt.Errorf("store update: %v", err)
	}
	return err
}

func (manager *NodeAgent) SendAck(ctx context.Context) error {
	nd, err := manager.GetCurrent(ctx)
	if err != nil {
		return err
	}
	return manager.eventStream.Ack(ctx, nd)
}

func (manager *NodeAgent) GetCurrent(ctx context.Context) (Node, error) {
	manager.rw.RLock()
	defer manager.rw.RUnlock()
	nd, err := manager.store.GetNode(ctx)
	return nd, err
}

func (manager *NodeAgent) LeaveCluster(ctx context.Context) error {
	manager.rw.Lock()
	defer manager.rw.Unlock()
	return manager.KeepLive(ctx, -1)
}
