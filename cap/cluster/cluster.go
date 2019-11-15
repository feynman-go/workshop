package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/feynman-go/workshop/cap/ring"
	"github.com/feynman-go/workshop/record"
	"github.com/feynman-go/workshop/syncrun/prob"
	"net/textproto"
	"sync"
	"time"
)

type NodeNotifyQueue interface {
	WaitNotification(ctx context.Context) ([]NodeNotification, error)
	PushNodeEvent(ctx context.Context, event NodesEvent) error
	Close() error
}

type NodeNotification struct {
	Timestamp time.Time
	Node Node
}

type NodesEvent struct {
	Seq   int64
	Nodes []ring.NodeRangeUpdate
}

type Cluster struct {
	ringScheduler   RingScheduler
	store           RingStore
	nodeNotifyQueue NodeNotifyQueue
	nodeGroupStore  NodeGroupStore
	records         record.Factory
}

func New(
	scheduler RingScheduler,
	nodeStore NodeGroupStore,
	store RingStore,
	nodeNotifyQueue NodeNotifyQueue,
	records record.Factory) *Cluster {

	return &Cluster{
		ringScheduler:   scheduler,
		store:           store,
		records:         records,
		nodeGroupStore:  nodeStore,
		nodeNotifyQueue: nodeNotifyQueue,
	}
}

func (cluster *Cluster) runWithContext(ctx context.Context) {
	for ctx.Err() == nil {
		err := cluster.waitNodeNotify(ctx)
		if err != nil {
			//TODO check error
		}
	}
}

func (cluster *Cluster) waitNodeNotify(ctx context.Context) error {
	ntf, err := cluster.nodeNotifyQueue.WaitNotification(ctx)
	if err != nil {
		return err
	}
	if ntf == nil {
		return nil
	}

	group, err := cluster.nodeGroupStore.ReadNodeGroup(ctx)
	if err != nil {
		return err
	}

	var updateNodes []Node
	for _, n := range ntf {
		group.UpdateNode(n.Node)
		updateNodes = append(updateNodes, n.Node)
	}

	err = cluster.nodeGroupStore.SaveNodes(ctx, updateNodes)
	if err != nil {
		return err
	}

	err = cluster.schedule(ctx, group)
	if err != nil {
		return err
	}

	return nil
}

func (cluster *Cluster) fetchLastRing(ctx context.Context) (*ring.Ring, error) {
	r, err := cluster.store.GetRingLastSnapshot(ctx)
	if err != nil {
		return nil, err
	}

	if r == nil {
		r = ring.NewRing(nil)
	}

	return r, nil
}

func (cluster *Cluster) schedule(ctx context.Context, group *NodeGroup) error {
	r, err := cluster.fetchLastRing(ctx)
	if err != nil {
		return err
	}

	nodeUpdate, err := cluster.ringScheduler.ScheduleRing(ctx, r, group)
	if err != nil {
		return err
	}

	if nodeUpdate != nil {
		err = cluster.nodeNotifyQueue.PushNodeEvent(ctx, NodesEvent{
			Seq:   r.Seq() + 1,
			Nodes: nodeUpdate,
		})
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (cluster *Cluster) run(ctx context.Context) {
	tk := time.NewTicker(time.Minute)
	for ctx.Err() == nil {
		select {
		case <- ctx.Done():
		case <- tk.C:
			cluster.refreshLastSnapshot(ctx)
		}
	}
}

func (cluster *Cluster) refreshLastSnapshot(ctx context.Context) {
	var err error
	if cluster.records != nil {
		var recorder record.Recorder
		recorder, ctx = cluster.records.ActionRecorder(ctx, "refreshLastSnapshot")
		defer func() {
			recorder.Commit(err)
		}()
	}

	var r *ring.Ring
	r, err = cluster.fetchLastRing(ctx)
	if err != nil {
		return
	}
	err = cluster.store.StoreSnapshot(ctx, r)
	if err != nil {
		return
	}

	err = cluster.store.StoreSnapshot(ctx, r)
	return
}

type RingStore interface {
	StoreSnapshot(ctx context.Context, snapshot *ring.Ring) error
	RingReader
}


type NodeAction struct {
	UnsetNodes []int64
	SetNodes []Node
}

type RingEventNotifier interface {
	WaitRingEvent(ctx context.Context, seq int64) (*RingEvent, error)
	Close() error
}

type RingReader interface {
	GetRingLastSnapshot(ctx context.Context) (*ring.Ring, error)
}

type RingEvent struct {
	Seq int64
	Nodes []ring.RingNode
}

type Update struct {

}

type NodeGroupStore interface {
	ReadNodeGroup(ctx context.Context) (*NodeGroup, error)
	SaveNodes(ctx context.Context, nodes []Node) error
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

type RingScheduler interface {
	ScheduleRing(ctx context.Context, ring *ring.Ring, group *NodeGroup) ([]ring.NodeRangeUpdate, error)
}

type NodeManager struct {
	rw           sync.RWMutex
	store        NodeStore
	expectRange  *KeySet
	unExpectChan chan struct{}
}

type NodeSyncEvent struct {
	Seq   int64
	Range KeySet
}

type NodeStore interface {
	UpdateNode(ctx context.Context, node Node) error
	GetNode(ctx context.Context) (Node, error)
}

func NewNodeManager(store NodeStore) *NodeManager {
	manager := &NodeManager {
		store: store,
		unExpectChan: make(chan struct{}, 1),
	}
	return manager
}

func (manager *NodeManager) KeepLive(ctx context.Context, available float64) error {
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

func (manager *NodeManager) ChangeNodeRange(ctx context.Context, rg KeySet) error {
	nd, err := manager.store.GetNode(ctx)
	if err != nil {
		return fmt.Errorf("store get: %v", err)
	}
	nd.LastRange = rg
	err = manager.store.UpdateNode(ctx, nd)
	if err != nil {
		return fmt.Errorf("store update: %v", err)
	}
	return err
}

func (manager *NodeManager) AcceptSyncEvent(event NodeSyncEvent) {
	manager.rw.Lock()
	defer manager.rw.Unlock()

	manager.expectRange = &event.Range
	manager.startTrans()
	return
}

func (manager *NodeManager) WaitEventUnexpected(ctx context.Context) (expect KeySet, actual KeySet, err error) {
	var (
		node Node
		expectPtr *KeySet
	)

	for ctx.Err() == nil {
		manager.rw.RLock()
		expectPtr = manager.expectRange
		manager.rw.RUnlock()
		node, err = manager.store.GetNode(ctx)
		if err != nil {
			err = fmt.Errorf("get node:%v", err)
			return
		}
		if expectPtr != nil && node.LastRange != *expectPtr {
			expect = *expectPtr
			actual = node.LastRange
			return
		}

		select {
		case <- manager.unExpectChan:
		case <- ctx.Done():
		}
	}
	err = ctx.Err()
	return
}

func (manager *NodeManager) GetCurrent(ctx context.Context) (Node, error) {
	manager.rw.RLock()
	defer manager.rw.RUnlock()
	nd, err := manager.store.GetNode(ctx)
	return nd, err
}

func (manager *NodeManager) LeaveCluster(ctx context.Context) error {
	manager.rw.Lock()
	defer manager.rw.Unlock()
	return manager.KeepLive(ctx, -1)
}

func (manager *NodeManager) startTrans() {
	select {
	case manager.unExpectChan <- struct{}{}:
	default:
	}
}


type Router struct {
	notifier RingEventNotifier
	reader   RingReader
	records  record.Factory
	ring *ring.Ring
	rw sync.RWMutex
	pb *prob.Prob
}

func NewClusterRouter(notifier RingEventNotifier, reader RingReader) *Router {
	cluster := &Router{
		notifier: notifier,
		reader: reader,
	}
	cluster.pb = prob.New(cluster.run)
	cluster.pb.Start()
	return cluster
}

func (cluster *Router) GetLastRing(ctx context.Context) (*ring.Ring, error) {
	r, err := cluster.reader.GetRingLastSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return r, ctx.Err()
}

func (cluster *Router) Close() {
	cluster.pb.Stop()
}

func (cluster *Router) run(ctx context.Context) {
	for ctx.Err() == nil {
		cluster.watchEvent(ctx)
	}
	cluster.notifier.Close()
}

func (cluster *Router) watchEvent(ctx context.Context) {
	var (
		err error
	)

	if cluster.records != nil {
		var recorder record.Recorder
		recorder, ctx = cluster.records.ActionRecorder(ctx, "watcher transaction")
		defer func() {
			recorder.Commit(err)
		}()
	}

	r, err := cluster.GetLastRing(ctx)
	if err != nil {
		return
	}

	cluster.rw.Lock()
	cluster.ring = r
	cluster.rw.Unlock()

	for ctx.Err() == nil {
		var event *RingEvent
		event, err = cluster.notifier.WaitRingEvent(ctx, r.seq)
		if err != nil {
			return
		}
		if event != nil {
			r = r.WithEvent(*event)
		}
		cluster.rw.Lock()
		cluster.ring = r
		cluster.rw.Unlock()
	}
}

type KeyRange struct {
	Start int64
	Length int64
}

func (rg KeyRange) End() int64{
	return rg.Start + rg.Length
}

type KeySet []KeyRange

func (rr KeySet) InRange(key int64) bool {
	// TODO improve search
	for _, r := range rr{
		if r.Start <= key && (r.Start + r.Length) > key {
			return true
		}
	}
	return false
}

func (rr KeySet) WithKeyRange(insert KeyRange) KeySet {
	// TODO improve search
	var (
		newSet = make([]KeyRange, 0, len(rr) + 1)
		startIndex = -1
		endIndex = len(rr)
	)
	//copy(newSet, rr)

	var readyInsert = insert
	for i, r := range rr {
		if startIndex < 0 && r.Start <= insert.Start {
			startIndex = i
		}
		if insert.End() <= r.End() {
			endIndex = i
			break
		}
	}

	// 前部的
	for i := 0; i < startIndex; i++ {
		newSet = append(newSet, rr[i])
	}

	// 合并的
	if startIndex >= 0 && insert.Start > rr[startIndex].Start {
		readyInsert.Start = rr[startIndex].Start
	}
	if endIndex < len(rr) && insert.End() < rr[endIndex].End() {
		readyInsert.Length = rr[endIndex].End() - readyInsert.Start
	}
	newSet = append(newSet, readyInsert)

	// 后部的
	for i := endIndex + 1; i < len(rr); i++ {
		newSet = append(newSet, rr[i])
	}
	return newSet
}

func (rr KeySet) WithExcludeKeyRange(insert KeyRange) KeySet {

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
}

type Labels textproto.MIMEHeader

type UnitSet struct {
	units map[string]Unit
}

func (set *UnitSet) Update(key string, unit Unit) {

}

func (set *UnitSet) Remove(key string) {

}

func (set *UnitSet) Count() int {

}

func (set *UnitSet) Get(key string) Unit {

}

func (set *UnitSet) List() []Unit {

}

type RouterIndex interface {
	UpdateUnit(ctx context.Context, unit ...Unit) error
	RouteUnit(ctx context.Context, key string) (string, bool)
}

