package cluster

import (
	"context"
	"github.com/feynman-go/workshop/record"
	"time"
)

type Manager struct {
	scheduler RingScheduler
	store     SnapshotStore
	transStore TransactionStore
	records   record.Factory
}

func New(scheduler RingScheduler, transStore TransactionStore, store SnapshotStore, records record.Factory) *Manager {
	return &Manager{
		scheduler:scheduler,
		store: store,
		transStore: transStore,
		records: records,
	}
}

func (cluster *Manager) AddNode(ctx context.Context, nodes []Node) error {
	return cluster.schedule(ctx, NodeAction{
		SetNodes: nodes,
	})
}

func (cluster *Manager) RemoveNode(ctx context.Context, ids []int64) error {
	return cluster.schedule(ctx, NodeAction{
		UnsetNodes: ids,
	})
}

func (cluster *Manager) UpdateNode(ctx context.Context, nodes []Node) error {
	return cluster.schedule(ctx, NodeAction {
		SetNodes: nodes,
	})
}

func (cluster *Manager) fetchLastRing(ctx context.Context) (*Ring, error) {
	r, err := cluster.store.GetLastSnapshot(ctx)
	if err != nil {
		return nil, err
	}

	if r == nil {
		r = NewRing(nil)
	}

	trans, err := cluster.transStore.GetTransactions(ctx, r.Seq())
	if err != nil {
		return nil, err
	}

	for _, tran := range trans {
		r = r.WithTransaction(tran)
	}

	return r, nil
}

func (cluster *Manager) schedule(ctx context.Context, action NodeAction) error {
	r, err := cluster.fetchLastRing(ctx)
	if err != nil {
		return err
	}

	trans, err := cluster.scheduler.ScheduleRing(r, action)
	if err != nil {
		return err
	}

	if trans != nil {
		err = cluster.transStore.AppendTransaction(ctx, *trans)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (cluster *Manager) run(ctx context.Context) {
	tk := time.NewTicker(time.Minute)
	for ctx.Err() == nil {
		select {
		case <- ctx.Done():
		case <- tk.C:
			cluster.refreshLastSnapshot(ctx)
		}
	}
}

func (cluster *Manager) refreshLastSnapshot(ctx context.Context) {
	var err error
	if cluster.records != nil {
		var recorder record.Recorder
		recorder, ctx = cluster.records.ActionRecorder(ctx, "refreshLastSnapshot")
		defer func() {
			recorder.Commit(err)
		}()
	}

	var r *Ring
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

type SnapshotStore interface {
	StoreSnapshot(ctx context.Context, snapshot *Ring) error
	SnapshotReader
}

type TransactionStore interface {
	AppendTransaction(ctx context.Context, transaction Transaction) error
	GetTransactions(ctx context.Context, afterSeq int64) ([]Transaction, error)
}

type RingScheduler interface {
	ScheduleRing(ring *Ring, action NodeAction) (*Transaction, error)
}

type NodeAction struct {
	UnsetNodes []int64
	SetNodes []Node
}

type TransactionNotifier interface {
	WaitTransaction(ctx context.Context, seq int64) (*Transaction, error)
}

type SnapshotReader interface {
	GetLastSnapshot(ctx context.Context) (*Ring, error)
}

type Follower struct {
	notifier TransactionNotifier
	reader SnapshotReader
	records record.Factory
}

func NewCluster(notifier TransactionNotifier, reader SnapshotReader) *Follower {
	cluster := &Follower{
		notifier: notifier,
		reader: reader,
	}
	return cluster
}

func (cluster *Follower) GetLastRing(ctx context.Context) (*Ring, error) {
	r, err := cluster.reader.GetLastSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return r, ctx.Err()
}

func (cluster *Follower) WaitTransaction(ctx context.Context, fromRing *Ring) (*Transaction, error) {
	return cluster.notifier.WaitTransaction(ctx, fromRing.Seq())
}


// help func
func (cluster *Follower) RunTransfer(ctx context.Context, transfer Transfer) {
	for ctx.Err() == nil {
		cluster.watcherTransaction(ctx, transfer)
	}
}

func (cluster *Follower) watcherTransaction(ctx context.Context, transfer Transfer) {
	var (
		err error
		r *Ring
	)

	if cluster.records != nil {
		var recorder record.Recorder
		recorder, ctx = cluster.records.ActionRecorder(ctx, "watcher transaction")
		defer func() {
			recorder.Commit(err)
		}()
	}

	r, err = cluster.GetLastRing(ctx)
	if err != nil {
		return
	}

	err = transfer(ctx, r, nil)
	if err != nil {
		return
	}

	for ctx.Err() == nil {
		var trans *Transaction
		trans, err = cluster.notifier.WaitTransaction(ctx, r.seq)
		if err != nil {
			return
		}
		if trans != nil {
			err = transfer(ctx, r, trans)
			if err != nil {
				return
			}
			r = r.WithTransaction(*trans)
		}
	}
}

type Transfer func(ctx context.Context, before *Ring, trans *Transaction) error