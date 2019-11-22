package cluster

import (
	"context"
	"github.com/pkg/errors"
	"sync"
	"time"
)

var _ NodeGroupStore = (*MemoGroupStore)(nil)

var _ ScheduleEventQueue = (*MemoScheduleQueue)(nil)
var _ NodeMessageQueue = (*MemoMessageQueue)(nil)

type MemoGroupStore struct {
	rw sync.RWMutex
	group *NodeGroup
}

func (store *MemoGroupStore) ReadNodeGroup(ctx context.Context) (*NodeGroup, error) {
	store.rw.RLock()
	if store.group != nil {
		store.rw.RUnlock()
		return store.group, nil
	}
	store.rw.RUnlock()

	store.rw.Lock()
	defer store.rw.Unlock()

	if store.group == nil {
		store.group = NewNodeGroup([]Node{})
	}
	return store.group, nil
}

func (store *MemoGroupStore) SaveNodes(ctx context.Context, group *NodeGroup) error {
	store.rw.Lock()
	defer store.rw.Unlock()
	store.group = group
	return nil
}

type MemoScheduleQueue struct {
	nodeScheduleReader <- chan NodeScheduleEvent
	nodeMessageWriter chan <- NodeMessage
}

func (queue *MemoScheduleQueue) WaitEvent(ctx context.Context) (*NodeScheduleEvent, error) {
	select {
	case <- ctx.Done():
		return nil, nil
	case evt, ok := <- queue.nodeScheduleReader:
		if !ok {
			return nil, errors.New("queue closed")
		}
		return &evt, nil
	}
}

func (queue *MemoScheduleQueue) Ack(ctx context.Context, node Node) error {
	select {
	case queue.nodeMessageWriter <- NodeMessage{Timestamp: time.Now(), Node: node}:
		return nil
	case <- ctx.Done():
		return ctx.Err()
	}
}

func (queue *MemoScheduleQueue) Close() error {
	return nil
}


type MemoMessageQueue struct {
	nodeQueueReader chan NodeMessage
	nodeQueueWriters map[string]chan NodeScheduleEvent
}

func NewMemoMessageQueue() *MemoMessageQueue {
	return &MemoMessageQueue{
		nodeQueueReader: make(chan NodeMessage, 0),
		nodeQueueWriters: make(map[string]chan NodeScheduleEvent, 0),
	}
}

func (queue *MemoMessageQueue) WaitMessages(ctx context.Context) ([]NodeMessage, error) {
	select {
	case msg := <- queue.nodeQueueReader:
		return []NodeMessage{msg}, nil
	case <- ctx.Done():
		return nil, nil
	}
}

func (queue *MemoMessageQueue) PushNodeTransaction(ctx context.Context, transaction Transaction) error {
	for _, s := range transaction.Schedules {
		writer, ext := queue.nodeQueueWriters[s.NodeID]
		if ext {
			event := NodeScheduleEvent{
				Schedule: s,
				Seq: transaction.Seq,
			}
			select {
			case writer <- event:
				return ctx.Err()
			default:
				return nil
			}
		}
	}
	return nil
}

func (queue *MemoMessageQueue) Close() error {
	for _, q := range queue.nodeQueueWriters {
		close(q)
	}
	close(queue.nodeQueueReader)
	return nil
}

func (queue *MemoMessageQueue) NewScheduleQueue(nodeID string) *MemoScheduleQueue {
	c, ext := queue.nodeQueueWriters[nodeID]
	if  !ext {
		c = make(chan NodeScheduleEvent, 1)
	}

	return &MemoScheduleQueue{
		nodeScheduleReader: c,
		nodeMessageWriter: queue.nodeQueueReader,
	}
}
