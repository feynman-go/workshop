package cluster

import (
	"fmt"
	"github.com/MauriceGit/skiplist"
)

const (
	freeElementPosition = RingPosition(-1)
)

type Ring struct {
	seq     int64
	ring skiplist.SkipList
	nodes map[int64]RingNode
}

type Node struct {
	ID int64
	Available float64
}

func NewRing(nodes []Node) *Ring {
	r := &Ring {
		ring: skiplist.New(),
		nodes: map[int64]RingNode{},
	}
	for _, n := range nodes {
		r.nodes[n.ID] = RingNode{
			Node:         n,
			RingPosition: freeElementPosition,
		}
	}
	return r
}

func (r *Ring) Seq() int64 {
	return r.seq
}

func (r *Ring) MapNode(key int64) *RingNode {
	e, _ :=  r.ring.FindGreaterOrEqual(RingPosition(key))
	if e == nil {
		e = r.ring.GetSmallestNode()
	}
	if e != nil {
		ref := e.GetValue().(reference)
		node, ext := r.nodes[ref.NodeID]
		if !ext {
			return nil
		}
		return &node
	}
	return nil
}

func (r *Ring) WithTransaction(transaction Transaction) *Ring {
	ret := r.copy()
	ret.seq = transaction.Seq
	for _, freeNode := range transaction.Free {
		rn, ext := r.nodes[freeNode.ID]
		if !ext {
			rn.Node = freeNode
			rn.RingPosition = freeElementPosition
		}
		if !rn.IsFree() {
			r.ring.Delete(rn.RingPosition)
			rn.RingPosition = freeElementPosition
		}
		r.nodes[freeNode.ID] = rn
	}
	for _, available := range transaction.Available {
		rn, ext := r.nodes[available.Node.ID]
		if ext && !rn.IsFree() {
			r.ring.Delete(rn)
		}
		r.ring.Insert(available)
		r.nodes[available.Node.ID] = available
	}

	return ret
}

func (r *Ring) copy() *Ring {
	nodes := make(map[int64]RingNode, len(r.nodes))

	ring := skiplist.New()
	for e := r.ring.GetSmallestNode(); e != nil; e = r.ring.Next(e) {
		ring.Insert(e.GetValue())
	}

	for id, nd := range r.nodes {
		nodes[id] = nd
	}

	return &Ring {
		seq: r.seq,
		ring: ring,
		nodes: nodes,
	}
}

type Range struct {
	From *RingNode
	To *RingNode
}

type RingNode struct {
	Node Node
	RingPosition
}


func (e RingNode) IsFree() bool {
	return e.RingPosition < 0
}

func (e RingNode) Start() int64 {
	return int64(e.RingPosition)
}

type RingPosition int64

func (e RingPosition) ExtractKey() float64 {
	return float64(e)
}
func (e RingPosition) String() string {
	return fmt.Sprintf("%03d", e)
}

type reference struct {
	RingPosition
	NodeID int64
}

type NodeUpdate struct {
	Form *RingNode // if from is nil, means node is insert
	To *RingNode   // if to is nil, means node is removed
}

type Transaction struct {
	Seq int64
	Free []Node
	Available []RingNode
}

