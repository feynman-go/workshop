package ring

import (
	"fmt"
	"github.com/MauriceGit/skiplist"
	"github.com/feynman-go/workshop/cap/cluster"
	"time"
)

type Transference struct {
	Seq int64
	StartTime time.Time
	Update NodeRangeUpdate
}

type NodeRangeUpdate struct {
	NodeID      string
	ExpectRange cluster.KeySet
}

type Ring struct {
	expectMax int64
	seq   int64
	ring  skiplist.SkipList
	nodes map[string][]RingNode // id is ring node
}

func NewRing(nodes []RingNode) *Ring {
	r := &Ring{
		ring:  skiplist.New(),
	}

	for _, n := range nodes {
		r.ring.Insert(reference{
			n.RingPosition, n.NodeID,
		})
		r.nodes[n.NodeID] = n
	}

	return r
}

func (r *Ring) RangeSize() int64 {
	return r.expectMax
}

func (r *Ring) Seq() int64 {
	return r.seq
}

func (r *Ring) MapNode(key int64) *RingNode {
	e, _ := r.ring.FindGreaterOrEqual(RingPosition(key))
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

func (r *Ring) WithEvent(event cluster.RingEvent) *Ring {
	ret := r.copy()
	ret.seq = cluster.Seq

	for _, node := range cluster.Nodes {
		current, ext := r.nodes[node.NodeID]
		if node.IsRemoved() {
			if ext {
				r.ring.Delete(current)
				delete(r.nodes, node.NodeID)
			}
		} else {
			if ext {
				r.ring.Delete(current)
			}
			r.nodes[node.NodeID] = node
			r.ring.Insert(reference{
				node.RingPosition, node.NodeID,
			})
		}
	}

	return ret
}

func (r *Ring) copy() *Ring {
	nodes := make(map[string]RingNode, len(r.nodes))

	ring := skiplist.New()
	for e := r.ring.GetSmallestNode(); e != nil; e = r.ring.Next(e) {
		ring.Insert(e.GetValue())
	}

	for id, nd := range r.nodes {
		nodes[id] = nd
	}

	return &Ring{
		seq:   r.seq,
		ring:  ring,
		nodes: nodes,
	}
}

type RingNode struct {
	NodeID string
	RingPosition
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

func (e RingPosition) IsRemoved() bool {
	return e < 0
}

type reference struct {
	RingPosition
	NodeID string
}