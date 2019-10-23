package balance

import (
	"fmt"
	"github.com/MauriceGit/skiplist"
	"sync"
)

type Ring struct {
	transaction int64
	max int64
	rw sync.RWMutex
	list skiplist.SkipList
}

func NewRing(max int64) *Ring {
	return &Ring {
		max: max,
		list: skiplist.New(),
	}
}

func (r *Ring) MapNode(key int64) *Node {
	r.rw.RLock()
	defer r.rw.RUnlock()

	e, _ :=  r.list.FindGreaterOrEqual(element(key))
	if e == nil {
		e = r.list.GetSmallestNode()
	}
	if e != nil {
		return e.GetValue().(*Node)
	}
	return nil
}

func (r *Ring) AddNode(node Node, overwrite bool) (Range, bool) {
	r.rw.Lock()
	defer r.rw.Unlock()

	input := element(node.start)
	e, _ := r.list.FindGreaterOrEqual(input)
	if e == nil {
		r.list.Insert(input)
		return Range{}, false
	}
	if e.GetValue() == input {
		return Range{}, false
	}
}

func (r *Ring) GetNode(id int64) *Node {

}

func (r *Ring) RemoveNode(start int64, overwrite bool) (Range, bool) {

}

func (r *Ring) GetAffectRange(start int64) Range {

}

func (r *Ring) Replace(fromNodeKey int64, toNodeKey int64) {

}

func (r *Ring) WithTransaction(transaction Transaction) *Ring {

}

type Range struct {
	From *Node
	To *Node
}

type Node struct {
	id int64
	start int64
	length int64
	availability float64
}

func (e Node) ID() int64 {
	return e.id
}

func (e Node) Available() float64 {
	return e.availability
}

func (e Node) StartKey() int64 {
	return e.start
}

// not equal
func (e Node) EndKey() int64 {
	return e.start + e.length
}

func (e Node) ExtractKey() float64 {
	return float64(e.start)
}

func (e Node) String() string {
	return fmt.Sprintf("%03d", e.start)
}

type element int64

func (e element) ExtractKey() float64 {
	return float64(e)
}
func (e element) String() string {
	return fmt.Sprintf("%03d", e)
}

type NodeUpdate struct {
	Form *Node // if from is nil, means node is insert
	To *Node // if to is nil, means node is removed
}

type Transaction struct {
	Sequence int64
	Updates []NodeUpdate
}

type RingScheduler interface {
	ScheduleRing(ring *Ring) (*Transaction, error)
}
