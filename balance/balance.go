package balance

import (
	"sort"
	"sync"
	"sync/atomic"
)

type Balancer struct {
	rw       sync.RWMutex
	instance []*Instance
	schedule Scheduler
}

func New(schedule Scheduler, instances []*Instance) *Balancer {
	ret := &Balancer{
		instance: instances,
		schedule: schedule,
	}
	for i, in := range instances {
		in.updateRef(i, ret)
		in.UpdateScore(in.Score())
	}
	return ret
}

func (b *Balancer) Pick(partition int64) *Instance {
	b.rw.RLock()
	defer b.rw.RUnlock()
	l := len(b.instance)

	i := b.schedule.Pick(partition, l)
	if i < 0 || i >= l {
		return nil
	}
	ret := b.instance[i]
	if ret.Score() < 0 {
		return nil
	}
	return ret
}

func (b *Balancer) updateInstanceStatus(i int, score int32) {
	b.rw.RLock()
	defer b.rw.RUnlock()
	l := len(b.instance)
	if i < 0 || i >= l {
		return
	}
	b.schedule.UpdateStatus(i, score)
}

type Instance struct {
	rw    sync.RWMutex
	score int32
	index int
	v     interface{}
	b     *Balancer
}

func NewInstance(v interface{}, score int32) *Instance {
	return &Instance{
		v:     v,
		score: score,
	}
}

// > 0 healthy; < 0 unhealthy; == 0 unknown, more high score more health, can also use as weight
func (ins *Instance) Score() int32 {
	ins.rw.RLock()
	defer ins.rw.RUnlock()
	return ins.score
}

func (ins *Instance) Value() interface{} {
	ins.rw.RLock()
	defer ins.rw.RUnlock()
	return ins.v
}

func (ins *Instance) Update(v interface{}, score int32) {
	ins.rw.Lock()
	defer ins.rw.Unlock()
	ins.v = v
	ins.updateScore(score)
}

func (ins *Instance) UpdateValue(v interface{}) {
	ins.rw.Lock()
	defer ins.rw.Unlock()
	ins.v = v
}

func (ins *Instance) UpdateScore(score int32) {
	ins.rw.Lock()
	defer ins.rw.Unlock()
	ins.updateScore(score)
}

func (ins *Instance) updateScore(score int32) {
	ins.score = score
	if ins.b != nil {
		ins.b.updateInstanceStatus(ins.index, score)
	}
}

func (ins *Instance) updateRef(index int, b *Balancer) {
	ins.rw.Lock()
	defer ins.rw.Unlock()
	ins.index = index
	ins.b = b
}

type Scheduler interface {
	// return < 0 means no instance available
	Pick(partition int64, total int) int
	UpdateStatus(index int, score int32)
}

// scheduler implement
type RoundRobin struct {
	rw         sync.RWMutex
	monotonous uint64
	list       []*robinItem
	cur        uint64
	sum        uint64
	start      int
}

type robinItem struct {
	index int
	score int32
	start uint64
}

func (rr *RoundRobin) Pick(partition int64, total int) int {
	rr.rw.RLock()
	defer rr.rw.RUnlock()

	l := len(rr.list)

	cur := atomic.AddUint64(&rr.monotonous, 1)
	idx := cur % rr.sum

	i := sort.Search(l, func(i int) bool {
		r := rr.list[i]
		return r.score >= 0 && (r.start+uint64(r.score)+1) > idx
	})
	if i < 0 || i >= l {
		return -1
	}
	return rr.list[i].index
}

func (rr *RoundRobin) UpdateStatus(index int, score int32) {
	rr.rw.Lock()
	defer rr.rw.Unlock()

	var ext bool

	var start uint64 = 0
	rr.sum = 0
	for _, r := range rr.list {
		if r.index == index {
			r.score = score
			ext = true
		}
		r.start = start
		if r.score >= 0 {
			ct := 1 + uint64(r.score)
			start = r.start + ct
			rr.sum += ct
		}
	}
	if !ext {
		rr.list = append(rr.list, &robinItem{
			index: index,
			score: score,
			start: start,
		})
		ct := 1 + uint64(score)
		rr.sum += ct
	}
}
