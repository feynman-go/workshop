package client

import (
	"context"
	"errors"
	"github.com/feynman-go/workshop/breaker"
	"github.com/valyala/fastrand"
	"golang.org/x/time/rate"
	"sync"
)

type Refresher func(last interface{}) (new interface{}, err error)

type memoryPool struct {
	rw sync.RWMutex
	rmp map[*resourceInstance]int
	indies []*resourceInstance
}

func NewMemoryPool(count int, refresher Refresher, refreshLimit rate.Limit, config breaker.StatusConfig) ResourcePool {
	ret := &memoryPool{
		indies: []*resourceInstance{},
		rmp: map[*resourceInstance]int{},
	}
	for i := 0 ; i < count; i ++ {
		instance := &resourceInstance{
			status: breaker.NewStatus(config),
			refresh: refresher,
			refreshLimiter: rate.NewLimiter(refreshLimit, 1),
		}
		ret.indies = append(ret.indies, instance)
		ret.rmp[instance] = len(ret.indies) - 1
	}
	return ret
}

func(mp *memoryPool) Get(ctx context.Context, partition bool, partitionKey int) (*Resource, error) {
	mp.rw.RLock()
	l := len(mp.indies)
	if l == 0 {
		mp.rw.RUnlock()
		return nil, errors.New("not support")
	}
	var idx int
	if partition {
		idx = partitionKey % len(mp.indies)
	} else {
		idx = int(fastrand.Uint32n(uint32(l)))
	}
	ri := mp.indies[idx]
	defer mp.rw.RUnlock()

	v := ri.get()
	if v == nil {
		return nil, errors.New("resource unavailable")
	}
	return NewResource(mp.putBackFunc(ri), v), nil
}

func(mp *memoryPool) putBackFunc(ri *resourceInstance) func(abnormal bool) {
	return func(abnormal bool) {
		ri.record(abnormal)
		if abnormal && ri.status.StatusCode() != breaker.StatusCodeAbnormal {
			ri.resetWithLock()
		}
	}
}

type resourceInstance struct {
	status *breaker.Status
	rw sync.RWMutex
	v interface{}
	refresh Refresher
	refreshErr error
	refreshLimiter *rate.Limiter
}

func (res *resourceInstance) available() bool {
	return res.status.StatusCode() != breaker.StatusCodeAbnormal
}

func (res *resourceInstance) get() interface{} {
	res.rw.Lock()
	defer res.rw.Unlock()
	if res.status.StatusCode() == breaker.StatusCodeAbnormal {
		return nil
	}
	if res.v == nil {
		res.reset()
	}
	return res.v
}

func (res *resourceInstance) record(abnormal bool) {
	res.status.Record(abnormal)
}

func (res *resourceInstance) resetWithLock() {
	res.rw.Lock()
	defer res.rw.Unlock()
	res.reset()
	return
}

func (res *resourceInstance) reset() {
	if res.refresh != nil {
		var err error
		if !res.refreshLimiter.Allow() {
			res.status.ConvertToAbnormal()
			res.v = nil
			return
		}
		res.v, err = res.refresh(res.v)
		if err != nil {
			res.status.ConvertToAbnormal()
			res.refreshErr = err
		} else {
			res.refreshErr = nil
		}
	}
}

