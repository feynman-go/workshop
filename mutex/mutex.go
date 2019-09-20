package mutex

import (
	"context"
	"sync"
)

type Mutex struct {
	rwm sync.RWMutex
	cn  chan struct{}
}

func (mx *Mutex) Hold(ctx context.Context) bool {
	var cn chan struct{}
	for ctx.Err() == nil {
		mx.rwm.RLock()
		cn = mx.cn
		mx.rwm.RUnlock()
		if cn != nil {
			select {
			case <- cn:
			case <- ctx.Done():
				return false
			}
		}
		if mx.occupy() {
			return true
		}
	}
	return false
}

func (mx *Mutex) occupy() bool {
	mx.rwm.Lock()
	defer mx.rwm.Unlock()
	if mx.cn == nil {
		mx.cn = make(chan struct{})
		return true
	}
	return false
}

func (mx *Mutex) Release() {
	mx.rwm.Lock()
	defer mx.rwm.Unlock()
	if mx.cn == nil {
		return
	}
	close(mx.cn)
	mx.cn = nil
}