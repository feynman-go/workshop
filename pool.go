package workshop

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

type TaskFunc func(ctx context.Context)

type Pool struct {
	max     int
	offset  uintptr
	workers *sync.Map
	p *sync.Pool
	closed chan struct{} // Close channel to close pool
}

func NewPool(maxConcurrent int) *Pool {
	closed := make(chan struct{}, 0)
	pool := &Pool{
		max:     maxConcurrent,
		workers: &sync.Map{},
		closed: closed,
		p: &sync.Pool{
			New: func() interface{} {
				return &worker{
					closed: closed,
					c: make(chan taskBox, 1),
				}
			},
		},
	}
	return pool
}

func (pool *Pool) feedAnyway(box taskBox) error {
	var ct int
	for ct < pool.max {
		id := atomic.AddUintptr(&pool.offset, 1)
		if pool.tryFeed(box, int(id)) {
			return nil
		}
		ct++
	}
	return fmt.Errorf("over max feed times %v", ct)
}

func (pool *Pool) tryFeed(box taskBox, id int) bool {
	var cn = pool.getWorker(id)
	select {
	case cn <- box:
		return true
	default:
		return false
	}
}

func (pool *Pool) getWorker(hashId int) chan <- taskBox {
	id := hashId % pool.max
	wk := pool.p.Get()
	a, loaded := pool.workers.LoadOrStore(id, wk)
	if loaded {
		pool.p.Put(wk)
	} else {
		go func() {
			for {
				err := wk.(*worker).run()
				if err != nil {
					log.Println("run worker err", err, ". rerun worker")
				}
			}
		}()
	}
	return a.(*worker).c
}

type worker struct {
	mu sync.Mutex
	closed chan struct{}
	c  chan taskBox
}

func (w *worker) run() (err error) {
	c := w.c
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()
			err = fmt.Errorf("recove from %v", r)
			return
		}
	}()
	for {
		select {
		case <- w.closed:
			return nil
		case box, ok := <-c:
			if !ok {
				return nil
			}
			newCtx, cancel := context.WithCancel(context.Background())
			go func(taskClose, workderClose <- chan struct{}) {
				select {
				case <- workderClose:
					cancel()
				case <- taskClose:
					cancel()
				}
			}(box.closed, w.closed)
			box.f(newCtx)
			cancel()
		}
	}
}


type taskBox struct {
	closed <- chan struct{}
	f TaskFunc
}
