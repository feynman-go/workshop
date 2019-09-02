package promise

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type TaskFunc func(ctx context.Context, localId int)

type Pool struct {
	status  int32
	max     int
	offset  uintptr
	workers *sync.Map
	p       *sync.Pool
	closed  chan struct{} // Close channel to close pool
}

func NewPool(maxConcurrent int) *Pool {
	closed := make(chan struct{}, 0)
	pool := &Pool{
		max:     maxConcurrent,
		workers: &sync.Map{},
		closed:  closed,
		p: &sync.Pool{
			New: func() interface{} {
				return &worker{
					closed: closed,
					c:      make(chan TaskBox),
				}
			},
		},
	}
	pool.startWorkers()
	return pool
}

func (pool *Pool) MaxLocalID() int {
	return pool.max
}

func (pool *Pool) Feed(ctx context.Context, box TaskBox) error {
	if box.stubborn {
		if !pool.tryFeedBlock(ctx, box, box.localId) {
			return errors.New("stubborn " +
				"feed block")
		}
	} else {
		id := atomic.AddUintptr(&pool.offset, 1)
		if !pool.tryFeedBlock(ctx, box, int(id)) {
			return errors.New("feed block")
		}
	}
	return nil
}

func (pool *Pool) tryFeedNoBlock(box TaskBox, id int) bool {
	var wk = pool.getWorker(id)
	select {
	case <-pool.closed:
		return false
	case <-box.closed:
		return false
	case wk.c <- box:
		return true
	default:
		return false
	}
}

func (pool *Pool) tryFeedBlock(ctx context.Context, box TaskBox, id int) bool {
	var wk = pool.getWorker(id)
	select {
	case <-pool.closed:
		return false
	case <-box.closed:
		return false
	case wk.c <- box: // first check not in loop, why? more faster
		return true
	case <-ctx.Done():
		return false
	default:
		//
		for {
			select {
			case <-box.closed:
				return false
			case <-ctx.Done():
				return false
			case wk.c <- box:
				return true
			case <-pool.closed:
				return false
			}
		}
	}
}

func (pool *Pool) startWorkers() {
	for i := 0; i < pool.max; i++ {
		wk := pool.p.Get()
		pool.workers.Store(i, wk)
		go func(wk *worker) {
			for {
				err := wk.run()
				if err != nil {
					log.Println("run worker err", err, ". rerun worker")
				}
			}
		}(wk.(*worker))
	}
}

func (pool *Pool) Close() error {
	if atomic.CompareAndSwapInt32(&pool.status, 0, -1) {
		close(pool.closed)
		return nil
	}
	return nil
}

func (pool *Pool) getWorker(hashId int) *worker {
	id := hashId % pool.max
	wk, _ := pool.workers.Load(id)
	//wk := pool.p.Get()
	/*a, loaded := pool.workers.Load(id)
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
	}*/
	return wk.(*worker)
}

type worker struct {
	idx    int
	mu     sync.Mutex
	closed chan struct{}
	c      chan TaskBox
}

func (w *worker) run() (err error) {
	c := w.c
	defer func() {
		if r := recover(); r != nil {
			//debug.PrintStack()
			err = fmt.Errorf("recove from %v", r)
			return
		}
	}()

	idx := w.idx
	for {
		select {
		case <-w.closed:
			return nil
		case box, ok := <-c:
			if !ok {
				return nil
			}
			newCtx, cancel := context.WithCancel(context.Background())
			go func(ctx context.Context, taskClose, workerClose <-chan struct{}, cancel func()) {
				select {
				case <-workerClose:
					cancel()
				case <-taskClose:
					cancel()
				case <-newCtx.Done():
				}
			}(newCtx, box.closed, w.closed, cancel)
			box.f(newCtx, idx)
			cancel()
		}
	}
}

type TaskBox struct {
	closed   <-chan struct{}
	f        TaskFunc
	stubborn bool
	localId  int
}
