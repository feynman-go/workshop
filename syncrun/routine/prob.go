package routine

import (
	"context"
	"github.com/feynman-go/workshop/closes"
	"github.com/pkg/errors"
	"sync"
	"time"
)

const (
	_EXPECT_STATE_INIT = 0
	_EXPECT_STATE_UP   = 1
	_EXPECT_STATE_DOWN     = 2
)

// a easy manager go routine based on goroutine
type Routine struct {
	rw           sync.RWMutex
	cancel       func()

	expectState int32

	//stopAccepted bool
	stopChan     chan struct{}
	runningChan  chan struct{}
	f            func(ctx context.Context)
	//started      bool
}

func New(f func(ctx context.Context)) *Routine {
	pb := &Routine{
		stopChan:   make(chan struct{}),
		runningChan:   make(chan struct{}),
		f:          f,
	}
	return pb
}

func (r *Routine) run() {
	defer r.didStopped()
	runCtx, cancel := context.WithCancel(context.Background())
	// notify update
	if !r.didRunning(cancel) {
		return
	}
	if r.f != nil {
		r.f(runCtx)
	}
}

func (r *Routine) Stop() {
	r.rw.Lock()
	defer r.rw.Unlock()

	switch r.expectState {
	case _EXPECT_STATE_INIT, _EXPECT_STATE_UP:
		r.notifyStop()
		r.expectState = _EXPECT_STATE_DOWN

		select {
		// 还未开始
		case <- r.runningChan:
		default:
			close(r.stopChan)
		}

	case _EXPECT_STATE_DOWN:
	}
}

func (r *Routine) StopAccepted() bool {
	r.rw.RLock()
	defer r.rw.RUnlock()

	return r.expectState == _EXPECT_STATE_DOWN
}

func (r *Routine) Stopped() chan struct{} {
	return r.stopChan
}

func (r *Routine) didRunning(cancel func()) bool {
	r.rw.Lock()
	defer r.rw.Unlock()

	if r.expectState == _EXPECT_STATE_UP {
		r.cancel = cancel
		close(r.runningChan)
		return true
	}
	return false
}

func (r *Routine) didStopped() {
	r.rw.Lock()
	defer r.rw.Unlock()

	r.cancel = nil

	select {
	case <- r.stopChan:
	default:
		close(r.stopChan)
	}
}

func (r *Routine) IsStopped() bool {
	select {
	case <- r.stopChan:
		return true
	default:
		return false
	}
}

func (r *Routine) IsRunning() bool {
	select {
	case <- r.stopChan:
		return false
	case <- r.runningChan:
		select {
		case <- r.stopChan:
			return false
		default:
			return true
		}
		return true
	default:
		return false
	}
}

func (r *Routine) notifyStop() {
	if r.cancel != nil {
		r.cancel()
	}
}

func (r *Routine) Running() <- chan struct{}{
	r.rw.RLock()
	defer r.rw.RUnlock()
	return r.runningChan
}

func (r *Routine) Start() bool {
	r.rw.Lock()
	defer r.rw.Unlock()

	switch r.expectState {
	case _EXPECT_STATE_INIT:
		r.expectState = _EXPECT_STATE_UP
		go r.run()
	case _EXPECT_STATE_UP:
		return false
	case _EXPECT_STATE_DOWN:
		return false
	}
	return true
}

func (r *Routine) StopAndWait(ctx context.Context) error {
	r.Stop()
	r.rw.Lock()
	r.rw.Unlock()

	select {
	case <- ctx.Done():
		return ctx.Err()
	case <- r.Stopped():
		return nil
	}
}

func (r *Routine) StopAndWaitDuration(duration time.Duration) error {
	r.Stop()
	select {
	case <- time.After(duration):
		return errors.New("stop timeout")
	case <- r.Stopped():
		return nil
	}
}


func RunSync(ctx context.Context, prob *Routine, f func(ctx context.Context)) bool {
	select {
	case <- prob.Stopped():
		return false
	case <- ctx.Done():
		return false
	case <- prob.Running():
		var wg = &sync.WaitGroup{}
		var runCtx, cancel = context.WithCancel(ctx)
		wg.Add(1)
		var res bool
		go func() {
			defer cancel()
			defer wg.Done()
			if runCtx.Err() != nil {
				return
			}
			f(runCtx)
			res = true
		}()

		select {
		case <- prob.Stopped():
			cancel()
		case <- runCtx.Done():
		}
		wg.Wait()
		return res
	}

}

func WrapCloser(pb *Routine) closes.WithContextCloser {
	return closer{pb}
}

type closer struct {
	pb *Routine
}

func (c closer) CloseWithContext(ctx context.Context) error{
	return c.pb.StopAndWait(ctx)
}