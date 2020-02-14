package prob

import (
	"context"
	"github.com/feynman-go/workshop/richclose"
	"github.com/pkg/errors"
	"sync"
	"time"
)

const (
	_EXPECT_STATE_INIT = 0
	_EXPECT_STATE_UP   = 1
	_EXPECT_STATE_DOWN     = 2
)

type Prob struct {
	rw           sync.RWMutex
	cancel       func()

	expectState int32

	//stopAccepted bool
	stopChan     chan struct{}
	runningChan  chan struct{}
	f            func(ctx context.Context)
	//started      bool
}

func New(f func(ctx context.Context)) *Prob {
	pb := &Prob{
		stopChan:   make(chan struct{}),
		runningChan:   make(chan struct{}),
		f:          f,
	}
	return pb
}

func (prob *Prob) run() {
	defer prob.didStopped()
	runCtx, cancel := context.WithCancel(context.Background())
	// notify update
	if !prob.didRunning(cancel) {
		return
	}
	if prob.f != nil {
		prob.f(runCtx)
	}
}

func (prob *Prob) Stop() {
	prob.rw.Lock()
	defer prob.rw.Unlock()

	switch prob.expectState {
	case _EXPECT_STATE_INIT, _EXPECT_STATE_UP:
		prob.notifyStop()
		prob.expectState = _EXPECT_STATE_DOWN

		select {
		// 还未开始
		case <- prob.runningChan:
		default:
			close(prob.stopChan)
		}

	case _EXPECT_STATE_DOWN:
	}
}

func (prob *Prob) StopAccepted() bool {
	prob.rw.RLock()
	defer prob.rw.RUnlock()

	return prob.expectState == _EXPECT_STATE_DOWN
}

func (prob *Prob) Stopped() chan struct{} {
	return prob.stopChan
}

func (prob *Prob) didRunning(cancel func()) bool {
	prob.rw.Lock()
	defer prob.rw.Unlock()

	if prob.expectState == _EXPECT_STATE_UP {
		prob.cancel = cancel
		close(prob.runningChan)
		return true
	}
	return false
}

func (prob *Prob) didStopped() {
	prob.rw.Lock()
	defer prob.rw.Unlock()

	prob.cancel = nil

	select {
	case <- prob.stopChan:
	default:
		close(prob.stopChan)
	}
}

func (prob *Prob) IsStopped() bool {
	select {
	case <- prob.stopChan:
		return true
	default:
		return false
	}
}

func (prob *Prob) IsRunning() bool {
	select {
	case <- prob.stopChan:
		return false
	case <- prob.runningChan:
		select {
		case <- prob.stopChan:
			return false
		default:
			return true
		}
		return true
	default:
		return false
	}
}

func (prob *Prob) notifyStop() {
	if prob.cancel != nil {
		prob.cancel()
	}
}

func (prob *Prob) Running() <- chan struct{}{
	prob.rw.RLock()
	defer prob.rw.RUnlock()
	return prob.runningChan
}

func (prob *Prob) Start() bool {
	prob.rw.Lock()
	defer prob.rw.Unlock()

	switch prob.expectState {
	case _EXPECT_STATE_INIT:
		prob.expectState = _EXPECT_STATE_UP
		go prob.run()
	case _EXPECT_STATE_UP:
		return false
	case _EXPECT_STATE_DOWN:
		return false
	}
	return true
}

func (prob *Prob) StopAndWait(ctx context.Context) error {
	prob.Stop()
	prob.rw.Lock()
	prob.rw.Unlock()

	select {
	case <- ctx.Done():
		return ctx.Err()
	case <- prob.Stopped():
		return nil
	}
}

func (prob *Prob) StopAndWaitDuration(duration time.Duration) error {
	prob.Stop()
	select {
	case <- time.After(duration):
		return errors.New("stop timeout")
	case <- prob.Stopped():
		return nil
	}
}


func RunSync(ctx context.Context, prob *Prob, f func(ctx context.Context)) bool {
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

func WrapCloser(pb *Prob) richclose.WithContextCloser {
	return closer{pb}
}

type closer struct {
	pb *Prob
}

func (c closer) CloseWithContext(ctx context.Context) error{
	return c.pb.StopAndWait(ctx)
}