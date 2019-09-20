package prob

import (
	"context"
	"sync"
)

type Prob struct {
	rw           sync.RWMutex
	cancel       func()
	stopAccepted bool
	stopChan     chan struct{}
	runningChan  chan struct{}
	f            func(ctx context.Context)
	started      bool
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
	prob.didRunning(cancel)
	if prob.f != nil {
		prob.f(runCtx)
	}
}

func (prob *Prob) Stop() {
	prob.rw.Lock()
	defer prob.rw.Unlock()

	if !prob.stopAccepted {
		prob.stopAccepted = true
		prob.stop()
	}
}

func (prob *Prob) Stopping() bool {
	prob.rw.Lock()
	defer prob.rw.Unlock()

	return prob.stopAccepted
}

func (prob *Prob) Stopped() chan struct{} {
	return prob.stopChan
}

func (prob *Prob) didRunning(cancel func()) {
	prob.rw.Lock()
	defer prob.rw.Unlock()

	if !prob.IsRunning() && !prob.IsStopped() {
		close(prob.runningChan)
	}
	prob.cancel = cancel
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
		return true
	default:
		return false
	}
}


func (prob *Prob) didStopped() {
	prob.rw.Lock()
	defer prob.rw.Unlock()

	prob.stop()
	prob.cancel = nil
	if !prob.IsRunning() {
		close(prob.runningChan)
	}
	if !prob.IsStopped() {
		close(prob.stopChan)
	}
}

func (prob *Prob) stop() {
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

	if prob.stopAccepted || prob.IsStopped() || prob.IsRunning() || prob.started {
		return false
	}
	go prob.run()
	prob.started = true
	return true
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