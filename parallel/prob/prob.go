package prob

import (
	"context"
	"github.com/feynman-go/workshop/randtime"
	"sync"
	"time"
)

type Prob struct {
	rw          sync.RWMutex
	readyToRun  bool
	running     bool
	cancel      func()
	updateChan  chan struct{}
	closeAccepted bool
	closeChane  chan struct{}
}

func New() *Prob {
	return &Prob{
		updateChan: make(chan struct{}, 1),
		closeChane:   make(chan struct{}, 2),
	}
}

func (prob *Prob) Close() {
	prob.rw.Lock()
	defer prob.rw.Unlock()

	if !prob.closeAccepted {
		prob.closeAccepted = true
		prob.stop()
		select {
		case prob.updateChan <- struct{}{}:
		default:
		}
	}
}

func (prob *Prob) CloseAccepted() bool {
	prob.rw.Lock()
	defer prob.rw.Unlock()

	return prob.closeAccepted
}

func (prob *Prob) Closed() chan struct{} {
	return prob.closeChane
}

func (prob *Prob) didRunning(cancel func()) {
	prob.rw.Lock()
	defer prob.rw.Unlock()

	prob.running = true
	prob.cancel = cancel
}

func (prob *Prob) didStopped() {
	prob.rw.Lock()
	defer prob.rw.Unlock()

	prob.running = false
	prob.cancel = nil
	if prob.closeAccepted {
		select {
		case <- prob.closeChane:
		default:
			close(prob.closeChane)
		}
	}
}

func (prob *Prob) stop() {
	prob.readyToRun = false
	if prob.cancel != nil {
		prob.cancel()
	}
}

func (prob *Prob) Stop() {
	prob.rw.Lock()
	defer prob.rw.Unlock()

	if prob.closeAccepted {
		return
	}

	prob.stop()
}

func (prob *Prob) Start() {
	prob.rw.Lock()
	defer prob.rw.Unlock()

	if prob.closeAccepted {
		return
	}

	prob.readyToRun = true
	select {
	case prob.updateChan <- struct{}{}:
	default:
	}
}

func (prob *Prob) ReadyToRun() bool {
	prob.rw.RLock()
	defer prob.rw.RUnlock()

	if prob.closeAccepted {
		return false
	}

	return prob.readyToRun
}

func (prob *Prob) Running() bool {
	prob.rw.RLock()
	defer prob.rw.RUnlock()
	return prob.running
}

func SyncRun(prob *Prob, runFunc func(ctx context.Context)) {
	tk := time.NewTicker(time.Second)
	for {

		select {
		case <- prob.updateChan:
		case <- tk.C:
		case <- prob.closeChane:
			return
		}

		if prob.CloseAccepted() {
			prob.didStopped()
		}

		if prob.ReadyToRun() {
			runCtx, cancel := context.WithCancel(context.Background())
			// notify update
			prob.didRunning(cancel)
			runFunc(runCtx)
			prob.didStopped()
		}
	}
}

func SyncRunWithRestart(prob *Prob, runFunc func(ctx context.Context) (canRestart bool), restartWait func() time.Duration) {
	SyncRun(prob, func(ctx context.Context) {
		for ctx.Err() == nil {
			if !runFunc(ctx) {
				return
			}
			wait := restartWait()
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <- ctx.Done():
				case <- timer.C:
				}
			}
		}
	})
}

func RandRestart(min, max time.Duration) func() time.Duration {
	return func() time.Duration {
		return randtime.RandDuration(min, max)
	}
}