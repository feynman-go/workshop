package parallel

import (
	"context"
	"sync"
	"time"
)

func Run(ctx context.Context, exitWait time.Duration, runner ...func(ctx context.Context) error) error {
	exitChan := make(chan struct{})
	for i := range runner {
		r := runner[i]
		go func(ctx context.Context) error {
			defer func() {
				exitChan <- struct{}{}
			}()
			return r(ctx)
		}(ctx)
	}

	select {
	case <- ctx.Done():
		select {

		}
	}
}


type status int

const (
	statusInit = 0
	statusRunning = 1
	statusStoped = 2
	statusExit = 3
)

type Runner struct {
	rw sync.RWMutex
	prob *Prob
	expect status
	actual status
	sub map[*Runner]bool
	depOn map[*Runner]bool
	runner func(ctx context.Context) error
}

func (p *Runner) Fork(parallel *Runner) {

}

func (p *Runner) ForkGroup(coupled bool, runner ...func(ctx context.Context) error) {

}

func (p *Runner) run(ctx context.Context, prob *Prob) {

}

func (p *Runner) updateExpectStatus(status status) error {

}

func (p *Runner) Stop() {

}

func (p *Runner) start(ctx context.Context) {

}

func (p *Runner) ExpectStatus() {

}

func (p *Runner) ActualStatus() {

}


type group struct {
	coupled bool

}

type Prob struct {

}