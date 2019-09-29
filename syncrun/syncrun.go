package syncrun

import (
	"context"
	"github.com/feynman-go/workshop/randtime"
	"sync"
	"time"
)

func Run(ctx context.Context, runner ...func(ctx context.Context)) {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)

	startGroup := &sync.WaitGroup{}
	endGroup := &sync.WaitGroup{}

	for _, f := range runner {
		startGroup.Add(1)
		endGroup.Add(1)
		go func(ctx context.Context, f func(ctx context.Context), cancel func()) {
			startGroup.Done()
			defer cancel()
			defer endGroup.Done()
			if ctx.Err() == nil {
				f(ctx)
			}
		}(ctx, f, cancel)
	}

	startGroup.Wait()
	endGroup.Wait()
}



func FuncWithRandomStart(runFunc func(ctx context.Context) (canRestart bool), restartWait func() time.Duration) func(ctx context.Context) {
	return func(ctx context.Context) {
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
	}
}

func RandRestart(min, max time.Duration) func() time.Duration {
	return func() time.Duration {
		return randtime.RandDuration(min, max)
	}
}


//type Errors struct {
//	hasErr bool
//	errs map[int]error
//}
//
//func (errors Errors) HasErr() bool {
//	return len(errors.errs) > 0
//}
//
//func (errors Errors) Errors() map[int]error {
//	return errors.errs
//}
//
//func (errors Errors) GetErr(index int) error {
//	return errors.errs[index]
//}
//
//func RunAndReturn(ctx context.Context, runner ...func(ctx context.Context) error) Errors {
//
//}


//func RunByContext(ctx context.Context, runner ...func(ctx context.Context) ) {
//	rt := NewRoot()
//	for i := range runner {
//		r := runner[i]
//		rt.Fork(func(ctx context.Context) bool {
//			r(ctx)
//			return false
//		})
//	}
//	rt.StartExec()
//	<- rt.Closed()
//}
//
//type status int32
//
//const (
//	statusInit    status = 1
//	statusRunning status = 2
//	statusExit    status = 3
//)
//
//
//type Root struct {
//	runner *Runner
//}
//
//func NewRoot() *Root {
//	return &Root{
//		runner: Empty(),
//	}
//}
//
//func (root *Root) Runner() *Runner {
//	return root.runner
//}
//
//func (root *Root) StartExec() {
//	root.runner.port.notifyUpdate(nil, true)
//}
//
//func (root *Root) Fork(runner func(ctx context.Context) bool) *Runner {
//	return root.runner.Fork(runner)
//}
//
//func (root *Root) Release() {
//	root.runner.end()
//}
//
//func (root *Root) Closed() <- chan struct{} {
//	return root.runner.Closed()
//}
//
//type Runner struct {
//	depBy  map[*Port]bool
//	depOn  map[*Port]bool
//	runFunc func(ctx context.Context) (rerun bool)
//	started bool
//
//	rw sync.RWMutex
//	status status
//	depUpdateChan chan *Port
//	prob *prob.Prob
//
//	port *Port
//}
//
//
//func New(runFunc func(ctx context.Context) bool) *Runner {
//	runner := &Runner{
//		depBy:  map[*Port]bool{},
//		depOn:  map[*Port]bool{},
//		runFunc: runFunc,
//		depUpdateChan: make(chan *Port, 2),
//		prob: prob.New(),
//		status: statusInit,
//	}
//
//	runner.port = &Port {
//		runner: runner,
//	}
//
//	err := runner.start()
//	if err != nil {
//		panic(err)
//	}
//	return runner
//}
//
//func Empty() *Runner {
//	return New(func(ctx context.Context) bool{
//		<- ctx.Done()
//		return true
//	})
//}
//
//func (runner *Runner) Closed() <- chan struct{} {
//	return runner.prob.Closed()
//}
//
//func (runner *Runner) Separate() {
//	runner.prob.Release()
//	runner.rw.Lock()
//	defer runner.rw.Unlock()
//	for d := range runner.depOn {
//		d.removeDependBy(runner.port)
//	}
//}
//
//
//func (runner *Runner) DependOn(dependOn ...*Runner) {
//	runner.rw.Lock()
//	for _, d := range dependOn {
//		runner.depOn[d.port] = true
//	}
//	runner.rw.Unlock()
//	for _, d := range dependOn {
//		d.dependBy(runner)
//	}
//}
//
//func (runner *Runner) Fork(runFunc func(ctx context.Context) bool) *Runner {
//	ret := New(runFunc)
//	ret.DependOn(runner)
//	return ret
//}
//
//func (runner *Runner) ForkEmpty(ctx context.Context) *Runner {
//	return runner.Fork(func(ctx context.Context) bool{
//		<- ctx.Done()
//		return true
//	})
//}
//
//func (runner *Runner) dependBy(dependOn *Runner) {
//	runner.rw.Lock()
//	defer runner.rw.Unlock()
//	runner.depBy[dependOn.port] = true
//}
//
//
//
//func (runner *Runner) start() error {
//	runner.rw.Lock()
//	defer runner.rw.Unlock()
//
//	if !runner.started {
//		runner.started = true
//		go runner.runDaemon(context.Background())
//		go prob.Go(runner.prob, runner.run)
//	} else {
//		return errors.New("started")
//	}
//	return nil
//}
//
//func (runner *Runner) end() {
//	runner.rw.Lock()
//	defer runner.rw.Unlock()
//	runner.prob.Release()
//}
//
//func (runner *Runner) run(ctx context.Context) {
//	runner.rw.RLock()
//	fun := runner.runFunc
//	pb := runner.prob
//	runner.rw.RUnlock()
//
//	if fun == nil {
//		pb.Release()
//	} else {
//		ok := runner.runFunc(ctx)
//		if !ok {
//			pb.Release()
//		}
//	}
//}
//
//func (runner *Runner) runDaemon(ctx context.Context) {
//	var tk = time.NewTicker(time.Second)
//	runner.processStatus(ctx)
//	for {
//		select {
//		case <- tk.C:
//			runner.processStatus(ctx)
//		case <- runner.Closed():
//			runner.processStatus(ctx)
//			return
//		case <- runner.depUpdateChan: // may nil
//			runner.processStatus(ctx)
//		case <- ctx.Done():
//			return
//		}
//
//		select {
//		case <- runner.Closed():
//			return
//		default:
//		}
//	}
//}
//
//
//func (runner *Runner) processStatus(ctx context.Context) {
//	runner.rw.Lock()
//	var old = runner.status
//	var expect = old
//	//var pb = runner.self.prob
//	runner.rw.Unlock()
//
//	switch expect {
//	case statusInit:
//		for on := range runner.depOn {
//			if on.getStatus() == statusExit {
//				expect = statusExit
//				break
//			}
//		}
//		if expect != statusExit {
//			expect = statusRunning
//		}
//	case statusRunning:
//		for on := range runner.depOn {
//			if on.getStatus() == statusExit {
//				expect = statusExit
//				break
//			}
//		}
//	case statusExit:
//	}
//
//	runner.rw.Lock()
//	runner.status = expect // 跟新状态
//	runner.rw.Unlock()
//
//	runner.notifyAllDepBy()
//
//	if expect == statusExit {
//		runner.waitDepOnClose(ctx)
//	}
//
//	if old != expect {
//		runner.syncStatusToProb(expect)
//	}
//}
//
//func (runner *Runner) waitDepOnClose(ctx context.Context) {
//	var ls = make([]*Port, 0, 5)
//	runner.rw.Lock()
//	for d := range runner.depOn {
//		ls = append(ls, d)
//	}
//	runner.rw.Unlock()
//
//	for _, l := range ls {
//		l.end()
//		select {
//		case <- l.closed():
//		case <- ctx.Done():
//			break
//		}
//	}
//}
//
//func (runner *Runner) notifyAllDepBy() {
//	runner.rw.Lock()
//	defer runner.rw.Unlock()
//	for port := range runner.depBy {
//		port.notifyUpdate(port, true)
//	}
//}
//
//func (runner *Runner) syncStatusToProb(s status) {
//	prob := runner.prob
//	switch s {
//	case statusInit:
//		prob.Stop() // may never happen
//	case statusRunning:
//		prob.StartExec()
//	case statusExit:
//		prob.Release()
//	}
//}
//
//type Port struct {
//	runner *Runner
//}
//
//func (p *Port) closed() <- chan struct{} {
//	return p.runner.Closed()
//}
//
//func (p *Port) end() {
//	p.runner.end()
//}
//
//func (p *Port) getStatus() status {
//	p.runner.rw.RLock()
//	defer p.runner.rw.RUnlock()
//	return p.runner.status
//}
//
//func (p *Port) removeDependBy(port *Port) {
//	p.runner.rw.Lock()
//	defer p.runner.rw.Unlock()
//	delete(p.runner.depBy, port)
//}
//
//func (p *Port) notifyUpdate(pbChange *Port, syncWait bool) {
//	if syncWait {
//		select {
//		case p.runner.depUpdateChan <- p:
//		}
//	} else {
//		select {
//		case p.runner.depUpdateChan <- p:
//		default:
//		}
//	}
//}