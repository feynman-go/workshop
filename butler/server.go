package butler

import (
	"context"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
	"sync/atomic"
	"unsafe"

	"sync"
	"time"
)

const (
	RunnerStatusInit = iota
	RunnerStatusStarting
	RunnerStatusRunning
	RunnerStatusStopping
	RunnerStatusStoppedSuccess
	RunnerStatusStoppedError
	RunnerStatusWaitingRun
	RunnerStatusExists
	RunnerStatusZombie // to use for health check failed
)

const (
	RestartStrategyOnFailed = iota
	RestartStrategyNo
	RestartStrategyOnExitSuccess
	RestartStrategyAlways
)

const (
	ServerStatusBorn = iota
	ServerStatusUp
	ServerStatusDown
	ServerStatusZombie
	ServerStatusUnknown
	ServerStatusExist
)

type Status int32

type Butler struct {
	mx      sync.RWMutex
	servers map[string]*Server
	dependence map[string]map[string]bool
}

func New() *Butler {
	return &Butler{
		servers: map[string]*Server{},
		dependence: map[string]map[string]bool{},
	}
}

func (b *Butler) Until(ctx context.Context) {
	select {
	case <- ctx.Done():
		b.ExitAll()
	}
}

func (b *Butler) StartAll() {
	b.mx.RLock()
	defer b.mx.RUnlock()

	for _, srv := range b.servers {
		srv.Start()
	}
}

func (b *Butler) ExitAll() {
	b.mx.RLock()
	defer b.mx.RUnlock()

	for _, srv := range b.servers {
		srv.Exit()
	}
}

func (b *Butler) AddServer(template Template) (*Server, error) {
	b.mx.Lock()
	defer b.mx.Unlock()

	if template.Config.Name == "" {
		return nil, errors.New("empty server name")
	}

	if _, ext := b.servers[template.Config.Name]; ext {
		return nil, errors.New("already asyncExists")
	}

	srv := newServer(template, b)
	b.servers[srv.getName()] = srv

	for _, dep := range template.Config.Depends {
		s, ext := b.dependence[dep.DependOnServer]
		if !ext {
			s = map[string]bool{}
			b.dependence[dep.DependOnServer] = s
		}
		s[srv.getName()] = true
	}
	return srv, nil
}

func (b *Butler) GetServer(name string) *Server {
	b.mx.RLock()
	defer b.mx.RUnlock()
	v, ext := b.servers[name]
	if !ext {
		return nil
	}
	return v
}

func (b *Butler) GetDependentBy(name string) map[string]bool {
	b.mx.RLock()
	defer b.mx.RUnlock()
	v, ext := b.dependence[name]
	if !ext {
		return nil
	}
	return v
}


type ServerConfig struct {
	Name string
	Depends []ServerDependent
	Restart Restart
	StartWaiting time.Duration
	RestartRate rate.Limit
	HealthRate rate.Limit
}

type Template struct {
	Config ServerConfig
	Run func(ctx RunContext) (int, string)
	CheckStatus func(ctx context.Context) (Status, string)
}

type RunContext struct {
	*runStatus
	context.Context
}

type ServerDependent struct {
	DependOnServer string
	DependByServer string
	Required bool // runner require dependent server running, or stop
	After bool // only first work after dependent server work
}

type Restart struct {
	Cond int
	RetryDuration time.Duration
	RestartLimit *rate.Limiter
}

type runStatus struct {
	status       Status
	startChan    chan struct{}
	stopChan     chan struct{}
	lastMessage  string
	lastCode     int
	startingTime time.Time
	runTime      time.Time
	cancelTime   time.Time
	endTime      time.Time
	ctxCancel    func()
}

type Server struct {
	mx sync.RWMutex
	template *Template
	rn *runner
	expectChan chan struct{}
	expect     Status // current expect status
	butler *Butler
}

func newServer(template Template, b *Butler) *Server {
	var (
		srv = &Server{
			template: &template,
			butler: b,
		}
		mnt = monitor{}
	)
	mnt.srv = srv
	srv.rn = newRunner(template.Config.Restart, template.Run, mnt)
	return srv
}

func (r *Server) GetExpectStatus() Status {
	return Status(atomic.LoadInt32((*int32)(unsafe.Pointer(&r.expect))))
}

func (r *Server) Start() {
	r.mx.Lock()
	defer r.mx.Unlock()
	if r.expect == ServerStatusBorn {
		r.expectChan = make(chan struct{}, 1)
		go r.checkExpectStatusLoop()
		go r.rn.work()
	}
	r.updateServerStatus(ServerStatusUp)
}

func (r *Server) Exit() {
	r.mx.Lock()
	defer r.mx.Unlock()
	r.updateServerStatus(ServerStatusExist)
}


func (r *Server) getName() string {
	return r.template.Config.Name
}

// expect status manager goroutine
func (r *Server) checkExpectStatusLoop() {
	tk := time.NewTicker(time.Second)
	defer tk.Stop()
	var c chan struct{}
	for {
		r.mx.RLock()
		c = r.expectChan
		r.mx.RUnlock()
		select {
		case <- tk.C:
			select {
			case c <- struct{}{}:
			default:
			}
		case _, ok := <- c:
			if !ok {
				return
			}
			r.checkServerStatus()
		}
	}
}

func (r *Server) asyncCheckStatus() {
	select {
	case r.expectChan <- struct{}{}:
	default:
	}
}

func (r *Server) checkServerStatus() {
	r.mx.Lock()
	defer r.mx.Unlock()
	switch r.expect {
	case ServerStatusBorn:
		r.checkStatusBorn()
	case ServerStatusUp:
		r.checkStatusUp()
	case ServerStatusDown:
		r.checkStatusDown()
	}
	r.rn.asyncCheckRunnerStatus(r.expect)
	return
}

func (r *Server) getDepStatus(name string) Status {
	srv := r.butler.GetServer(name)
	if srv == nil {
		return ServerStatusUnknown
	}
	return srv.GetExpectStatus()
}

func (r *Server) updateServerStatus(status Status) {
	if r.expect == status {
		return
	}
	r.expect = status
	r.asyncCheckStatus()
}

func (r *Server) checkStatusBorn() {
	//var toStart = true
	//
	//for _, dep := range r.template.Config.Depends {
	//	//to status run
	//	if dep.DependOnServer == "" {
	//		continue
	//	}
	//	status := r.getDepStatus(dep.DependOnServer)
	//	if dep.After && status != ServerStatusUp {
	//		toStart = false
	//		break
	//	}
	//	if dep.Required && status != ServerStatusUp {
	//		toStart = false
	//		break
	//	}
	//}
	//if toStart {
	//	r.updateServerStatus(ServerStatusUp)
	//}
}


func (r *Server) checkStatusUp() {
	for _, dep := range r.template.Config.Depends {
		//to depStatus run
		if dep.DependOnServer == "" {
			continue
		}

		depStatus := r.getDepStatus(dep.DependOnServer)

		if dep.Required {
			switch depStatus {
			case ServerStatusDown:
				r.updateServerStatus(ServerStatusDown)
				return
			case ServerStatusZombie:
				r.updateServerStatus(ServerStatusDown)
				return
			}
		}

		if dep.After {
			switch depStatus {
			case ServerStatusDown:
				r.updateServerStatus(ServerStatusDown)
				return
			case ServerStatusZombie:
				r.updateServerStatus(ServerStatusDown)
				return
			}
		}
	}
	return
}

func (r *Server) checkStatusDown() {
	for _, dep := range r.template.Config.Depends {
		//to depStatus run
		if dep.DependOnServer == "" {
			continue
		}
		depStatus := r.getDepStatus(dep.DependOnServer)
		if dep.Required {
			switch depStatus {
			case ServerStatusUp:
				r.updateServerStatus(ServerStatusUp)
				return
			}
		}
	}
	return
}

func (r *Server) DependentChangeStatus(dependentName string, status Status) {
	r.asyncCheckStatus()
}

func (r *Server) reportStatusChange(runStatus Status) {
	var needNotify bool

	serverStatus := r.GetExpectStatus()

	// 这边只需要同步实时的状态，周期性同步由checkStatusLoop触发
	r.mx.Lock()
	switch runStatus {
	case RunnerStatusExists:
		r.updateServerStatus(ServerStatusExist)
		needNotify = true
	case RunnerStatusStoppedSuccess, RunnerStatusStoppedError:
		switch serverStatus {
		case ServerStatusDown:
			needNotify = true
		case ServerStatusExist:
			r.rn.asyncCheckRunnerStatus(serverStatus)
			needNotify = true
		}
	case RunnerStatusRunning:
		if serverStatus == ServerStatusUp {
			needNotify = true
		}
	default:
		r.rn.asyncCheckRunnerStatus(serverStatus)
	}
	r.mx.Unlock()
	if needNotify {
		srvName := r.template.Config.Name
		for depSrvName, ok := range r.butler.GetDependentBy(r.getName()) {
			if ok {
				runner := r.butler.GetServer(srvName)
				if runner != nil {
					runner.DependentChangeStatus(depSrvName, r.GetExpectStatus())
				}
			}
		}
	}
}


type runner struct {
	runnerChan chan Status // chan value is of runner status
	*runStatus
	mx sync.RWMutex
	runFunc func(ctx RunContext) (int, string)
	restartCfg Restart
	restartLimit *rate.Limiter
	mnt monitor
}

func newRunner(restart Restart, runFunc func(ctx RunContext) (int, string), mnt monitor) *runner {
	return &runner{
		runFunc: runFunc,
		restartCfg: restart,
		runStatus: new(runStatus),
		mnt:mnt,
	}
}

func (r *runner) work() {
	r.mx.Lock()
	if r.runnerChan != nil {
		r.mx.Unlock()
		return
	}
	r.runnerChan = make(chan Status, 1)
	r.mx.Unlock()

	tk := time.NewTicker(time.Second)

	var status Status

	for {
		select {
		case <- tk.C:
			r.mx.RLock()
			status = r.status
			r.mx.RUnlock()
			r.mnt.OnRunnerStatusChange(status)
		case expect, ok := <- r.runnerChan:
			if !ok {
				return
			}
			r.checkRunnerStatus(expect)
		}
	}
}


// check runner status
func (r *runner) checkRunnerStatus(expect Status) {
	r.mx.Lock()
	switch expect {
	case ServerStatusUp:
		switch r.status {
		case RunnerStatusInit:
			r.startRun(0)
		case RunnerStatusStoppedError, RunnerStatusStoppedSuccess:
			r.restartRun()
		}
	case ServerStatusDown:
		switch r.status {
		case RunnerStatusRunning:
			r.stopRun()
		case RunnerStatusWaitingRun:
			r.stopRun()
		}
	case ServerStatusExist:
		switch r.status {
		case RunnerStatusStoppedSuccess, RunnerStatusStoppedError:
			r.changeRunnerStatus(RunnerStatusExists)
		default:
			r.stopRun()
		}
	case ServerStatusZombie:
	}
	r.mx.Unlock()
}


func (r *runner) asyncCheckRunnerStatus(expect Status) {
	select {
	case r.runnerChan <- expect:
	default:
	}

}

// under locker
func (r *runner) changeRunnerStatus(status Status) {
	if r.status == status {
		return
	}
	r.status = status
	switch r.status{
	case RunnerStatusExists,
	RunnerStatusStoppedError,
	RunnerStatusStoppedSuccess,
	RunnerStatusRunning:
		r.mnt.OnRunnerStatusChange(r.status)
	}
}


func (r *runner) startRun(wait time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())

	// clear status
	r.endTime = time.Time{}
	r.runTime = time.Time{}
	r.cancelTime = time.Time{}

	r.startChan = make(chan struct{})
	r.stopChan = make(chan struct{})
	r.startingTime = time.Now()
	r.changeRunnerStatus(RunnerStatusStarting)
	r.runStatus.ctxCancel = cancel

	rc := RunContext{r.runStatus, ctx}

	go r.run(0, rc)
	return
}

func (r *runner) restartRun() {
	cfg := r.restartCfg
	if r.canRestart() {
		r.startRun(cfg.RetryDuration)
	} else {
		r.changeRunnerStatus(RunnerStatusExists)
	}
}

func (r *runner) stopRun() {
	r.cancelTime = time.Now()
	if r.runStatus.ctxCancel != nil {
		r.runStatus.ctxCancel()
	}
	r.changeRunnerStatus(RunnerStatusStopping)
}


func (r *runner) canRestart() bool {
	status := r.status
	cfg := r.restartCfg
	switch cfg.Cond {
	case RestartStrategyNo:
	case RestartStrategyAlways:
		return true
	case RestartStrategyOnExitSuccess:
		if status == RunnerStatusStoppedSuccess {
			return true
		}
	case RestartStrategyOnFailed:
		if status == RunnerStatusStoppedError {
			return true
		}
	}
	return false
}


func (r *runner) run(runWait time.Duration, rc RunContext) {
	r.mx.Lock()
	close(rc.startChan)
	r.changeRunnerStatus(RunnerStatusWaitingRun)
	r.runTime = time.Now()
	r.mx.Unlock()

	if runWait != 0 {
		tm := time.NewTimer(runWait)
		select {
		case <- tm.C:
		case <- rc.Done():
			r.stopRun()
			return
		}
	}

	r.mx.Lock()
	r.changeRunnerStatus(RunnerStatusRunning)
	r.mx.Unlock()

	r.lastCode, r.lastMessage = r.runFunc(rc)

	r.mx.Lock()
	r.endTime = time.Now()
	if r.lastCode != 0 {
		r.changeRunnerStatus(RunnerStatusStoppedError)
	} else {
		r.changeRunnerStatus(RunnerStatusStoppedSuccess)
	}
	close(r.stopChan)
	r.mx.Unlock()
}


type monitor struct {
	srv *Server
}

func (m monitor) OnRunnerStatusChange(status Status) {
	m.srv.reportStatusChange(status)
}