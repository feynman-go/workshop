package leader

import (
	"context"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
	"github.com/feynman-go/workshop/task"
	"log"
	"math/rand"
	"sync"
	"time"
)

type MemberInfo struct {
	LeaderID string
	ElectionID string
	Sequence int64
}

func (info MemberInfo) IsLeader() bool {
	return info.LeaderID != "" && info.LeaderID == info.ElectionID
}

type Member struct {
	rw           sync.RWMutex
	info         MemberInfo
	elector      Elector
	electFactory ElectionFactory
	tasks        *task.Manager
	option       Option
	pb             *prob.Prob
	onFollowerChan chan struct{}
	onLeaderChan   chan struct{}
	inited bool
}

func NewMember(elector Elector, electFactory ElectionFactory, option Option) *Member {
	var mb = &Member{
		elector:        elector,
		electFactory:   electFactory,
		option:         option,
		onFollowerChan: make(chan struct{}),
		onLeaderChan:   make(chan struct{}),
	}

	close(mb.onFollowerChan)

	mb.tasks = task.NewManager(
		task.NewMemoScheduler(5 * time.Second),
		task.ExecutorFunc(mb.process),
		task.DefaultManagerOption(),
	)
	return mb
}

// block and run do until leader, return if not leader
func (mb *Member) SyncLeader(ctx context.Context, do func(ctx context.Context)) {
	for {
		select {
		case <- ctx.Done():
			return
		default:
			onLeader, _ := mb.getWaitChan()
			select {
			case <- ctx.Done():
				return
			case <- onLeader:
				if mb.DoAsLeader(ctx, do) {
					return
				}
			}
		}
	}
}

func (mb *Member) DoAsLeader(ctx context.Context, do func(ctx context.Context)) bool {
	onLeader, onFollower := mb.getWaitChan()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <- runCtx.Done():
		case <- onFollower:
			cancel()
		}
	}()

	select {
	case <- runCtx.Done():
		return false
	case <- onLeader:
		do(runCtx)
		return true
	}
}

func (mb *Member) getWaitChan() (updateChan <- chan struct{}, waitFollower<- chan struct{}) {
	mb.rw.RLock()
	defer mb.rw.RUnlock()

	return mb.onLeaderChan, mb.onFollowerChan
}

func (mb *Member) GetInfo() MemberInfo {
	mb.rw.RLock()
	defer mb.rw.RUnlock()
	return mb.info
}

func (mb *Member) prepareElection(ctx context.Context) (Election, error) {
	mb.rw.Lock()
	defer mb.rw.Unlock()

	mInfo := mb.info
	elect, err := mb.electFactory.NewElection(ctx, mInfo.Sequence)
	if err != nil {
		return Election{}, err
	}

	mb.info.Sequence = elect.Sequence
	mb.info.ElectionID = elect.ElectID
	return  Election{
		Sequence: mb.info.Sequence,
		ElectID: mb.info.ElectionID,
	}, nil
}

func (mb *Member) startElect(ctx context.Context) error {
	elect, err := mb.prepareElection(ctx)
	if err != nil {
		return err
	}

	go func() {
		err = mb.elector.PostElection(ctx, elect)
		log.Println("PostElection:", elect)
		if err != nil {
			log.Println("PostElection err:", err)
		}
	}()
	return nil
}

func (mb *Member) startKeepLive(ctx context.Context) error {
	mb.rw.RLock()
	defer mb.rw.RUnlock()

	return mb.elector.KeepLive(ctx, KeepLive{
		Sequence: mb.info.Sequence,
		ElectID: mb.info.ElectionID,
	})
}

func (mb *Member) process(ctx task.Context, result *task.Result)  {
	mb.rw.Lock()
	defer mb.rw.Unlock()

	var delta time.Duration
	info := mb.info
	if info.IsLeader() {
		delta = mb.getKeepLiveDuration()
		go mb.startKeepLive(ctx)
		log.Println("did start keep live next:", time.Now().Add(delta))
	} else {
		delta = mb.getElectionDuration()
		go mb.startElect(ctx)
		log.Println("did start elect next:", time.Now().Add(delta))
	}

	result.WaitAndReDo(delta)
	result.SetMaxDuration(mb.getElectionDuration())
	result.SetMaxRecover(0)
}

func (mb *Member) getKeepLiveDuration() time.Duration {
	delta := int64(mb.option.MaxKeepLiveDuration - mb.option.MinKeepLiveDuration)
	if delta != 0 {
		delta = rand.Int63n(delta)
	}
	return mb.option.MinKeepLiveDuration + time.Duration(delta)
}

func (mb *Member) getElectionDuration() time.Duration {
	delta := int64(mb.option.MaxElectDuration - mb.option.MinElectDuration)
	if delta != 0 {
		delta = rand.Int63n(delta)
	}
	return mb.option.MinElectDuration + time.Duration(delta)
}

func (mb *Member) getExecDuration() time.Duration {
	delta := int64(mb.option.MaxExecDuration - mb.option.MinElectDuration)
	if delta != 0 {
		delta = rand.Int63n(delta)
	}
	return mb.option.MinExecDuration + time.Duration(delta)
}

func (mb *Member) handleElectionNotify(ctx context.Context, election Election) {
	mb.rw.Lock()
	defer mb.rw.Unlock()

	log.Println("handleElectionNotify", election, mb.info, mb.info.Sequence > election.Sequence)

	if mb.info.Sequence > election.Sequence {
		return
	}
	err := mb.updateElection(ctx, election)
	if err != nil {
		log.Println("update election err:", err)
	}
}

func (mb *Member) updateElection(ctx context.Context, election Election) error {
	if mb.info.Sequence == election.Sequence && mb.info.ElectionID == election.ElectID {
		if !mb.info.IsLeader() {
			log.Println("handleElectionNotify select as leader", election.ElectID, election.Sequence)

			select {
			case <- mb.onLeaderChan:
			default:
				close(mb.onLeaderChan)
				mb.onFollowerChan = make(chan struct{})
			}
		}
	} else {
		if mb.info.IsLeader() {
			select {
			case <- mb.onFollowerChan:
			default:
				close(mb.onFollowerChan)
				mb.onLeaderChan = make(chan struct{})
			}
		}
	}
	mb.info.Sequence = election.Sequence
	mb.info.LeaderID = election.ElectID
	return mb.resetTask(ctx)
}


func (mb *Member) resetTask(ctx context.Context) error {
	var expectDuration time.Duration
	if !mb.inited {
		expectDuration = mb.getElectionDuration()
		mb.inited = true
	} else {
		if mb.info.IsLeader() {
			expectDuration = mb.getKeepLiveDuration()
		} else {
			expectDuration = mb.getElectionDuration()
		}
	}

	err := mb.tasks.ApplyNewTask(ctx, "process", task.Option{}.
		SetExpectStartTime(time.Now().Add(expectDuration)).
		SetMaxExecDuration(mb.getExecDuration()).
		SetMaxRecoverCount(0))

	if err != nil {
		return err
	}
	return nil
}

func (mb *Member) Start() bool {
	mb.rw.Lock()
	defer mb.rw.Unlock()
	if mb.pb != nil {
		return false
	} else {
		mb.pb = prob.New(syncrun.FuncWithRandomStart(func(ctx context.Context) bool {
			err := mb.resetTask(ctx)
			if err != nil {
				return true
			}

			for ctx.Err() == nil {
				runCtx, _ := context.WithTimeout(ctx, time.Second)
				e, err := mb.elector.WaitElectionNotify(runCtx)
				if err != nil {
					err = nil
					continue
				}
				mb.handleElectionNotify(ctx, e)
			}
			return true
		}, syncrun.RandRestart(0, 0)))
		mb.pb.Start()
		return true
	}
}

func (mb *Member) StartElect(ctx context.Context) error {
	mb.rw.Lock()
	defer mb.rw.Unlock()
	err := mb.startElect(ctx)
	if err != nil {
		return err
	}

	return mb.resetTask(ctx)
}

func (mb *Member) ToFollower(ctx context.Context) error {
	mb.rw.Lock()
	defer mb.rw.Unlock()

	mb.info.LeaderID = ""
	mb.info.ElectionID = ""
	return mb.resetTask(ctx)
}


func (mb *Member) CloseWithContext(ctx context.Context) error {
	mb.rw.Lock()
	defer mb.rw.Unlock()
	if mb.pb != nil {
		mb.pb.Stop()
		defer mb.tasks.CloseWithContext(ctx)
		select {
		case <- mb.pb.Stopped():
		case <- ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type Option struct {
	MaxElectDuration time.Duration
	MinElectDuration time.Duration
	MaxKeepLiveDuration time.Duration
	MinKeepLiveDuration time.Duration
	MaxExecDuration time.Duration
	MinExecDuration time.Duration
}

type Election struct {
	Sequence int64
	ElectID string
}

type KeepLive struct {
	Sequence int64
	ElectID string
}

type Elector interface {
	PostElection(ctx context.Context, election Election) error
	KeepLive(ctx context.Context, keepLive KeepLive) error
	WaitElectionNotify(ctx context.Context) (Election, error)
}

type ElectionFactory interface {
	NewElection(ctx context.Context, sequence int64) (Election, error)
}

type ElectionFactoryFunc func (ctx context.Context, sequence int64) (Election, error)

func (fun ElectionFactoryFunc) NewElection(ctx context.Context, sequence int64) (Election, error) {
	return fun(ctx, sequence)
}

type ConstElectionFactory struct {
	ConstID string
}

func (factory *ConstElectionFactory) NewElection(ctx context.Context, sequence int64) (Election, error) {
	return Election {
		Sequence: sequence + 1,
		ElectID: factory.ConstID,
	}, nil
}