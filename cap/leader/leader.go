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
	IsLeader bool
	ElectionID string
	Sequence int64
}

type Member struct {
	rw           sync.RWMutex
	info         MemberInfo
	elector      Elector
	electFactory ElectionFactory
	tasks        *task.Manager
	option       Option

	pb *prob.Prob
	onFollwerChan chan struct{}
	onLeaderChan  chan struct{}
}

func NewMember(elector Elector, electFactory ElectionFactory, option Option) *Member {
	var mb = &Member{
		elector:       elector,
		electFactory:  electFactory,
		option:        option,
		onFollwerChan: make(chan struct{}),
		onLeaderChan:  make(chan struct{}),
	}

	close(mb.onFollwerChan)

	mb.tasks = task.NewManager(
		task.NewMemoScheduler(5 * time.Second),
		task.FuncExecutor(func(cb task.Context) task.ExecInfo {
			res := mb.process(cb)
			return res
		}),
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
		case <-onFollower:
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

	return mb.onLeaderChan, mb.onFollwerChan
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

func (mb *Member) process(ctx context.Context) task.ExecInfo {
	var delta time.Duration
	info := mb.GetInfo()
	if info.IsLeader {
		mb.startKeepLive(ctx)
		delta = mb.getKeepLiveDuration()
		log.Println("did start keep live next:", time.Now().Add(delta))
	} else {
		mb.startElect(ctx)
		delta = mb.getElectionDuration()
		log.Println("did start elect next:", time.Now().Add(delta))
	}

	return task.ExecInfo{
		NextExec: task.ExecOption{}.
			SetExpectStartTime(time.Now().Add(delta)).
			SetMaxExecDuration(mb.getElectionDuration()).
			SetMaxRecoverCount(0),
	}
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

	if mb.info.Sequence == election.Sequence && mb.info.ElectionID == election.ElectID {
		mb.info.IsLeader = true
		log.Println("handleElectionNotify select as leader", election)

		select {
		case <- mb.onLeaderChan:
		default:
			close(mb.onLeaderChan)
			mb.onFollwerChan = make(chan struct{})
		}
	} else {
		mb.info.IsLeader = false
		select {
		case <- mb.onFollwerChan:
		default:
			close(mb.onFollwerChan)
			mb.onLeaderChan = make(chan struct{})
		}
	}
	mb.info.Sequence = election.Sequence
	mb.info.LeaderID = election.ElectID

	var expectTime = time.Now().Add(mb.getElectionDuration())
	if mb.info.IsLeader {
		expectTime = time.Now().Add(mb.getKeepLiveDuration())
	}

	err := mb.tasks.ApplyNewTask(ctx, "process",
		task.Option{}.
		SetExpectStartTime(expectTime).
			SetMaxRestartCount(0),
	)
	if err != nil {
		log.Println("apply new task err", err)
	}
}

func (mb *Member) Start() bool {
	mb.rw.Lock()
	defer mb.rw.Unlock()
	if mb.pb != nil {
		return false
	} else {
		mb.pb = prob.New(syncrun.FuncWithRandomStart(func(ctx context.Context) bool {
			expectTime := time.Now().Add(mb.getElectionDuration())
			err := mb.tasks.ApplyNewTask(
				ctx,
				"process",
				task.Option{}.
					SetExpectStartTime(expectTime).
					SetMaxExecDuration(mb.getExecDuration()).
					SetMaxRestartCount(0),
			)

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

func (mb *Member) Close(ctx context.Context) error {
	mb.rw.Lock()
	defer mb.rw.Unlock()
	if mb.pb != nil {
		mb.pb.Stop()
		defer mb.tasks.Close(ctx)
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

func (fun ElectionFactoryFunc) NewElection(ctx context.Context, sequence int64) (Election, error){
	return fun(ctx, sequence)
}