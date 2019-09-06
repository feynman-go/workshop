package leader

import (
	"context"
	"github.com/feynman-go/workshop/parallel"
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

	waitFollowerChan chan struct{}
	waitLeaderChan   chan struct{}
}

func NewMember(elector Elector, electFactory ElectionFactory, option Option) *Member {
	var mb = &Member{
		elector: elector,
		electFactory: electFactory,
		option: option,
		waitLeaderChan: make(chan struct{}),
	}

	mb.tasks = task.NewManager(
		task.NewMemoScheduler(5 * time.Second),
		task.FuncExecutor(func(cb task.Context) error {
			res := mb.process(cb)
			return cb.Callback(cb, res)
		}),
		time.Second,
	)
	return mb
}

// block & run no leader
func (mb *Member) WaitLeader(ctx context.Context, do func(ctx context.Context)) {
	waitLeader, _ := mb.getWaitChan()
	for {
		select {
		case <- ctx.Done():
			return
		case <- waitLeader:
			if !mb.DoAsLeader(ctx, do) {
				continue
			}
		}
	}
}

func (mb *Member) DoAsLeader(ctx context.Context, do func(ctx context.Context)) bool {
	_, waitFollower := mb.getWaitChan()
	if waitFollower == nil {
		return false
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <- runCtx.Done():
		case <- waitFollower:
			cancel()
		}
	}()
	do(runCtx)
	return true
}

func (mb *Member) getWaitChan() (waitLeader <- chan struct{}, waitFollower<- chan struct{}) {
	mb.rw.RLock()
	defer mb.rw.RUnlock()

	return mb.waitLeaderChan, mb.waitFollowerChan
}

func (mb *Member) FetchInfo() MemberInfo {
	mb.rw.RLock()
	defer mb.rw.RUnlock()
	return mb.info
}

func (mb *Member) prepareElection(ctx context.Context) (Election, error) {
	mb.rw.Lock()
	defer mb.rw.Unlock()

	mInfo := mb.FetchInfo()
	elect, err := mb.electFactory.FetchElection(ctx, mInfo.Sequence)
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
	err = mb.elector.PostElection(ctx, elect)
	if err != nil {
		return err
	}
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

func (mb *Member) process(ctx context.Context) task.ExecResult {
	var delta time.Duration
	if mb.info.IsLeader {
		mb.startKeepLive(ctx)
		delta = mb.getElectionDuration()
	} else {
		mb.startElect(ctx)
		delta = mb.getElectionDuration()
	}
	return task.ExecResult{
		NextExec: task.ExecConfig {
			MaxExecDuration: mb.getExecDuration(),
			RemainExecCount: -1,
			ExpectStartTime: time.Now().Add(delta),
		},
	}
}

func (mb *Member) getKeepLiveDuration() time.Duration {
	delta := rand.Int63n(int64(mb.option.MaxKeepLiveDuration - mb.option.MinKeepLiveDuration))
	return mb.option.MinKeepLiveDuration + time.Duration(delta)
}

func (mb *Member) getElectionDuration() time.Duration {
	delta := rand.Int63n(int64(mb.option.MaxElectDuration - mb.option.MinElectDuration))
	return mb.option.MinElectDuration + time.Duration(delta)
}

func (mb *Member) getExecDuration() time.Duration {
	delta := rand.Int63n(int64(mb.option.MaxExecDuration - mb.option.MinExecDuration))
	return mb.option.MinExecDuration + time.Duration(delta)
}



func (mb *Member) handleElectionNotify(ctx context.Context, election Election) {
	if mb.info.Sequence > election.Sequence {
		return
	}
	if mb.info.Sequence == election.Sequence && mb.info.LeaderID == election.ElectID {
		mb.info.IsLeader = true
		if mb.waitFollowerChan == nil {
			mb.waitFollowerChan = make(chan struct{})
		}
		if mb.waitLeaderChan != nil {
			close(mb.waitLeaderChan)
			mb.waitLeaderChan = nil
		}
	} else {
		mb.info.IsLeader = false
		if mb.waitFollowerChan != nil {
			close(mb.waitFollowerChan)
			mb.waitFollowerChan = nil
		}
		if mb.waitLeaderChan == nil {
			mb.waitLeaderChan = make(chan struct{})
		}
	}
	mb.info.Sequence = election.Sequence
	mb.info.LeaderID = election.ElectID

	var expectTime = time.Now().Add(mb.getElectionDuration())
	if mb.info.IsLeader {
		expectTime = time.Now().Add(mb.getKeepLiveDuration())
	}

	err := mb.tasks.ApplyNewTask(ctx, task.Desc{
		TaskKey: "process",
		ExecDesc: task.ExecConfig{
			ExpectStartTime: expectTime,
			RemainExecCount: 10,
		},
		Overlap: true,
	})

	if err != nil {
		log.Println("apply new task err")
	}
}

func (mb *Member) Run(ctx context.Context) error {
	var err error
	parallel.RunParallel(ctx, func(ctx context.Context) {
		mb.tasks.Run(ctx)
	}, func(ctx context.Context) {
		err = mb.tasks.ApplyNewTask(ctx, task.Desc{
			Overlap: false,
			TaskKey: "process",
			ExecDesc: task.ExecConfig{
				ExpectStartTime: time.Now().Add(mb.getElectionDuration()),
				MaxExecDuration: mb.getExecDuration(),
				RemainExecCount: -1,
			},
		})

		if err != nil {
			return
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
		return
	})
	return err
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
	FetchElection(ctx context.Context, sequence int64) (Election, error)
}