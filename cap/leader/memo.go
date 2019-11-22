package leader

import (
	"context"
	"sync"
)

type MemoDecider struct {
	rw sync.RWMutex
	e Election
	electors map[*MemoElector]bool
}

func NewMemoDecider() *MemoDecider {
	return &MemoDecider{
		electors: map[*MemoElector]bool{},
	}
}



func (decider *MemoDecider) CreateElector() *MemoElector {
	e := &MemoElector{
		decider: decider,
		cn: make(chan Election, 2),
	}
	decider.rw.Lock()
	decider.electors[e] = true
	decider.rw.Unlock()
	return e
}

func (decider *MemoDecider) postElection(election Election) error {
	decider.rw.Lock()
	defer decider.rw.Unlock()

	if decider.e.Sequence > election.Sequence {
		return nil
	}

	if decider.e.Sequence < election.Sequence {
		decider.e = election
	}

	go decider.notifyElection()
	return nil
}

func (decider *MemoDecider) keepLive(live KeepLive) error {
	decider.rw.Lock()
	defer decider.rw.Unlock()

	if decider.e.Sequence > live.Sequence {
		return nil
	}

	if decider.e.Sequence < live.Sequence {
		if decider.e.ElectID != live.ElectID {
			return nil
		}

		decider.e = Election{
			Sequence: live.Sequence,
			ElectID: live.ElectID,
		}
	}

	go decider.notifyElection()
	return nil
}

func (decider *MemoDecider) notifyElection() {
	decider.rw.RLock()
	e := decider.e
	decider.rw.RUnlock()
	for elector, _ := range decider.electors {
		select {
		case elector.cn <- e:
		default:
		}
	}
}

type MemoElector struct {
	decider *MemoDecider
	rw sync.RWMutex
	cn chan Election
}

func (elector *MemoElector) PostElection(ctx context.Context, election Election) error {
	elector.rw.Lock()
	defer elector.rw.Unlock()
	return elector.decider.postElection(election)
}

func (elector *MemoElector) KeepLive(ctx context.Context, keepLive KeepLive) error {
	elector.rw.Lock()
	defer elector.rw.Unlock()
	return elector.decider.keepLive(keepLive)
}

func (elector *MemoElector) WaitElectionNotify(ctx context.Context) (Election, error) {
	select {
	case <- ctx.Done():
		return Election{}, ctx.Err()
	case e := <- elector.cn:
		return e, nil
	}
}