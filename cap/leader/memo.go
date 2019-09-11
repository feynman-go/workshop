package leader

import (
	"context"
	"sync"
)

type MemoElector struct {
	cn chan Election
	rw sync.RWMutex
	e Election
}

func NewMemoElector() *MemoElector {
	return &MemoElector{
		cn: make(chan Election, 64),
	}
}

func (elector *MemoElector) PostElection(ctx context.Context, election Election) error {
	elector.rw.Lock()
	defer elector.rw.Unlock()

	if elector.e.Sequence > election.Sequence {
		return nil
	}

	if elector.e.Sequence < election.Sequence {
		elector.e = election
	}

	go func(e Election) {
		elector.cn <- e
	}(elector.e)
	return nil
}

func (elector *MemoElector) KeepLive(ctx context.Context, keepLive KeepLive) error {
	elector.rw.Lock()
	defer elector.rw.Unlock()

	if elector.e.Sequence > keepLive.Sequence {
		return nil
	}

	if elector.e.Sequence < keepLive.Sequence {
		elector.e = Election{
			Sequence: keepLive.Sequence,
			ElectID: keepLive.ElectID,
		}
	}

	go func(e Election) {
		elector.cn <- e
	}(elector.e)
	return nil
}

func (elector *MemoElector) WaitElectionNotify(ctx context.Context) (Election, error) {
	select {
	case <- ctx.Done():
		return Election{}, ctx.Err()
	case e := <- elector.cn:
		return e, nil
	}
}