package leader

import (
	"context"
	"github.com/feynman-go/workshop/record"
)

type Middle interface {
	WrapElector(elector Elector) Elector
}

func NewRecorderMiddle(factory record.Factory) Middle {
	return recorderMiddle {
		factory: factory,
	}
}

type recorderMiddle struct {
	factory record.Factory
}

func (mid recorderMiddle) WrapElector(elector Elector) Elector {
	return recordWrapper {
		mid.factory, elector,
	}
}

type recordWrapper struct {
	recorders record.Factory
	elector Elector
}

func (wrapper recordWrapper) PostElection(ctx context.Context, election Election) (err error) {
	rd, ctx := wrapper.recorders.ActionRecorder(ctx, "PostElection")
	defer func() {
		rd.Commit(err)
	}()
	err = wrapper.elector.PostElection(ctx, election)
	return err
}

func (wrapper recordWrapper) KeepLive(ctx context.Context, keepLive KeepLive) (err error) {
	rd, ctx := wrapper.recorders.ActionRecorder(ctx, "SetLiveInfo")
	defer func() {
		rd.Commit(err)
	}()
	err = wrapper.elector.KeepLive(ctx, keepLive)
	return err
}

func (wrapper recordWrapper) WaitElectionNotify(ctx context.Context) (election Election, err error) {
	rd, ctx := wrapper.recorders.ActionRecorder(ctx, "WaitElectionNotify")
	defer func() {
		rd.Commit(err)
	}()
	election, err = wrapper.elector.WaitElectionNotify(ctx)
	return
}
