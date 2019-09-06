package coordinate

import (
	"context"
	"github.com/pkg/errors"
	"testing"
	"time"
)

func TestCoordinateBasic(t *testing.T) {
	opts := &Operations{

	}

	opts.Stage(1).TaskExec()


	manager := NewFlowManager(opts)
	flow, loaded, err := manager.FetchFlow("a")
	if err != nil {
		t.Fatal(err)
	}

	if loaded {
		t.Fatal("not can be fetched")
	}

	flow.AddTask()

	flow.Start(context.Background())

	flow.


	ctx, _ := context.WithTimeout(context.Background(), 2 * time.Second)

	err = flow.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}


}