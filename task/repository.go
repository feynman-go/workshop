package task

import (
	"context"
	"time"
)

type Repository interface {
	//return ErrOccupied
	OccupyTask(ctx context.Context, unique string, sessionTimeout time.Duration) (task *Task, err error)

	ReadSessionTask(ctx context.Context, unique string, session Session) (*Task, error)

	ReleaseTaskSession(ctx context.Context, unique string, sessionID int64) error

	UpdateTask(ctx context.Context, task *Task) error

	ReadTask(ctx context.Context, unique string) (*Task, error)

	GetTaskSummary(ctx context.Context) (*Summery, error)
}
