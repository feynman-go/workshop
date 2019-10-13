package closer

import (
	"context"
	"time"
)

type WithContextCloser interface {
	CloseWithContext(ctx context.Context) error
}

func CloseAndWait(closer WithContextCloser, duration time.Duration) error {
	c, _ := context.WithTimeout(context.Background(), duration)
	return closer.CloseWithContext(c)
}