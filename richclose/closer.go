package richclose

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

type Closer interface {
	Close() error
}

type closerWrapper struct {
	closer Closer
}

func (c closerWrapper) CloseWithContext(ctx context.Context) error {
	return c.closer.Close()
}

func WrapCloserContext(closer Closer) WithContextCloser {
	return closerWrapper{closer: closer}
}
