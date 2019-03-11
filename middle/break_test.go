package middle

import (
	"testing"
	"time"
)

func TestBreaker(t *testing.T) {

	t.Run("test breaker open", func(t *testing.T) {
		bk := NewBreaker(6, 6, time.Second)
		bk.AddResult(true)
		bk.AddResult(true)
		bk.AddResult(true)
		bk.AddResult(false)
		bk.AddResult(false)
		bk.AddResult(false)
		bk.AddResult(false)
		bk.AddResult(false)
		bk.AddResult(false)
		bk.AddResult(false)
		bk.AddResult(false)

		if bk.Status() != BreakerStatusOpen {
			t.Fatal("break should be opend")
		}
	})

	t.Run("test breaker init status", func(t *testing.T) {
		bk := NewBreaker(6, 6, time.Second)

		if bk.Status() != BreakerStatusClosed {
			t.Fatal("breaker init should be closed")
		}
	})

	t.Run("test breaker close status keep", func(t *testing.T) {
		bk := NewBreaker(6, 6, time.Second)

		bk.AddResult(false)
		bk.AddResult(true)
		bk.AddResult(false)
		bk.AddResult(true)
		bk.AddResult(false)
		bk.AddResult(true)
		bk.AddResult(true)
		bk.AddResult(true)
		bk.AddResult(true)

		if bk.Status() != BreakerStatusClosed {
			t.Fatal("breaker init should be closed")
		}
	})

	t.Run("test breaker open -> close -> trans", func(t *testing.T) {
		bk := NewBreaker(6, 6, time.Second)

		bk.AddResult(true)
		bk.AddResult(true)
		bk.AddResult(true)
		bk.AddResult(false)
		bk.AddResult(false)
		bk.AddResult(false)
		bk.AddResult(false)
		bk.AddResult(false)
		bk.AddResult(false)
		bk.AddResult(false)

		if bk.Status() != BreakerStatusOpen {
			t.Fatal("break should be opened")
		}

		time.Sleep(time.Second)

		if bk.Status() != BreakerStatusTransition {
			t.Fatal("break should be transition")
		}

		bk.AddResult(false)

		if bk.Status() != BreakerStatusOpen {
			t.Fatal("break should be opened")
		}

		time.Sleep(time.Second)
		bk.AddResult(true)

		if bk.Status() != BreakerStatusClosed {
			t.Fatal("break should be closed")
		}
	})


}
