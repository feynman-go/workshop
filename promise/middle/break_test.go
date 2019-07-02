package middle

import (
	"testing"
	"time"
)

func TestBreaker(t *testing.T) {

	t.Run("test breaker open", func(t *testing.T) {
		bk := NewBreaker(6, 6, time.Second)
		AddResult(true)
		AddResult(true)
		AddResult(true)
		AddResult(false)
		AddResult(false)
		AddResult(false)
		AddResult(false)
		AddResult(false)
		AddResult(false)
		AddResult(false)
		AddResult(false)

		if Status() != BreakerStatusOpen {
			t.Fatal("break should be opend")
		}
	})

	t.Run("test breaker init status", func(t *testing.T) {
		bk := NewBreaker(6, 6, time.Second)

		if Status() != BreakerStatusClosed {
			t.Fatal("breaker init should be closed")
		}
	})

	t.Run("test breaker close status keep", func(t *testing.T) {
		bk := NewBreaker(6, 6, time.Second)

		AddResult(false)
		AddResult(true)
		AddResult(false)
		AddResult(true)
		AddResult(false)
		AddResult(true)
		AddResult(true)
		AddResult(true)
		AddResult(true)

		if Status() != BreakerStatusClosed {
			t.Fatal("breaker init should be closed")
		}
	})

	t.Run("test breaker open -> close -> trans", func(t *testing.T) {
		bk := NewBreaker(6, 6, time.Second)

		AddResult(true)
		AddResult(true)
		AddResult(true)
		AddResult(false)
		AddResult(false)
		AddResult(false)
		AddResult(false)
		AddResult(false)
		AddResult(false)
		AddResult(false)

		if Status() != BreakerStatusOpen {
			t.Fatal("break should be opened")
		}

		time.Sleep(time.Second)

		if Status() != BreakerStatusTransition {
			t.Fatal("break should be transition")
		}

		AddResult(false)

		if Status() != BreakerStatusOpen {
			t.Fatal("break should be opened")
		}

		time.Sleep(time.Second)
		AddResult(true)

		if Status() != BreakerStatusClosed {
			t.Fatal("break should be closed")
		}
	})


}
