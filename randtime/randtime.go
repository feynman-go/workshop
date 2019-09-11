package randtime

import (
	"math/rand"
	"time"
)

func RandDuration(minDuration, maxDuration time.Duration) time.Duration {
	if minDuration >= maxDuration {
		return maxDuration
	}
	return time.Duration(rand.Int63n(int64(maxDuration - minDuration)))
}