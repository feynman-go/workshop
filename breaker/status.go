package breaker

import (
	"golang.org/x/time/rate"
	"sync"
	"time"
)

type StatusCode int

const (
	StatusCodeNormal StatusCode = iota
	StatusCodeAbnormal
	StatusCodeRestore
)

func (s StatusCode) String() string {
	switch s {
	case StatusCodeNormal:
		return "Normal"
	case StatusCodeAbnormal:
		return "Abnormal"
	case StatusCodeRestore:
		return "Restore"
	default:
		return "Unknown"
	}
}

/** 异常状态维护，
通过 Record 方法记录当前的最新一次更新状态
Status 可以处于 StatusCodeNormal StatusCodeAbnormal StatusCodeRestore 这三个状态
初始的时候 Status 处于 StatusCodeNormal， 当abnormal 的记录达到一定频率的时候 转化为 StatusCodeAbnormal 状态
Status 会停留在 StateTypeAbnormal状态一段时间后（时间可以为0），进入恢复状态。恢复状态会统计正常记录的数量和错误的频率
如果错误记录的频率达到一定值，则Status状态会转回 StatusCodeAbnormal 状态
*/
type StatusConfig struct {
	AbnormalLimit    rate.Limit
	AbnormalDuration time.Duration
	RestoreLimit     rate.Limit
	RestoreCount     int32
}

type Status struct {
	rw               sync.RWMutex
	abLimiter        *rate.Limiter // abnormal Limiter
	resetLimiter     *rate.Limiter // restore reset limiter, if restore reach this limiter. No data get
	restoreCount     int32
	maxRestoreCount  int32
	abnormalDuration time.Duration
	key              StatusCode
	abnormalEnd      time.Time
}

func NewStatus(config StatusConfig) *Status {
	return &Status{
		abLimiter:        rate.NewLimiter(config.AbnormalLimit, 1),
		resetLimiter:     rate.NewLimiter(config.RestoreLimit, 1),
		abnormalDuration: config.AbnormalDuration,
		restoreCount:     0,
		maxRestoreCount:  config.RestoreCount,
		key:              StatusCodeNormal,
	}
}

func (st *Status) ConvertToAbnormal() {
	st.rw.Lock()
	defer st.rw.Unlock()
	st.convertToAbnormal()
}


func (st *Status) StatusCode() StatusCode {
	st.rw.RLock()
	defer st.rw.RUnlock()
	return st.getStateKey()
}

func (st *Status) getStateKey() StatusCode {
	if st.key == StatusCodeAbnormal {
		if time.Now().After(st.abnormalEnd) {
			st.convertToRestore()
		}
	}
	return st.key
}

func (st *Status) Record(abnormal bool) {
	st.rw.Lock()
	defer st.rw.Unlock()

	switch st.key {
	case StatusCodeNormal:
		if abnormal {
			if !st.abLimiter.Allow() {
				st.convertToAbnormal()
			}
		}
	case StatusCodeAbnormal:
		st.getStateKey()
	case StatusCodeRestore:
		if !abnormal {
			st.restoreCount++
			if st.restoreCount == st.maxRestoreCount {
				st.convertToNormal()
			}
		} else {
			if !st.resetLimiter.Allow() {
				st.convertToAbnormal()
			}
		}
	}
}

func (st *Status) reset() {
	st.key = StatusCodeNormal
	st.abnormalEnd = time.Time{}
	st.resetLimiter.SetLimit(st.resetLimiter.Limit())
	st.abLimiter.SetLimit(st.abLimiter.Limit())
}

func (st *Status) convertToAbnormal() {
	st.reset()
	st.key = StatusCodeAbnormal
	st.abnormalEnd = time.Now().Add(st.abnormalDuration)
}

func (st *Status) convertToRestore() {
	st.reset()
	st.key = StatusCodeRestore
	st.restoreCount = 0
}

func (st *Status) convertToNormal() {
	st.reset()
	st.key = StatusCodeNormal
}

