package balance

import (
	"context"
)

type MemberScheduler interface {
	UpdateMemberInfo(ctx context.Context, info MemberInfo) error
	WaitMemberSchedules(ctx context.Context) ([]MemberSchedule, error)
}

type MemberSchedule struct {
	MemberID int64
	AcceptKey []KeyRange
	RemoveKey []KeyRange
}

type MemberInfo struct {
	MemberID int64
	AvailableScore int64
	KeySet KeySet
	InTrans bool
}

type KeyRange struct {
	Start int64
	End int64
}

type KeySet struct {
	Ranges []KeyRange
}