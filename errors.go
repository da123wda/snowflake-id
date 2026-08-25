package id

import "errors"

var (
	// ErrInvalidMachineID 表示机器 ID 超出 [0, MaxMachineID] 范围。
	ErrInvalidMachineID = errors.New("id: machine ID must be between 0 and 1023")

	// ErrInvalidTimestamp 表示时间戳发生回拨或超出支持范围。
	ErrInvalidTimestamp = errors.New("id: invalid timestamp")

	// ErrInvalidID 表示无法解析给定的 ID。
	ErrInvalidID = errors.New("id: invalid ID")

	// ErrLeaseUnavailable 表示后台 Actor 尚未准备好下一个号段，调用方可以重试。
	ErrLeaseUnavailable = errors.New("id: lease queue is temporarily unavailable")

	// ErrInvalidActorCapacity 表示 Actor 邮箱容量小于环形队列所需的 64。
	ErrInvalidActorCapacity = errors.New("id: actor capacity must be at least 64")

	errInvalidSegmentSize = errors.New("id: segment size must be between 1 and 4096")
)
