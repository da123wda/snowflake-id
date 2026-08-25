package id

import "errors"

var (
	// ErrInvalidMachineID 表示机器 ID 超出 [0, MaxMachineID] 范围。
	ErrInvalidMachineID = errors.New("id: machine ID must be between 0 and 1023")

	// ErrInvalidTimestamp 表示时间戳发生回拨或超出支持范围。
	ErrInvalidTimestamp = errors.New("id: invalid timestamp")

	// ErrInvalidID 表示无法解析给定的 ID。
	ErrInvalidID = errors.New("id: invalid ID")

	errInvalidSegmentSize = errors.New("id: segment size must be between 1 and 4096")
)
