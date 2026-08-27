package actor

import "errors"

var (
	// ErrInvalidBitLayout 表示自定义位数不满足 Actor 的 63 位布局约束。
	ErrInvalidBitLayout = errors.New("snowflake-id/actor: timestamp, machine and business bits must total 51")

	// ErrInvalidMachineID 表示机器 ID 超出默认或自定义机器位可编码的范围。
	ErrInvalidMachineID = errors.New("snowflake-id/actor: machine ID exceeds configured bit width")

	// ErrInvalidBusinessID 表示业务 ID 超出自定义业务位可编码的范围。
	ErrInvalidBusinessID = errors.New("snowflake-id/actor: business ID exceeds configured bit width")

	// ErrInvalidTimestamp 表示时间戳发生回拨或超出支持范围。
	ErrInvalidTimestamp = errors.New("snowflake-id/actor: invalid timestamp")

	// ErrInvalidID 表示无法解析给定的 ID。
	ErrInvalidID = errors.New("snowflake-id/actor: invalid ID")

	// ErrLeaseUnavailable 表示后台 Actor 尚未准备好下一个号段，调用方可以重试。
	ErrLeaseUnavailable = errors.New("snowflake-id/actor: lease queue is temporarily unavailable")

	errInvalidSegmentSize = errors.New("snowflake-id/actor: segment size must be between 1 and 4096")
)
