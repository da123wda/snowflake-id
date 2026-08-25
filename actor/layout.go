package actor

const (
	// EpochMilliseconds 是自定义纪元：2025-01-01T00:00:00Z。
	EpochMilliseconds int64 = 1_735_689_600_000

	// MachineIDBits 是机器 ID 占用的位数。
	MachineIDBits = 10

	// MaxMachineID 是有效机器 ID 的最大值。
	MaxMachineID int64 = (1 << MachineIDBits) - 1

	// IDsPerMillisecond 是单个生成器每毫秒最多可生成的 ID 数量。
	IDsPerMillisecond = 1 << sequenceBits

	// MaxTimestampMilliseconds 是支持的最晚时间戳：2094-09-07T15:47:35.551Z。
	MaxTimestampMilliseconds int64 = EpochMilliseconds + (1 << timestampBits) - 1
)

const (
	timestampBits = 41
	sequenceBits  = 12

	sequenceMask   int64 = IDsPerMillisecond - 1
	maxTimestamp   int64 = MaxTimestampMilliseconds - EpochMilliseconds
	machineIDShift       = sequenceBits
	timestampShift       = MachineIDBits + sequenceBits
)
