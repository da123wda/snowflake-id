package actor

const (
	// MachineIDBits 是机器 ID 占用的位数。
	MachineIDBits = 10

	// MaxMachineID 是有效机器 ID 的最大值。
	MaxMachineID int64 = (1 << MachineIDBits) - 1

	// IDsPerMillisecond 是单个生成器每毫秒最多可生成的 ID 数量。
	IDsPerMillisecond = 1 << sequenceBits

	// MaxTimestampDeltaMilliseconds 是自定义纪元之后可编码的最大毫秒差。
	MaxTimestampDeltaMilliseconds int64 = (1 << timestampBits) - 1
)

const (
	timestampBits = 41
	sequenceBits  = 12

	sequenceMask   int64 = IDsPerMillisecond - 1
	maxTimestamp   int64 = MaxTimestampDeltaMilliseconds
	machineIDShift       = sequenceBits
	timestampShift       = MachineIDBits + sequenceBits
)
