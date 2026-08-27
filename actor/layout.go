package actor

const (
	// TimestampBits 是默认布局的时间戳位数。
	TimestampBits = 41

	// MachineIDBits 是机器 ID 占用的位数。
	MachineIDBits  = 10
	BusinessIDBits = 0

	// SequenceBits 是 Actor 固定使用的序列号位数。
	SequenceBits = 12

	// MaxMachineID 是有效机器 ID 的最大值。
	MaxMachineID  int64 = (1 << MachineIDBits) - 1
	MaxBusinessID int64 = 0

	// IDsPerMillisecond 是单个生成器每毫秒最多可生成的 ID 数量。
	IDsPerMillisecond = 1 << sequenceBits

	// MaxTimestampDeltaMilliseconds 是自定义纪元之后可编码的最大毫秒差。
	MaxTimestampDeltaMilliseconds int64 = (1 << timestampBits) - 1
)

type bitLayout struct {
	machineIDShift uint8
	businessShift  uint8
	timestampShift uint8
	machineIDMask  int64
	businessMask   int64
	sequenceMask   int64
	maxTimestamp   int64
}

const (
	timestampBits = TimestampBits
	sequenceBits  = SequenceBits

	sequenceMask   int64 = IDsPerMillisecond - 1
	maxTimestamp   int64 = MaxTimestampDeltaMilliseconds
	machineIDShift       = sequenceBits
	timestampShift       = MachineIDBits + sequenceBits
)

var defaultBitLayout = bitLayout{
	machineIDShift: sequenceBits,
	businessShift:  sequenceBits,
	timestampShift: timestampShift,
	machineIDMask:  MaxMachineID,
	businessMask:   MaxBusinessID,
	sequenceMask:   sequenceMask,
	maxTimestamp:   maxTimestamp,
}

func newBitLayout(customTimestampBits, customMachineIDBits, customBusinessBits uint8) (bitLayout, error) {
	if customTimestampBits == 0 || int(customTimestampBits)+int(customMachineIDBits)+int(customBusinessBits) != 63-sequenceBits {
		return bitLayout{}, ErrInvalidBitLayout
	}
	return bitLayout{
		machineIDShift: sequenceBits + customBusinessBits,
		businessShift:  sequenceBits,
		timestampShift: sequenceBits + customBusinessBits + customMachineIDBits,
		machineIDMask:  mask(customMachineIDBits),
		businessMask:   mask(customBusinessBits),
		sequenceMask:   sequenceMask,
		maxTimestamp:   mask(customTimestampBits),
	}, nil
}

func mask(bits uint8) int64 {
	if bits == 0 {
		return 0
	}
	return int64((uint64(1) << bits) - 1)
}

func (layout bitLayout) prefix(timestamp, machineID, businessID int64) int64 {
	return (timestamp << layout.timestampShift) |
		(machineID << layout.machineIDShift) |
		(businessID << layout.businessShift)
}
