package mutex

const (
	TimestampBits                       = 41
	MachineIDBits                       = 10
	BusinessIDBits                      = 0
	SequenceBits                        = 12
	MaxMachineID                  int64 = (1 << MachineIDBits) - 1
	MaxBusinessID                 int64 = 0
	IDsPerMillisecond                   = 1 << sequenceBits
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

func newBitLayout(customTimestampBits, customMachineIDBits, customBusinessBits, customSequenceBits uint8) (bitLayout, error) {
	if customTimestampBits == 0 || customSequenceBits < 6 ||
		int(customTimestampBits)+int(customMachineIDBits)+int(customBusinessBits)+int(customSequenceBits) != 63 {
		return bitLayout{}, ErrInvalidBitLayout
	}
	return bitLayout{
		machineIDShift: customSequenceBits + customBusinessBits,
		businessShift:  customSequenceBits,
		timestampShift: customSequenceBits + customBusinessBits + customMachineIDBits,
		machineIDMask:  mask(customMachineIDBits),
		businessMask:   mask(customBusinessBits),
		sequenceMask:   mask(customSequenceBits),
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
