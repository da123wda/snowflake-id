package mutex

const (
	EpochMilliseconds        int64 = 1_735_689_600_000
	MachineIDBits                  = 10
	MaxMachineID             int64 = (1 << MachineIDBits) - 1
	IDsPerMillisecond              = 1 << sequenceBits
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
