package mutex

const (
	MachineIDBits                       = 10
	MaxMachineID                  int64 = (1 << MachineIDBits) - 1
	IDsPerMillisecond                   = 1 << sequenceBits
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
