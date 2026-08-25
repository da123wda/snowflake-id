package mutex

import (
	"time"

	"github.com/lee-ext/go-extend/ext"
)

// Parse 解析 ID 的时间戳、机器 ID 和序列号。
func Parse(value int64) (ext.T3[time.Time, int64, int64], error) {
	if value < 0 {
		return ext.T3[time.Time, int64, int64]{}, ErrInvalidID
	}
	timestamp := (value >> timestampShift) + EpochMilliseconds
	machineID := (value >> machineIDShift) & MaxMachineID
	sequence := value & sequenceMask
	return ext.T3_(time.UnixMilli(timestamp), machineID, sequence), nil
}
