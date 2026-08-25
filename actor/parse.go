package actor

import (
	"math"
	"time"

	"github.com/lee-ext/go-extend/ext"
)

// Parse 使用生成 ID 时的自定义纪元进行解析。返回的 T3 依次包含时间、机器 ID 和序列号。
func Parse(value int64, epoch time.Time) (ext.T3[time.Time, int64, int64], error) {
	if value < 0 {
		return ext.T3[time.Time, int64, int64]{}, ErrInvalidID
	}

	sequence := value & sequenceMask
	machineID := (value >> machineIDShift) & MaxMachineID
	delta := value >> timestampShift
	epochMilliseconds := epoch.UnixMilli()
	if epochMilliseconds > math.MaxInt64-delta {
		return ext.T3[time.Time, int64, int64]{}, ErrInvalidTimestamp
	}
	timestamp := time.UnixMilli(epochMilliseconds + delta)
	return ext.T3_(timestamp, machineID, sequence), nil
}
