package mutex

import (
	"math"
	"time"

	"github.com/lee-ext/go-extend/ext"
)

// Parse 使用生成 ID 时的自定义纪元解析时间戳、机器 ID 和序列号。
func Parse(value int64, epoch time.Time) (ext.T3[time.Time, int64, int64], error) {
	if value < 0 {
		return ext.T3[time.Time, int64, int64]{}, ErrInvalidID
	}
	delta := value >> timestampShift
	epochMilliseconds := epoch.UnixMilli()
	if epochMilliseconds > math.MaxInt64-delta {
		return ext.T3[time.Time, int64, int64]{}, ErrInvalidTimestamp
	}
	timestamp := delta + epochMilliseconds
	machineID := (value >> machineIDShift) & MaxMachineID
	sequence := value & sequenceMask
	return ext.T3_(time.UnixMilli(timestamp), machineID, sequence), nil
}
