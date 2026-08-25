package id

import (
	"time"

	"github.com/lee-ext/go-extend/ext"
)

// Parse 解析由此包生成的 ID。返回的 T3 依次包含时间、机器 ID 和序列号。
func Parse(value int64) (ext.T3[time.Time, int64, int64], error) {
	if value < 0 {
		return ext.T3[time.Time, int64, int64]{}, ErrInvalidID
	}

	sequence := value & sequenceMask
	machineID := (value >> machineIDShift) & MaxMachineID
	timestamp := time.UnixMilli(EpochMilliseconds + (value >> timestampShift))
	return ext.T3_(timestamp, machineID, sequence), nil
}
