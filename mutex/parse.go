package mutex

import (
	"math"
	"time"

	"github.com/lee-ext/go-extend/ext"
)

// Parse 使用生成 ID 时的自定义纪元解析时间戳、机器 ID 和序列号。
func Parse(value int64, epoch time.Time) (ext.T3[time.Time, int64, int64], error) {
	parsed, err := parseWithLayout(value, defaultBitLayout, epoch)
	if err != nil {
		return ext.T3[time.Time, int64, int64]{}, err
	}
	return ext.T3_(parsed.V0, parsed.V1, parsed.V3), nil
}

// Parse 使用生成器自身的位布局和纪元解析时间、机器、业务和序列号。
func (g *MutexGenerator) Parse(value int64) (ext.T4[time.Time, int64, int64, int64], error) {
	return parseWithLayout(value, g.state.layout, time.UnixMilli(g.state.epochMilliseconds))
}

func parseWithLayout(value int64, layout bitLayout, epoch time.Time) (ext.T4[time.Time, int64, int64, int64], error) {
	if value < 0 {
		return ext.T4[time.Time, int64, int64, int64]{}, ErrInvalidID
	}
	delta := value >> layout.timestampShift
	epochMilliseconds := epoch.UnixMilli()
	if epochMilliseconds > math.MaxInt64-delta {
		return ext.T4[time.Time, int64, int64, int64]{}, ErrInvalidTimestamp
	}
	timestamp := delta + epochMilliseconds
	machineID := (value >> layout.machineIDShift) & layout.machineIDMask
	businessID := (value >> layout.businessShift) & layout.businessMask
	sequence := value & layout.sequenceMask
	return ext.T4_(time.UnixMilli(timestamp), machineID, businessID, sequence), nil
}
