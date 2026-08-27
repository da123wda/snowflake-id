package actor

import (
	"runtime"
	"time"
)

const leaseWaitSpinWindow = 200 * time.Microsecond

// idState 保存一个生成器独占的机器 ID、时间戳和序列号状态。
type idState struct {
	epochMilliseconds int64
	machineID         int64
	layout            bitLayout
	lastTimestamp     int64
	sequence          int64
}

// newIDState 使用指定的初始 Unix 毫秒时间戳创建生成状态。
func newIDState(machineID, epochMilliseconds, initialUnixMilliseconds int64) (*idState, error) {
	return newIDStateWithLayout(defaultBitLayout, machineID, epochMilliseconds, initialUnixMilliseconds)
}

func newIDStateWithLayout(layout bitLayout, machineID, epochMilliseconds, initialUnixMilliseconds int64) (*idState, error) {
	if machineID < 0 || machineID > layout.machineIDMask {
		return nil, ErrInvalidMachineID
	}

	if epochMilliseconds > initialUnixMilliseconds || epochMilliseconds < initialUnixMilliseconds-layout.maxTimestamp {
		return nil, ErrInvalidTimestamp
	}
	initialTimestamp := initialUnixMilliseconds - epochMilliseconds

	return &idState{
		epochMilliseconds: epochMilliseconds,
		machineID:         machineID,
		layout:            layout,
		lastTimestamp:     initialTimestamp,
		sequence:          -1,
	}, nil
}

type sequenceRange struct {
	timestamp int64
	start     int64
	end       int64
}

// lease 使用指定的 Unix 毫秒时间戳预留一个连续号段。
// 调用方必须保证同一个状态实例不会被并发调用。
func (s *idState) lease(unixMilliseconds int64, size int) (*lease, error) {
	if size < 1 || int64(size)-1 > s.layout.sequenceMask {
		return nil, errInvalidSegmentSize
	}

	reserved, err := s.reserve(unixMilliseconds, int64(size))
	if err != nil {
		return nil, err
	}
	return newLeaseWithPrefix(s.layout.prefix(reserved.timestamp, s.machineID, 0), reserved.start, reserved.end), nil
}

// reserve 预留绑定到单一毫秒时间戳的连续序列号。
func (s *idState) reserve(unixMilliseconds, size int64) (sequenceRange, error) {
	timestamp := unixMilliseconds - s.epochMilliseconds
	if timestamp < s.lastTimestamp || timestamp > s.layout.maxTimestamp {
		return sequenceRange{}, ErrInvalidTimestamp
	}

	start := int64(0)
	if timestamp == s.lastTimestamp {
		start = s.sequence + 1
		if start+size-1 > s.layout.sequenceMask {
			var err error
			timestamp, err = waitUntilNextMillisecond(s.epochMilliseconds, s.lastTimestamp)
			if err != nil {
				return sequenceRange{}, err
			}
			if timestamp <= s.lastTimestamp || timestamp > s.layout.maxTimestamp {
				return sequenceRange{}, ErrInvalidTimestamp
			}
			start = 0
		}
	}

	end := start + size - 1
	s.lastTimestamp = timestamp
	s.sequence = end
	return sequenceRange{timestamp: timestamp, start: start, end: end}, nil
}

// waitUntilNextMillisecond 在距离下一毫秒较远时休眠，接近边界时让出调度权。
func waitUntilNextMillisecond(epochMilliseconds, lastTimestamp int64) (int64, error) {
	target := time.UnixMilli(epochMilliseconds + lastTimestamp + 1)
	for {
		now := time.Now()
		timestamp := now.UnixMilli() - epochMilliseconds
		if timestamp < lastTimestamp {
			return 0, ErrInvalidTimestamp
		}
		if timestamp > lastTimestamp {
			return timestamp, nil
		}
		if remaining := target.Sub(now); remaining > leaseWaitSpinWindow {
			time.Sleep(remaining - leaseWaitSpinWindow)
			continue
		}
		runtime.Gosched()
	}
}
