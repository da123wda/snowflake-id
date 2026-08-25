package mutex

import (
	"runtime"
	"time"
)

const leaseWaitSpinWindow = 200 * time.Microsecond

type idState struct {
	machineID     int64
	lastTimestamp int64
	sequence      int64
}

func newIDState(machineID, initialUnixMilliseconds int64) (*idState, error) {
	if machineID < 0 || machineID > MaxMachineID {
		return nil, ErrInvalidMachineID
	}
	initialTimestamp := initialUnixMilliseconds - EpochMilliseconds
	if initialTimestamp < 0 || initialTimestamp > maxTimestamp {
		return nil, ErrInvalidTimestamp
	}
	return &idState{machineID: machineID, lastTimestamp: initialTimestamp, sequence: -1}, nil
}

type sequenceRange struct {
	timestamp int64
	start     int64
	end       int64
}

func (s *idState) lease(unixMilliseconds int64, size int) (*lease, error) {
	if size < 1 || size > IDsPerMillisecond {
		return nil, errInvalidSegmentSize
	}
	reserved, err := s.reserve(unixMilliseconds, int64(size))
	if err != nil {
		return nil, err
	}
	return newLease(reserved.timestamp, s.machineID, reserved.start, reserved.end), nil
}

func (s *idState) reserve(unixMilliseconds, size int64) (sequenceRange, error) {
	timestamp := unixMilliseconds - EpochMilliseconds
	if timestamp < s.lastTimestamp || timestamp > maxTimestamp {
		return sequenceRange{}, ErrInvalidTimestamp
	}
	start := int64(0)
	if timestamp == s.lastTimestamp {
		start = s.sequence + 1
		if start+size-1 > sequenceMask {
			var err error
			timestamp, err = waitUntilNextMillisecond(s.lastTimestamp)
			if err != nil {
				return sequenceRange{}, err
			}
			if timestamp <= s.lastTimestamp || timestamp > maxTimestamp {
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

func waitUntilNextMillisecond(lastTimestamp int64) (int64, error) {
	target := time.UnixMilli(EpochMilliseconds + lastTimestamp + 1)
	for {
		now := time.Now()
		timestamp := now.UnixMilli() - EpochMilliseconds
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
