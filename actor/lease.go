package actor

import "sync/atomic"

const (
	defaultLeaseSize   = 64
	leaseQueueSize     = IDsPerMillisecond / defaultLeaseSize
	leaseSwitchRetries = 10
)

// lease 是绑定到单一毫秒时间戳的内部连续 ID 号段。
type lease struct {
	generation uint64
	err        error
	prefix     int64
	next       atomic.Int64
	end        int64
}

func newLease(timestamp, machineID, start, end int64) *lease {
	return newLeaseWithPrefix(defaultBitLayout.prefix(timestamp, machineID, 0), start, end)
}

func newLeaseWithPrefix(prefix, start, end int64) *lease {
	segment := &lease{
		prefix: prefix,
		end:    end,
	}
	segment.next.Store(start)
	return segment
}

func (l *lease) take() (int64, bool) {
	for {
		sequence := l.next.Load()
		if sequence > l.end {
			return 0, false
		}
		if l.next.CompareAndSwap(sequence, sequence+1) {
			return l.prefix | sequence, true
		}
	}
}
