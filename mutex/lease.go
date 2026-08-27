package mutex

import "sync/atomic"

const defaultLeaseSize = 64

type lease struct {
	prefix int64
	next   atomic.Int64
	end    int64
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
