package id

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

func TestInternalLeaseReturnsContinuousIDs(t *testing.T) {
	const machineID = int64(513)
	milliseconds := EpochMilliseconds + 123
	generator := mustNewActorGenerator(machineID, milliseconds)
	segment, err := acquireMutexAtValue(generator, milliseconds, defaultLeaseSize)
	if err != nil {
		t.Fatal(err)
	}

	for wantSequence := range int64(defaultLeaseSize) {
		value, ok := segment.take()
		if !ok {
			t.Fatal("lease exhausted early")
		}
		timestamp, gotMachineID, sequence, err := parseValue(value)
		if err != nil {
			t.Fatal(err)
		}
		if timestamp.UnixMilli() != milliseconds || gotMachineID != machineID || sequence != wantSequence {
			t.Fatalf("Parse(%d) = (%d, %d, %d), want (%d, %d, %d)", value, timestamp.UnixMilli(), gotMachineID, sequence, milliseconds, machineID, wantSequence)
		}
	}
	if _, ok := segment.take(); ok {
		t.Fatal("take() succeeded after exhaustion")
	}
}

func TestInternalLeaseCanBeConsumedConcurrently(t *testing.T) {
	segment := newLease(1, 1, 0, IDsPerMillisecond-1)
	ids := make(chan int64, IDsPerMillisecond)
	var wg sync.WaitGroup
	wg.Add(IDsPerMillisecond)
	for range IDsPerMillisecond {
		go func() {
			defer wg.Done()
			value, ok := segment.take()
			if !ok {
				t.Error("take() exhausted early")
				return
			}
			ids <- value
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[int64]struct{}, IDsPerMillisecond)
	for value := range ids {
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate ID: %d", value)
		}
		seen[value] = struct{}{}
	}
}

func TestInternalLeaseValidatesSize(t *testing.T) {
	generator := mustNewActorGenerator(1, time.Now().UnixMilli())
	for _, size := range []int{0, -1, IDsPerMillisecond + 1} {
		if _, err := acquireMutexAtValue(generator, time.Now().UnixMilli(), size); !errors.Is(err, errInvalidSegmentSize) {
			t.Fatalf("lease(size=%d) error = %v, want errInvalidSegmentSize", size, err)
		}
	}
}

func TestInternalLeaseMovesToNextMillisecondWhenSequenceIsInsufficient(t *testing.T) {
	initialMilliseconds := time.Now().UnixMilli()
	generator := mustNewActorGenerator(1, initialMilliseconds)
	if _, err := acquireMutexAtValue(generator, initialMilliseconds, IDsPerMillisecond-10); err != nil {
		t.Fatal(err)
	}

	segment, err := acquireMutexAtValue(generator, initialMilliseconds, 11)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := segment.take()
	if !ok {
		t.Fatal("lease exhausted early")
	}
	timestamp, _, sequence, err := parseValue(value)
	if err != nil {
		t.Fatal(err)
	}
	if timestamp.UnixMilli() <= initialMilliseconds || sequence != 0 {
		t.Fatalf("first ID after rollover = (timestamp %d, sequence %d), want timestamp > %d and sequence 0", timestamp.UnixMilli(), sequence, initialMilliseconds)
	}
}

func TestInternalLeaseSupportsMaximumTimestamp(t *testing.T) {
	generator := mustNewActorGenerator(MaxMachineID, MaxTimestampMilliseconds)
	segment, err := acquireMutexAtValue(generator, MaxTimestampMilliseconds, IDsPerMillisecond)
	if err != nil {
		t.Fatal(err)
	}

	var last int64
	for range IDsPerMillisecond {
		var ok bool
		last, ok = segment.take()
		if !ok {
			t.Fatal("lease exhausted early")
		}
	}
	if last != math.MaxInt64 {
		t.Fatalf("last ID = %d, want %d", last, int64(math.MaxInt64))
	}
}
