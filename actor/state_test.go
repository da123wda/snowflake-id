package actor

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestStateLayoutAndOrder(t *testing.T) {
	milliseconds := []int64{testEpochMilliseconds + 12, testEpochMilliseconds + 12, testEpochMilliseconds + 12, testEpochMilliseconds + 13}
	index := 0
	generator := mustNewActorGenerator(513, milliseconds[index])
	index++

	first, err := leaseIDAtValue(generator, milliseconds[index])
	if err != nil {
		t.Fatal(err)
	}
	index++
	second, err := leaseIDAtValue(generator, milliseconds[index])
	if err != nil {
		t.Fatal(err)
	}
	index++
	third, err := leaseIDAtValue(generator, milliseconds[index])
	if err != nil {
		t.Fatal(err)
	}
	if !(first < second && second < third) {
		t.Fatalf("IDs are not increasing: %d, %d, %d", first, second, third)
	}

	for _, test := range []struct {
		value    int64
		millis   int64
		sequence int64
	}{
		{first, testEpochMilliseconds + 12, 0},
		{second, testEpochMilliseconds + 12, 1},
		{third, testEpochMilliseconds + 13, 0},
	} {
		timestamp, machineID, sequence, err := parseValue(test.value)
		if err != nil {
			t.Fatal(err)
		}
		if timestamp.UnixMilli() != test.millis || machineID != 513 || sequence != test.sequence {
			t.Fatalf("Parse(%d) = (%d, %d, %d), want (%d, 513, %d)", test.value, timestamp.UnixMilli(), machineID, sequence, test.millis, test.sequence)
		}
	}
}

func TestStateWaitsWhenSequenceIsExhausted(t *testing.T) {
	initialMilliseconds := time.Now().UnixMilli()
	generator := mustNewActorGenerator(1, initialMilliseconds)

	var last int64
	var err error
	for range IDsPerMillisecond + 1 {
		last, err = leaseIDAtValue(generator, initialMilliseconds)
		if err != nil {
			t.Fatal(err)
		}
	}
	timestamp, _, sequence, err := parseValue(last)
	if err != nil {
		t.Fatal(err)
	}
	if sequence != 0 {
		t.Fatalf("sequence after exhaustion = %d, want 0", sequence)
	}
	if timestamp.UnixMilli() <= initialMilliseconds {
		t.Fatalf("timestamp after exhaustion = %d, want greater than %d", timestamp.UnixMilli(), initialMilliseconds)
	}
}

func TestStateDetectsClockRollback(t *testing.T) {
	generator := mustNewActorGenerator(1, testEpochMilliseconds+10)
	if _, err := leaseIDAtValue(generator, testEpochMilliseconds+10); err != nil {
		t.Fatal(err)
	}
	if _, err := leaseIDAtValue(generator, testEpochMilliseconds+9); !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("lease() error = %v, want ErrInvalidTimestamp", err)
	}
}

func TestStateDetectsRollbackBeforeEpoch(t *testing.T) {
	generator := mustNewActorGenerator(1, testEpochMilliseconds+10)
	if _, err := leaseIDAtValue(generator, testEpochMilliseconds+10); err != nil {
		t.Fatal(err)
	}
	if _, err := leaseIDAtValue(generator, testEpochMilliseconds-1); !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("lease() error = %v, want ErrInvalidTimestamp", err)
	}
}

func TestNewActorRejectsTimestampBeforeEpoch(t *testing.T) {
	_, err := newIDState(1, testEpochMilliseconds, testEpochMilliseconds-1)
	if !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("newIDState() error = %v, want ErrInvalidTimestamp", err)
	}
}

func TestStateSupportsMaximumTimestampWithoutSignBit(t *testing.T) {
	generator := mustNewActorGenerator(MaxMachineID, testMaxTimestampMilliseconds)

	var last int64
	var err error
	for range IDsPerMillisecond {
		last, err = leaseIDAtValue(generator, testMaxTimestampMilliseconds)
		if err != nil {
			t.Fatal(err)
		}
	}
	if last != math.MaxInt64 {
		t.Fatalf("last ID = %d, want %d", last, int64(math.MaxInt64))
	}
	if last < 0 {
		t.Fatalf("last ID = %d, want non-negative", last)
	}

	timestamp, machineID, sequence, err := parseValue(last)
	if err != nil {
		t.Fatal(err)
	}
	if timestamp.UnixMilli() != testMaxTimestampMilliseconds || machineID != MaxMachineID || sequence != sequenceMask {
		t.Fatalf("Parse(%d) = (%d, %d, %d), want (%d, %d, %d)", last, timestamp.UnixMilli(), machineID, sequence, testMaxTimestampMilliseconds, MaxMachineID, sequenceMask)
	}
}

func TestNewActorRejectsTimestampAfterMaximum(t *testing.T) {
	_, err := newIDState(1, testEpochMilliseconds, testMaxTimestampMilliseconds+1)
	if !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("newIDState() error = %v, want ErrInvalidTimestamp", err)
	}
}
