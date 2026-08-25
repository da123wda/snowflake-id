package actor

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestParseRejectsNegativeID(t *testing.T) {
	_, _, _, err := parseValue(-1)
	if !errors.Is(err, ErrInvalidID) {
		t.Fatalf("Parse(-1) error = %v, want ErrInvalidID", err)
	}
}

func TestParseTruncatesEpochToMilliseconds(t *testing.T) {
	epoch := testEpoch.Add(999 * time.Microsecond)
	parsed, err := Parse(0, epoch)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.UnixMilli(epoch.UnixMilli()); !parsed.V0.Equal(want) {
		t.Fatalf("Parse(0) timestamp = %v, want %v", parsed.V0, want)
	}
}

func TestParseRejectsTimestampOverflow(t *testing.T) {
	if _, err := Parse(math.MaxInt64, time.UnixMilli(math.MaxInt64)); !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("Parse overflow error = %v, want ErrInvalidTimestamp", err)
	}
}

func TestParseUsesCustomEpoch(t *testing.T) {
	timestamp, machineID, sequence, err := parseValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !timestamp.Equal(testEpoch) || machineID != 0 || sequence != 0 {
		t.Fatalf("Parse(0) = (%v, %d, %d), want custom epoch and zero fields", timestamp, machineID, sequence)
	}
}
