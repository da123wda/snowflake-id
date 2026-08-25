package actor

import (
	"errors"
	"testing"
	"time"
)

func TestParseRejectsNegativeID(t *testing.T) {
	_, _, _, err := parseValue(-1)
	if !errors.Is(err, ErrInvalidID) {
		t.Fatalf("Parse(-1) error = %v, want ErrInvalidID", err)
	}
}

func TestParseUsesCustomEpoch(t *testing.T) {
	timestamp, machineID, sequence, err := parseValue(0)
	if err != nil {
		t.Fatal(err)
	}
	if !timestamp.Equal(time.UnixMilli(EpochMilliseconds)) || machineID != 0 || sequence != 0 {
		t.Fatalf("Parse(0) = (%v, %d, %d), want custom epoch and zero fields", timestamp, machineID, sequence)
	}
}
