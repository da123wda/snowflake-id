package test

import (
	"errors"
	"testing"
	"time"

	mutexid "github.com/da123wda/snowflake-id/v2/mutex"
)

var customTestEpoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func BenchmarkMutexNext4096PerMillisecond(b *testing.B) {
	generator, err := mutexid.NewMutex(1, customTestEpoch)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := generator.Next(); err != nil {
				b.Errorf("Next() error: %v", err)
				return
			}
		}
	})
}

func BenchmarkMutexNextWithBusinessID4096PerMillisecond(b *testing.B) {
	generator, err := mutexid.NewMutexWithBits(40, 7, 4, 12, 1, customTestEpoch)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := generator.NextWithBusinessID(3); err != nil {
				b.Errorf("NextWithBusinessID() error: %v", err)
				return
			}
		}
	})
}

func TestMutexCustomBitsAndDynamicBusinessID(t *testing.T) {
	generator, err := mutexid.NewMutexWithBits(40, 7, 4, 12, 12, customTestEpoch)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[int64]struct{}, 16)
	for businessID := int64(0); businessID < 16; businessID++ {
		value, err := generator.NextWithBusinessID(businessID)
		if err != nil {
			t.Fatal(err)
		}
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate ID: %d", value)
		}
		seen[value] = struct{}{}
		parsed, err := generator.Parse(value)
		if err != nil || parsed.V1 != 12 || parsed.V2 != businessID {
			t.Fatalf("generator.Parse(%d) = %v, error %v", value, parsed, err)
		}
	}
}

func TestMutexCustomBitsSupportSixBitSequenceAndBatch(t *testing.T) {
	generator, err := mutexid.NewMutexWithBits(40, 13, 4, 6, 1234, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	batch, err := generator.NextBatchWithBusinessID(9)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 64 {
		t.Fatalf("batch size = %d, want 64", len(batch))
	}
	for index, value := range batch {
		parsed, err := generator.Parse(value)
		if err != nil || parsed.V1 != 1234 || parsed.V2 != 9 || parsed.V3 != int64(index) {
			t.Fatalf("generator.Parse(%d) = %v, error %v", value, parsed, err)
		}
	}
}

func TestMutexCustomBitsValidation(t *testing.T) {
	for _, bits := range [][4]uint8{{40, 7, 4, 11}, {40, 7, 4, 13}, {40, 14, 4, 5}, {0, 43, 4, 16}} {
		if _, err := mutexid.NewMutexWithBits(bits[0], bits[1], bits[2], bits[3], 0, customTestEpoch); !errors.Is(err, mutexid.ErrInvalidBitLayout) {
			t.Fatalf("NewMutexWithBits(%v) error = %v, want ErrInvalidBitLayout", bits, err)
		}
	}
	if _, err := mutexid.NewMutexWithBits(40, 7, 4, 12, 128, customTestEpoch); !errors.Is(err, mutexid.ErrInvalidMachineID) {
		t.Fatalf("machine error = %v, want ErrInvalidMachineID", err)
	}
	generator, err := mutexid.NewMutexWithBits(40, 7, 4, 12, 0, customTestEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := generator.NextWithBusinessID(16); !errors.Is(err, mutexid.ErrInvalidBusinessID) {
		t.Fatalf("business error = %v, want ErrInvalidBusinessID", err)
	}
}
