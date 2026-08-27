package test

import (
	"errors"
	"runtime"
	"testing"
	"time"

	actorid "github.com/da123wda/snowflake-id/v2/actor"
)

var customTestEpoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func nextActor(generator *actorid.ActorGenerator, businessID int64) (int64, error) {
	for {
		value, err := generator.NextWithBusinessID(businessID)
		if errors.Is(err, actorid.ErrLeaseUnavailable) {
			runtime.Gosched()
			continue
		}
		return value, err
	}
}

func BenchmarkActorNext4096PerMillisecond(b *testing.B) {
	generator, err := actorid.NewActor(1, customTestEpoch)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for {
				if _, err := generator.Next(); errors.Is(err, actorid.ErrLeaseUnavailable) {
					runtime.Gosched()
					continue
				} else if err != nil {
					b.Errorf("Next() error: %v", err)
					return
				}
				break
			}
		}
	})
}

func BenchmarkActorNextWithBusinessID4096PerMillisecond(b *testing.B) {
	generator, err := actorid.NewActorWithBits(40, 7, 4, 1, customTestEpoch)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := nextActor(generator, 3); err != nil {
				b.Errorf("NextWithBusinessID() error: %v", err)
				return
			}
		}
	})
}

func TestActorCustomBitsAndDynamicBusinessID(t *testing.T) {
	generator, err := actorid.NewActorWithBits(40, 7, 4, 12, customTestEpoch)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[int64]struct{}, 16)
	for businessID := int64(0); businessID < 16; businessID++ {
		value, err := nextActor(generator, businessID)
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

	batch, err := generator.NextBatchWithBusinessID(3)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range batch {
		parsed, err := generator.Parse(value)
		if err != nil || parsed.V1 != 12 || parsed.V2 != 3 {
			t.Fatalf("generator.Parse(%d) = %v, error %v", value, parsed, err)
		}
	}
}

func TestActorCustomBitsValidation(t *testing.T) {
	for _, bits := range [][3]uint8{{40, 7, 3}, {40, 7, 5}, {0, 47, 4}} {
		if _, err := actorid.NewActorWithBits(bits[0], bits[1], bits[2], 0, customTestEpoch); !errors.Is(err, actorid.ErrInvalidBitLayout) {
			t.Fatalf("NewActorWithBits(%v) error = %v, want ErrInvalidBitLayout", bits, err)
		}
	}
	if _, err := actorid.NewActorWithBits(40, 7, 4, 128, customTestEpoch); !errors.Is(err, actorid.ErrInvalidMachineID) {
		t.Fatalf("machine error = %v, want ErrInvalidMachineID", err)
	}
	generator, err := actorid.NewActorWithBits(40, 7, 4, 0, customTestEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := generator.NextWithBusinessID(16); !errors.Is(err, actorid.ErrInvalidBusinessID) {
		t.Fatalf("business error = %v, want ErrInvalidBusinessID", err)
	}
}
