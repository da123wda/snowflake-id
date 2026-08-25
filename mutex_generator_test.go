package id

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

func BenchmarkMutexNextCachedHotPath(b *testing.B) {
	generator := &MutexGenerator{}
	generator.current.Store(newLease(0, 0, 0, math.MaxInt64-1))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := generator.Next(); err != nil {
			b.Fatal(err)
		}
	}
}

// Sustained benchmarks include the 4096 IDs/ms format limit.
func BenchmarkMutexNextSustained(b *testing.B) {
	generator := mustNewMutex(1)
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

const shortBenchmarkBatchSize = 1000

// ShortBatch benchmarks reset state before the format limit can dominate the result.
// Generator.Next returns a standard value/error tuple.
func BenchmarkMutexNextShortBatch(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		generator := mustNewMutex(1)
		for range shortBenchmarkBatchSize {
			if _, err := generator.Next(); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*shortBenchmarkBatchSize), "ns/id")
}

func TestNewMutexMachineIDBounds(t *testing.T) {
	for _, machineID := range []int64{0, MaxMachineID} {
		if _, err := NewMutex(machineID); err != nil {
			t.Fatalf("NewMutex(%d) returned error: %v", machineID, err)
		}
	}

	for _, machineID := range []int64{-1, MaxMachineID + 1} {
		if _, err := NewMutex(machineID); !errors.Is(err, ErrInvalidMachineID) {
			t.Fatalf("NewMutex(%d) error = %v, want ErrInvalidMachineID", machineID, err)
		}
	}
}

func TestMutexNextIsConcurrentAndUnique(t *testing.T) {
	generator := mustNewMutex(1)

	const count = 10_000
	ids := make(chan int64, count)
	var wg sync.WaitGroup
	wg.Add(count)
	for range count {
		go func() {
			defer wg.Done()
			value, err := generator.Next()
			if err != nil {
				t.Errorf("Next() error: %v", err)
				return
			}
			ids <- value
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[int64]struct{}, count)
	for value := range ids {
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate ID: %d", value)
		}
		seen[value] = struct{}{}

		_, machineID, _, err := parseValue(value)
		if err != nil {
			t.Fatal(err)
		}
		if machineID != 1 {
			t.Fatalf("Parse(%d) machine ID = %d, want 1", value, machineID)
		}
	}
	if len(seen) != count {
		t.Fatalf("generated %d unique IDs, want %d", len(seen), count)
	}
}

func TestMutexNextRenewsAfterDefaultLease(t *testing.T) {
	generator := mustNewMutex(2)
	seen := make(map[int64]struct{}, defaultLeaseSize+1)
	for range defaultLeaseSize + 1 {
		value, err := generator.Next()
		if err != nil {
			t.Fatal(err)
		}
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate ID after renewal: %d", value)
		}
		seen[value] = struct{}{}
	}
}

func TestMutexConcurrentExhaustionReplacesLeaseOnce(t *testing.T) {
	generator := mustNewMutex(3)
	exhausted := newLease(0, 3, 0, -1)
	generator.current.Store(exhausted)

	ids := make(chan int64, defaultLeaseSize)
	var wg sync.WaitGroup
	wg.Add(defaultLeaseSize)
	for range defaultLeaseSize {
		go func() {
			defer wg.Done()
			value, err := generator.Next()
			if err != nil {
				t.Errorf("Next() error: %v", err)
				return
			}
			ids <- value
		}()
	}
	wg.Wait()
	close(ids)

	if generator.current.Load() == exhausted {
		t.Fatal("exhausted lease was not replaced")
	}
	if generator.state.sequence != defaultLeaseSize-1 {
		t.Fatalf("reserved sequence = %d, want exactly one lease ending at %d", generator.state.sequence, defaultLeaseSize-1)
	}
	seen := make(map[int64]struct{}, defaultLeaseSize)
	for value := range ids {
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate ID: %d", value)
		}
		seen[value] = struct{}{}
	}
	if len(seen) != defaultLeaseSize {
		t.Fatalf("unique IDs = %d, want %d", len(seen), defaultLeaseSize)
	}
}

func TestMutexFailedReplacementCanBeRetried(t *testing.T) {
	generator := mustNewMutexGenerator(4, MaxTimestampMilliseconds)
	exhausted := newLease(maxTimestamp, 4, 0, -1)
	generator.current.Store(exhausted)

	if _, err := generator.Next(); !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("Next() error = %v, want ErrInvalidTimestamp", err)
	}
	if generator.current.Load() != exhausted {
		t.Fatal("failed replacement changed current lease")
	}
	if !generator.mu.TryLock() {
		t.Fatal("failed replacement left mutex locked")
	}
	generator.state.lastTimestamp = time.Now().UnixMilli() - EpochMilliseconds
	generator.state.sequence = -1
	generator.mu.Unlock()

	if _, err := generator.Next(); err != nil {
		t.Fatalf("Next() retry error: %v", err)
	}
	if generator.current.Load() == exhausted {
		t.Fatal("successful retry did not replace current lease")
	}
}
