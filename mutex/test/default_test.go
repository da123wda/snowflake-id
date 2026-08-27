package test

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	mutexid "github.com/da123wda/snowflake-id/v2/mutex"
)

func TestMutexDefaultLayoutAndParse(t *testing.T) {
	generator, err := mutexid.NewMutex(42, customTestEpoch)
	if err != nil {
		t.Fatal(err)
	}
	value, err := generator.Next()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := mutexid.Parse(value, customTestEpoch)
	if err != nil || parsed.V1 != 42 || parsed.V2 < 0 || parsed.V2 >= mutexid.IDsPerMillisecond {
		t.Fatalf("Parse(%d) = %v, error %v", value, parsed, err)
	}
	if mutexid.TimestampBits != 41 || mutexid.MachineIDBits != 10 || mutexid.BusinessIDBits != 0 || mutexid.SequenceBits != 12 {
		t.Fatal("unexpected default layout constants")
	}
}

func TestMutexValidatesMachineIDAndEpoch(t *testing.T) {
	for _, machineID := range []int64{-1, mutexid.MaxMachineID + 1} {
		if _, err := mutexid.NewMutex(machineID, customTestEpoch); !errors.Is(err, mutexid.ErrInvalidMachineID) {
			t.Fatalf("NewMutex(%d) error = %v, want ErrInvalidMachineID", machineID, err)
		}
	}
	now := time.Now().UnixMilli()
	for _, epoch := range []time.Time{
		time.UnixMilli(now + int64(time.Hour/time.Millisecond)),
		time.UnixMilli(now - mutexid.MaxTimestampDeltaMilliseconds - 1),
	} {
		if _, err := mutexid.NewMutex(1, epoch); !errors.Is(err, mutexid.ErrInvalidTimestamp) {
			t.Fatalf("NewMutex(epoch=%v) error = %v, want ErrInvalidTimestamp", epoch, err)
		}
	}
}

func TestMutexParseValidation(t *testing.T) {
	if _, err := mutexid.Parse(-1, customTestEpoch); !errors.Is(err, mutexid.ErrInvalidID) {
		t.Fatalf("Parse(-1) error = %v, want ErrInvalidID", err)
	}
	if _, err := mutexid.Parse(math.MaxInt64, time.UnixMilli(math.MaxInt64)); !errors.Is(err, mutexid.ErrInvalidTimestamp) {
		t.Fatalf("Parse overflow error = %v, want ErrInvalidTimestamp", err)
	}
}

func TestMutexNextIsConcurrentAndUnique(t *testing.T) {
	generator, err := mutexid.NewMutex(2, customTestEpoch)
	if err != nil {
		t.Fatal(err)
	}
	const count = 10_000
	values := make(chan int64, count)
	var wait sync.WaitGroup
	wait.Add(count)
	for range count {
		go func() {
			defer wait.Done()
			value, err := generator.Next()
			if err != nil {
				t.Errorf("Next: %v", err)
				return
			}
			values <- value
		}()
	}
	wait.Wait()
	close(values)
	seen := make(map[int64]struct{}, count)
	for value := range values {
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate ID: %d", value)
		}
		seen[value] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("unique IDs = %d, want %d", len(seen), count)
	}
}

func TestMutexNextAndBatchDoNotOverlap(t *testing.T) {
	generator, err := mutexid.NewMutex(3, customTestEpoch)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[int64]struct{}, 128)
	first, err := generator.Next()
	if err != nil {
		t.Fatal(err)
	}
	seen[first] = struct{}{}
	batch, err := generator.NextBatch()
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 64 {
		t.Fatalf("batch size = %d, want 64", len(batch))
	}
	for index, value := range batch {
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate ID: %d", value)
		}
		seen[value] = struct{}{}
		if index > 0 && value != batch[index-1]+1 {
			t.Fatalf("batch is not continuous at index %d", index)
		}
	}
	for range 63 {
		value, err := generator.Next()
		if err != nil {
			t.Fatal(err)
		}
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate ID: %d", value)
		}
		seen[value] = struct{}{}
	}
}

func TestMutexMeets50000IDsPerSecondTarget(t *testing.T) {
	generator, err := mutexid.NewMutex(4, customTestEpoch)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Now()
	for range 50_000 {
		if _, err := generator.Next(); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("generated 50000 IDs in %v, want <= 1s", elapsed)
	}
}

func BenchmarkMutexNextBatch4096PerMillisecond(b *testing.B) {
	generator, err := mutexid.NewMutex(1, customTestEpoch)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := generator.NextBatch(); err != nil {
				b.Errorf("NextBatch() error: %v", err)
				return
			}
		}
	})
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*64), "ns/id")
}
