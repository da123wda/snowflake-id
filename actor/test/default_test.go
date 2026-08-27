package test

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	actorid "github.com/da123wda/snowflake-id/v2/actor"
	mutexid "github.com/da123wda/snowflake-id/v2/mutex"
)

func TestActorDefaultLayoutAndParse(t *testing.T) {
	generator, err := actorid.NewActor(42, customTestEpoch)
	if err != nil {
		t.Fatal(err)
	}
	value, err := nextActor(generator, 0)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := actorid.Parse(value, customTestEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.V1 != 42 || parsed.V2 < 0 || parsed.V2 >= actorid.IDsPerMillisecond {
		t.Fatalf("Parse(%d) = %v", value, parsed)
	}
	if actorid.TimestampBits != 41 || actorid.MachineIDBits != 10 || actorid.BusinessIDBits != 0 || actorid.SequenceBits != 12 {
		t.Fatal("unexpected default layout constants")
	}
}

func TestActorValidatesMachineIDAndEpoch(t *testing.T) {
	for _, machineID := range []int64{-1, actorid.MaxMachineID + 1} {
		if _, err := actorid.NewActor(machineID, customTestEpoch); !errors.Is(err, actorid.ErrInvalidMachineID) {
			t.Fatalf("NewActor(%d) error = %v, want ErrInvalidMachineID", machineID, err)
		}
	}
	now := time.Now().UnixMilli()
	for _, epoch := range []time.Time{
		time.UnixMilli(now + int64(time.Hour/time.Millisecond)),
		time.UnixMilli(now - actorid.MaxTimestampDeltaMilliseconds - 1),
	} {
		if _, err := actorid.NewActor(1, epoch); !errors.Is(err, actorid.ErrInvalidTimestamp) {
			t.Fatalf("NewActor(epoch=%v) error = %v, want ErrInvalidTimestamp", epoch, err)
		}
	}
}

func TestActorParseValidation(t *testing.T) {
	if _, err := actorid.Parse(-1, customTestEpoch); !errors.Is(err, actorid.ErrInvalidID) {
		t.Fatalf("Parse(-1) error = %v, want ErrInvalidID", err)
	}
	if _, err := actorid.Parse(math.MaxInt64, time.UnixMilli(math.MaxInt64)); !errors.Is(err, actorid.ErrInvalidTimestamp) {
		t.Fatalf("Parse overflow error = %v, want ErrInvalidTimestamp", err)
	}
	epoch := customTestEpoch.Add(999 * time.Microsecond)
	parsed, err := actorid.Parse(0, epoch)
	if err != nil || !parsed.V0.Equal(time.UnixMilli(epoch.UnixMilli())) {
		t.Fatalf("Parse(0) = %v, error %v", parsed, err)
	}
}

func TestActorNextIsConcurrentAndUnique(t *testing.T) {
	generator, err := actorid.NewActor(2, customTestEpoch)
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
			value, err := nextActor(generator, 0)
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

func TestActorNextAndBatchDoNotOverlap(t *testing.T) {
	generator, err := actorid.NewActor(3, customTestEpoch)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[int64]struct{}, 128)
	first, err := nextActor(generator, 0)
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
		value, err := nextActor(generator, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate ID: %d", value)
		}
		seen[value] = struct{}{}
	}
}

func TestActorMeets50000IDsPerSecondTarget(t *testing.T) {
	generator, err := actorid.NewActor(4, customTestEpoch)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Now()
	for range 50_000 {
		if _, err := nextActor(generator, 0); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("generated 50000 IDs in %v, want <= 1s", elapsed)
	}
}

func TestActorAndMutexPackagesAreIndependentlyUsable(t *testing.T) {
	actorGenerator, err := actorid.NewActor(10, customTestEpoch)
	if err != nil {
		t.Fatal(err)
	}
	mutexGenerator, err := mutexid.NewMutex(11, customTestEpoch)
	if err != nil {
		t.Fatal(err)
	}
	actorValue, err := nextActor(actorGenerator, 0)
	if err != nil {
		t.Fatal(err)
	}
	mutexValue, err := mutexGenerator.Next()
	if err != nil {
		t.Fatal(err)
	}
	if actorValue == mutexValue {
		t.Fatalf("independent packages returned the same ID: %d", actorValue)
	}
}

func BenchmarkActorNextBatch4096PerMillisecond(b *testing.B) {
	generator, err := actorid.NewActor(1, customTestEpoch)
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
