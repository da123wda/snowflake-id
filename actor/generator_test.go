package actor

import (
	"errors"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"
)

func newBufferedBenchmarkGenerator() *ActorGenerator {
	generator := &ActorGenerator{}
	for generation := range uint64(leaseQueueSize) {
		start := int64(generation) * defaultLeaseSize
		segment := newLease(0, 1, start, start+defaultLeaseSize-1)
		segment.generation = generation
		generator.queue.slots[generation].value.Store(segment)
		generator.queue.slots[generation].refilling.Store(true)
	}
	return generator
}

func BenchmarkActorNextCachedHotPath(b *testing.B) {
	generator := &ActorGenerator{}
	segment := newLease(0, 0, 0, math.MaxInt64-1)
	generator.queue.slots[0].value.Store(segment)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := generator.Next(); err != nil {
			b.Fatal(err)
		}
	}
}

// Sustained benchmarks include the 4096 IDs/ms format limit.
func BenchmarkActorNextSustained(b *testing.B) {
	generator := mustNewActor(b, 1)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for {
				if _, err := generator.Next(); errors.Is(err, ErrLeaseUnavailable) {
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

const shortBenchmarkBatchSize = 1000

// ShortBatch consumes prefilled ring slots before the format limit can dominate.
func BenchmarkActorNextShortBatch(b *testing.B) {
	generator := newBufferedBenchmarkGenerator()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		generator.active.Store(0)
		for generation := range uint64(leaseQueueSize) {
			generator.queue.slots[generation].value.Load().next.Store(int64(generation) * defaultLeaseSize)
		}
		b.StartTimer()
		for range shortBenchmarkBatchSize {
			if _, err := generator.Next(); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*shortBenchmarkBatchSize), "ns/id")
}

func TestNewActorValidatesMachineIDAndEpoch(t *testing.T) {
	for _, machineID := range []int64{0, MaxMachineID} {
		generator, err := NewActor(machineID, testEpoch)
		if err != nil {
			t.Fatalf("NewActor(%d) returned error: %v", machineID, err)
		}
		generator.refillActor.Close()
	}

	for _, machineID := range []int64{-1, MaxMachineID + 1} {
		if _, err := NewActor(machineID, testEpoch); !errors.Is(err, ErrInvalidMachineID) {
			t.Fatalf("NewActor(%d) error = %v, want ErrInvalidMachineID", machineID, err)
		}
	}

	nowMilliseconds := time.Now().UnixMilli()
	for _, epoch := range []time.Time{
		time.UnixMilli(nowMilliseconds + int64(time.Hour/time.Millisecond)),
		time.UnixMilli(nowMilliseconds - maxTimestamp - 1),
	} {
		if _, err := NewActor(1, epoch); !errors.Is(err, ErrInvalidTimestamp) {
			t.Fatalf("NewActor(epoch=%v) error = %v, want ErrInvalidTimestamp", epoch, err)
		}
	}
}

func TestActorInitializesFullLeaseQueue(t *testing.T) {
	generator := mustNewActor(t, 1)
	if leaseQueueSize != 64 {
		t.Fatalf("lease queue size = %d, want 64", leaseQueueSize)
	}
	for generation := range uint64(leaseQueueSize) {
		segment := generator.leaseSlot(generation).value.Load()
		if segment == nil || segment.generation != generation || segment.err != nil {
			t.Fatalf("slot %d was not initialized for its generation", generation)
		}
		if size := segment.end - segment.next.Load() + 1; size != defaultLeaseSize {
			t.Fatalf("slot %d size = %d, want %d", generation, size, defaultLeaseSize)
		}
	}
	if generator.state.sequence != sequenceMask {
		t.Fatalf("initial queue reserved through sequence %d, want %d", generator.state.sequence, sequenceMask)
	}
}

func TestActorNextIsConcurrentAndUnique(t *testing.T) {
	generator := mustNewActor(t, 1)

	const count = 10_000
	ids := make(chan int64, count)
	var wg sync.WaitGroup
	wg.Add(count)
	for range count {
		go func() {
			defer wg.Done()
			for {
				value, err := generator.Next()
				if errors.Is(err, ErrLeaseUnavailable) {
					runtime.Gosched()
					continue
				}
				if err != nil {
					t.Errorf("Next() error: %v", err)
					return
				}
				ids <- value
				return
			}
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

func TestActorNextSwitchesToNextQueueSlot(t *testing.T) {
	generator := mustNewActor(t, 2)
	seen := make(map[int64]struct{}, defaultLeaseSize+1)
	for range defaultLeaseSize + 1 {
		value, err := generator.Next()
		if err != nil {
			t.Fatal(err)
		}
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate ID after queue switch: %d", value)
		}
		seen[value] = struct{}{}
	}
	if generation := generator.active.Load(); generation != 1 {
		t.Fatalf("active generation = %d, want 1", generation)
	}
}

func TestActorRefillsEmptiedSlot(t *testing.T) {
	generator := mustNewActor(t, 3)
	for range defaultLeaseSize + 1 {
		if _, err := generator.Next(); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		segment := generator.leaseSlot(leaseQueueSize).value.Load()
		if segment != nil && segment.generation == leaseQueueSize && segment.err == nil &&
			!generator.leaseSlot(leaseQueueSize).refilling.Load() {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("actor did not refill the emptied slot")
}

func TestActorConsumersDoNotWaitWhileQueueHasLeases(t *testing.T) {
	generator := mustNewActor(t, 3)
	generator.mu.Lock()
	locked := true
	defer func() {
		if locked {
			generator.mu.Unlock()
		}
	}()

	done := make(chan error, 1)
	go func() {
		for range IDsPerMillisecond {
			if _, err := generator.Next(); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consume prefilled queue: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("consumers waited for the blocked actor while the queue still had leases")
	}
	if _, err := generator.Next(); !errors.Is(err, ErrLeaseUnavailable) {
		t.Fatalf("Next() after draining blocked queue error = %v, want ErrLeaseUnavailable", err)
	}

	generator.mu.Unlock()
	locked = false
}

func TestActorUnavailableQueueReturnsAfterTenRetries(t *testing.T) {
	generator := &ActorGenerator{}
	exhausted := newLease(0, 1, 0, -1)
	generator.queue.slots[0].value.Store(exhausted)
	generator.queue.slots[1].refilling.Store(true)

	if _, err := generator.Next(); !errors.Is(err, ErrLeaseUnavailable) {
		t.Fatalf("Next() error = %v, want ErrLeaseUnavailable", err)
	}
	if generation := generator.active.Load(); generation != 0 {
		t.Fatalf("active generation = %d after unavailable queue, want 0", generation)
	}
}

func TestActorConcurrentExhaustionSwitchesOnce(t *testing.T) {
	generator := mustNewActor(t, 3)
	for range defaultLeaseSize {
		if _, err := generator.Next(); err != nil {
			t.Fatal(err)
		}
	}

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

	if generation := generator.active.Load(); generation != 1 {
		t.Fatalf("active generation = %d, want one switch to generation 1", generation)
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

func TestActorFailureCanBeRetried(t *testing.T) {
	generator := mustNewRunningActorGenerator(t, 4, testMaxTimestampMilliseconds)
	for range IDsPerMillisecond {
		if _, err := generator.Next(); err != nil {
			t.Fatalf("consume initial queue: %v", err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for {
		_, err := generator.Next()
		if errors.Is(err, ErrInvalidTimestamp) {
			break
		}
		if !errors.Is(err, ErrLeaseUnavailable) {
			t.Fatalf("Next() error = %v, want ErrInvalidTimestamp or temporary unavailability", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("actor did not publish its refill error")
		}
		runtime.Gosched()
	}
	if generation := generator.active.Load(); generation != leaseQueueSize-1 {
		t.Fatalf("active generation = %d, want %d", generation, leaseQueueSize-1)
	}

	generator.mu.Lock()
	generator.state.lastTimestamp = time.Now().UnixMilli() - testEpochMilliseconds
	generator.state.sequence = -1
	generator.mu.Unlock()

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := generator.Next(); err == nil {
			if generation := generator.active.Load(); generation != leaseQueueSize {
				t.Fatalf("active generation = %d, want %d", generation, leaseQueueSize)
			}
			return
		} else if !errors.Is(err, ErrInvalidTimestamp) && !errors.Is(err, ErrLeaseUnavailable) {
			t.Fatalf("Next() retry error: %v", err)
		}
		runtime.Gosched()
	}
	t.Fatal("actor did not recover after state was repaired")
}
