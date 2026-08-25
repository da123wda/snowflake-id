package id

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/lee-ext/go-extend/ext"
)

func nextBatchAt(generator *ActorGenerator, unixMilliseconds int64) (ext.Vec[int64], error) {
	reserved, err := reserveBatchAt(generator, unixMilliseconds)
	if err != nil {
		return nil, err
	}
	return batchValues(reserved, generator.state.machineID), nil
}

func reserveBatchAt(generator *ActorGenerator, unixMilliseconds int64) (sequenceRange, error) {
	generator.mu.Lock()
	defer generator.mu.Unlock()
	return generator.state.reserve(unixMilliseconds, defaultLeaseSize)
}

func TestActorNextBatchReturns64OrderedIDs(t *testing.T) {
	const machineID = int64(7)
	batch, err := mustNewActor(t, machineID).NextBatch()
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != defaultLeaseSize {
		t.Fatalf("batch length = %d, want %d", len(batch), defaultLeaseSize)
	}

	firstTimestamp, _, _, err := parseValue(batch[0])
	if err != nil {
		t.Fatal(err)
	}
	for index, value := range batch {
		timestamp, gotMachineID, _, err := parseValue(value)
		if err != nil {
			t.Fatal(err)
		}
		if timestamp != firstTimestamp || gotMachineID != machineID {
			t.Fatalf("batch[%d] parsed as (%v, %d), want (%v, %d)", index, timestamp, gotMachineID, firstTimestamp, machineID)
		}
		if index > 0 && value != batch[index-1]+1 {
			t.Fatalf("batch[%d] = %d, previous = %d", index, value, batch[index-1])
		}
	}
}

func TestActorNextAndNextBatchAreConcurrentAndUnique(t *testing.T) {
	const machineID = int64(9)
	const batchCount = 64
	const nextCount = 4096
	generator := mustNewActor(t, machineID)
	ids := make(chan int64, batchCount*defaultLeaseSize+nextCount)
	var wg sync.WaitGroup
	wg.Add(batchCount + nextCount)

	for range batchCount {
		go func() {
			defer wg.Done()
			batch, err := generator.NextBatch()
			if err != nil {
				t.Errorf("NextBatch() error: %v", err)
				return
			}
			for index, value := range batch {
				if index > 0 && value != batch[index-1]+1 {
					t.Errorf("batch is not strictly increasing at %d", index)
					return
				}
				ids <- value
			}
		}()
	}
	for range nextCount {
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

	want := batchCount*defaultLeaseSize + nextCount
	seen := make(map[int64]struct{}, want)
	for value := range ids {
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate ID: %d", value)
		}
		seen[value] = struct{}{}
		_, gotMachineID, _, err := parseValue(value)
		if err != nil {
			t.Fatal(err)
		}
		if gotMachineID != machineID {
			t.Fatalf("machine ID = %d, want %d", gotMachineID, machineID)
		}
	}
	if len(seen) != want {
		t.Fatalf("unique IDs = %d, want %d", len(seen), want)
	}
}

func TestActorNextBatchDoesNotOverlapCurrentLease(t *testing.T) {
	generator := mustNewActor(t, 3)
	seen := make(map[int64]struct{}, 2*defaultLeaseSize)
	add := func(value int64) {
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate ID: %d", value)
		}
		seen[value] = struct{}{}
	}

	add(mustNext(generator))
	for _, value := range mustNextBatch(generator) {
		add(value)
	}
	for range defaultLeaseSize - 1 {
		add(mustNext(generator))
	}
	if want := 2 * defaultLeaseSize; len(seen) != want {
		t.Fatalf("unique IDs = %d, want %d", len(seen), want)
	}
}

func TestActorNextBatchMovesWholeBatchToNextMillisecond(t *testing.T) {
	initialMilliseconds := time.Now().UnixMilli()
	generator := mustNewActorGenerator(1, initialMilliseconds)
	generator.state.sequence = sequenceMask - 32

	batch, err := nextBatchAt(generator, initialMilliseconds)
	if err != nil {
		t.Fatal(err)
	}
	for index, value := range batch {
		timestamp, _, sequence, err := parseValue(value)
		if err != nil {
			t.Fatal(err)
		}
		if timestamp.UnixMilli() <= initialMilliseconds || sequence != int64(index) {
			t.Fatalf("batch[%d] = (timestamp %d, sequence %d), want next millisecond sequence %d", index, timestamp.UnixMilli(), sequence, index)
		}
	}
}

func TestActorNextBatchPropagatesClockRollback(t *testing.T) {
	generator := mustNewActorGenerator(1, EpochMilliseconds+10)
	if _, err := nextBatchAt(generator, EpochMilliseconds+9); !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("NextBatch error = %v, want ErrInvalidTimestamp", err)
	}
}

func TestActorNextBatchSupportsAndExhaustsMaximumTimestamp(t *testing.T) {
	generator := mustNewActorGenerator(MaxMachineID, MaxTimestampMilliseconds)
	generator.state.sequence = sequenceMask - defaultLeaseSize

	batch, err := nextBatchAt(generator, MaxTimestampMilliseconds)
	if err != nil {
		t.Fatal(err)
	}
	if last := batch[len(batch)-1]; last != math.MaxInt64 {
		t.Fatalf("last batch ID = %d, want %d", last, int64(math.MaxInt64))
	}
	if _, err := nextBatchAt(generator, MaxTimestampMilliseconds); !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("NextBatch after maximum timestamp exhaustion error = %v, want ErrInvalidTimestamp", err)
	}
}

// SingleCost resets internal state with the timer stopped so it measures one batch
// reservation and Vec construction without initialization or the sustained limit.
func BenchmarkActorNextBatchSingleCost(b *testing.B) {
	generator := mustNewActorGenerator(1, EpochMilliseconds+1)
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		generator.state.lastTimestamp = 0
		generator.state.sequence = -1
		b.StartTimer()
		batch, err := generator.NextBatch()
		if err != nil || len(batch) != defaultLeaseSize {
			b.Fatal("invalid batch")
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*defaultLeaseSize), "ns/id")
}

// ParallelSustained includes the single-machine 4096 IDs/ms format limit.
func BenchmarkActorNextBatchParallelSustained(b *testing.B) {
	generator := mustNewActor(b, 1)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := generator.NextBatch(); err != nil {
				b.Errorf("NextBatch() error: %v", err)
				return
			}
		}
	})
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*defaultLeaseSize), "ns/id")
}
