package mutex

import (
	"errors"
	"sync"
	"testing"
)

func TestNewMutexValidatesMachineID(t *testing.T) {
	for _, machineID := range []int64{0, MaxMachineID} {
		if _, err := NewMutex(machineID); err != nil {
			t.Fatalf("NewMutex(%d): %v", machineID, err)
		}
	}
	for _, machineID := range []int64{-1, MaxMachineID + 1} {
		if _, err := NewMutex(machineID); !errors.Is(err, ErrInvalidMachineID) {
			t.Fatalf("NewMutex(%d) error = %v, want ErrInvalidMachineID", machineID, err)
		}
	}
}

func TestMutexNextRenewsAfter64IDs(t *testing.T) {
	generator, err := NewMutex(1)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[int64]struct{}, defaultLeaseSize+1)
	for range defaultLeaseSize + 1 {
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

func TestMutexNextIsConcurrentAndUnique(t *testing.T) {
	generator, err := NewMutex(2)
	if err != nil {
		t.Fatal(err)
	}
	const count = 10_000
	ids := make(chan int64, count)
	var wg sync.WaitGroup
	wg.Add(count)
	for range count {
		go func() {
			defer wg.Done()
			value, err := generator.Next()
			if err != nil {
				t.Errorf("Next: %v", err)
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
		parsed, err := Parse(value)
		if err != nil || parsed.V1 != 2 {
			t.Fatalf("Parse(%d) = (%v, %d), error %v", value, parsed.V0, parsed.V1, err)
		}
	}
	if len(seen) != count {
		t.Fatalf("unique IDs = %d, want %d", len(seen), count)
	}
}

func TestMutexNextAndNextBatchDoNotOverlap(t *testing.T) {
	generator, err := NewMutex(3)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[int64]struct{}, 2*defaultLeaseSize)
	first, err := generator.Next()
	if err != nil {
		t.Fatal(err)
	}
	seen[first] = struct{}{}
	batch, err := generator.NextBatch()
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range batch {
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate batch ID: %d", value)
		}
		seen[value] = struct{}{}
	}
	for range defaultLeaseSize - 1 {
		value, err := generator.Next()
		if err != nil {
			t.Fatal(err)
		}
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate Next ID: %d", value)
		}
		seen[value] = struct{}{}
	}
}
