package id

import (
	"time"

	"github.com/lee-ext/go-extend/ext"
)

func mustNewMutexGenerator(machineID, unixMilliseconds int64) *MutexGenerator {
	generator, err := newMutexGenerator(machineID, unixMilliseconds)
	if err != nil {
		panic(err)
	}
	return generator
}

func mustNewMutex(machineID int64) *MutexGenerator {
	generator, err := NewMutex(machineID)
	if err != nil {
		panic(err)
	}
	return generator
}

func mustNext(generator *MutexGenerator) int64 {
	value, err := generator.Next()
	if err != nil {
		panic(err)
	}
	return value
}

func mustNextBatch(generator *MutexGenerator) ext.Vec[int64] {
	values, err := generator.NextBatch()
	if err != nil {
		panic(err)
	}
	return values
}

func acquireMutexAtValue(generator *MutexGenerator, unixMilliseconds int64, size int) (*lease, error) {
	generator.mu.Lock()
	defer generator.mu.Unlock()
	return generator.state.lease(unixMilliseconds, size)
}

func leaseIDAtValue(generator *MutexGenerator, unixMilliseconds int64) (int64, error) {
	segment, err := acquireMutexAtValue(generator, unixMilliseconds, 1)
	if err != nil {
		return 0, err
	}
	value, _ := segment.take()
	return value, nil
}

func parseValue(value int64) (time.Time, int64, int64, error) {
	parsed, err := Parse(value)
	if err != nil {
		return time.Time{}, 0, 0, err
	}
	return parsed.V0, parsed.V1, parsed.V2, nil
}
