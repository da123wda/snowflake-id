package actor

import (
	"testing"
	"time"

	"github.com/lee-ext/go-extend/ext"
)

func mustNewActorGenerator(machineID, unixMilliseconds int64) *ActorGenerator {
	state, err := newIDState(machineID, unixMilliseconds)
	if err != nil {
		panic(err)
	}
	return &ActorGenerator{state: state}
}

func mustNewActor(tb testing.TB, machineID int64, actorCapacity ...int) *ActorGenerator {
	tb.Helper()
	generator, err := NewActor(machineID, actorCapacity...)
	if err != nil {
		panic(err)
	}
	tb.Cleanup(generator.refillActor.Close)
	return generator
}

func mustNewRunningActorGenerator(tb testing.TB, machineID, unixMilliseconds int64) *ActorGenerator {
	tb.Helper()
	generator, err := newActorGenerator(machineID, unixMilliseconds, DefaultActorCapacity)
	if err != nil {
		panic(err)
	}
	tb.Cleanup(generator.refillActor.Close)
	return generator
}

func mustNext(generator *ActorGenerator) int64 {
	value, err := generator.Next()
	if err != nil {
		panic(err)
	}
	return value
}

func mustNextBatch(generator *ActorGenerator) ext.Vec[int64] {
	values, err := generator.NextBatch()
	if err != nil {
		panic(err)
	}
	return values
}

func acquireMutexAtValue(generator *ActorGenerator, unixMilliseconds int64, size int) (*lease, error) {
	generator.mu.Lock()
	defer generator.mu.Unlock()
	return generator.state.lease(unixMilliseconds, size)
}

func leaseIDAtValue(generator *ActorGenerator, unixMilliseconds int64) (int64, error) {
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
