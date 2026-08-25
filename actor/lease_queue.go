package actor

import (
	"runtime"
	"sync/atomic"
)

type leaseSlot struct {
	value            atomic.Pointer[lease]
	refilling        atomic.Bool
	refillGeneration atomic.Uint64
	refill           func()
}

type leaseQueue struct {
	slots [leaseQueueSize]leaseSlot
}

func (g *ActorGenerator) leaseSlot(generation uint64) *leaseSlot {
	return &g.queue.slots[generation%leaseQueueSize]
}

func (g *ActorGenerator) requestRefill(slot *leaseSlot, generation uint64) {
	if !slot.refilling.CompareAndSwap(false, true) {
		return
	}
	slot.refillGeneration.Store(generation)
	g.refillActor.Launch(slot.refill)
}

func (g *ActorGenerator) fillLeaseSlot(slot *leaseSlot) {
	defer slot.refilling.Store(false)
	generation := slot.refillGeneration.Load()
	segment, err := g.reserveLease()
	if err != nil {
		segment = &lease{generation: generation, err: err, end: -1}
	} else {
		segment.generation = generation
	}
	slot.value.Store(segment)
}

func (g *ActorGenerator) advanceLease(generation uint64) error {
	nextGeneration := generation + 1
	nextSlot := g.leaseSlot(nextGeneration)
	for range leaseSwitchRetries {
		if g.active.Load() != generation {
			return nil
		}

		next := nextSlot.value.Load()
		if next != nil && next.generation == nextGeneration {
			if next.err != nil {
				g.requestRefill(nextSlot, nextGeneration)
				return next.err
			}
			if g.active.CompareAndSwap(generation, nextGeneration) {
				g.requestRefill(g.leaseSlot(generation), generation+leaseQueueSize)
			}
			return nil
		}

		g.requestRefill(nextSlot, nextGeneration)
		runtime.Gosched()
	}
	return ErrLeaseUnavailable
}
