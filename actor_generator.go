package id

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/lee-ext/go-extend/ext"
)

// ActorGenerator 从环形号段队列并发取号，唯一后台 Actor 负责回填耗尽的号段。
type ActorGenerator struct {
	state *idState
	mu    sync.Mutex

	queue  leaseQueue
	active atomic.Uint64

	refillActor ext.Actor
}

// DefaultActorCapacity 是 Actor 邮箱的默认容量。
const DefaultActorCapacity = 64

// NewActor 创建 ActorGenerator。actorCapacity 省略时默认为 64，显式值不能小于 64。
// 同一个 machineID 在同一时刻只能分配给一个生成器。
func NewActor(machineID int64, actorCapacity ...int) (*ActorGenerator, error) {
	capacity, err := resolveActorCapacity(actorCapacity)
	if err != nil {
		return nil, err
	}
	return newActorGenerator(machineID, time.Now().UnixMilli(), capacity)
}

func resolveActorCapacity(capacities []int) (int, error) {
	if len(capacities) == 0 {
		return DefaultActorCapacity, nil
	}
	if len(capacities) != 1 || capacities[0] < leaseQueueSize {
		return 0, ErrInvalidActorCapacity
	}
	return capacities[0], nil
}

// newActorGenerator 使用指定的初始 Unix 毫秒时间戳创建 ActorGenerator。
func newActorGenerator(machineID, initialUnixMilliseconds int64, actorCapacity int) (*ActorGenerator, error) {
	state, err := newIDState(machineID, initialUnixMilliseconds)
	if err != nil {
		return nil, err
	}
	generator := &ActorGenerator{state: state}
	for generation := range uint64(leaseQueueSize) {
		segment, err := state.lease(initialUnixMilliseconds, defaultLeaseSize)
		if err != nil {
			return nil, err
		}
		segment.generation = generation
		generator.queue.slots[generation].value.Store(segment)
	}
	for index := range leaseQueueSize {
		slot := &generator.queue.slots[index]
		slot.refill = func() { generator.fillLeaseSlot(slot) }
	}
	generator.refillActor = ext.Actor_(actorCapacity, func(any) {})
	return generator, nil
}

// Next 从内部号段返回下一个全局唯一的 ID，号段会自动续租。
// 时钟回拨在号段预留时检测；缓存尚可用时 Next 可能继续成功。
func (g *ActorGenerator) Next() (int64, error) {
	for {
		generation := g.active.Load()
		current := g.leaseSlot(generation).value.Load()
		if current == nil || current.generation != generation {
			return 0, ErrLeaseUnavailable
		}
		if current.err != nil {
			g.requestRefill(g.leaseSlot(generation), generation)
			return 0, current.err
		}
		if value, ok := current.take(); ok {
			return value, nil
		}
		if err := g.advanceLease(generation); err != nil {
			return 0, err
		}
	}
}

func (g *ActorGenerator) reserveLease() (*lease, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state.lease(time.Now().UnixMilli(), defaultLeaseSize)
}

func (g *ActorGenerator) reserveBatch() (sequenceRange, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state.reserve(time.Now().UnixMilli(), defaultLeaseSize)
}

// NextBatch 一次返回 64 个严格递增的 ID。
// 批内 ID 具有相同时间戳和 machineID；与 Next 并发混用时保证唯一，但不保证按完成顺序全局单调。
func (g *ActorGenerator) NextBatch() (ext.Vec[int64], error) {
	reserved, err := g.reserveBatch()
	if err != nil {
		return nil, err
	}
	return batchValues(reserved, g.state.machineID), nil
}

func batchValues(reserved sequenceRange, machineID int64) ext.Vec[int64] {
	values := make(ext.Vec[int64], defaultLeaseSize)
	prefix := (reserved.timestamp << timestampShift) | (machineID << machineIDShift)
	for index := range values {
		values[index] = prefix | (reserved.start + int64(index))
	}
	return values
}
