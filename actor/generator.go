package actor

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

const actorCapacity = 64

// NewActor 使用自定义纪元创建 ActorGenerator。Actor 邮箱容量固定为 64。
// 同一个 machineID 在同一时刻只能分配给一个生成器。
func NewActor(machineID int64, epoch time.Time) (*ActorGenerator, error) {
	return newActorGenerator(machineID, epoch.UnixMilli(), time.Now().UnixMilli())
}

// newActorGenerator 使用指定的初始 Unix 毫秒时间戳创建 ActorGenerator。
func newActorGenerator(machineID, epochMilliseconds, initialUnixMilliseconds int64) (*ActorGenerator, error) {
	state, err := newIDState(machineID, epochMilliseconds, initialUnixMilliseconds)
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
