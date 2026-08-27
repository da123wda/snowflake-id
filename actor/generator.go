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
	return newActor(defaultBitLayout, machineID, epoch)
}

// NewActorWithBits 使用自定义时间戳、机器和业务位数创建生成器。序列固定为 12 位，业务 ID 在取号时指定。
func NewActorWithBits(timestampBits, machineIDBits, businessBits uint8, machineID int64, epoch time.Time) (*ActorGenerator, error) {
	layout, err := newBitLayout(timestampBits, machineIDBits, businessBits)
	if err != nil {
		return nil, err
	}
	return newActor(layout, machineID, epoch)
}

func newActor(layout bitLayout, machineID int64, epoch time.Time) (*ActorGenerator, error) {
	initialUnixMilliseconds := time.Now().UnixMilli()
	state, err := newIDStateWithLayout(layout, machineID, epoch.UnixMilli(), initialUnixMilliseconds)
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
	return g.next(0)
}

// NextWithBusinessID 使用本次调用的业务 ID 返回下一个全局唯一 ID。
func (g *ActorGenerator) NextWithBusinessID(businessID int64) (int64, error) {
	if businessID < 0 || businessID > g.state.layout.businessMask {
		return 0, ErrInvalidBusinessID
	}
	return g.next(businessID << g.state.layout.businessShift)
}

func (g *ActorGenerator) next(businessBits int64) (int64, error) {
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
			return value | businessBits, nil
		}
		if err := g.advanceLease(generation); err != nil {
			return 0, err
		}
	}
}

// NextBatch 一次返回 64 个严格递增的 ID。
// 批内 ID 具有相同时间戳、machineID 和 businessID；与 Next 并发混用时保证唯一，但不保证按完成顺序全局单调。
func (g *ActorGenerator) NextBatch() (ext.Vec[int64], error) {
	return g.nextBatch(0)
}

// NextBatchWithBusinessID 使用本次调用的业务 ID 返回 64 个 ID。
func (g *ActorGenerator) NextBatchWithBusinessID(businessID int64) (ext.Vec[int64], error) {
	if businessID < 0 || businessID > g.state.layout.businessMask {
		return nil, ErrInvalidBusinessID
	}
	return g.nextBatch(businessID)
}

func (g *ActorGenerator) nextBatch(businessID int64) (ext.Vec[int64], error) {
	g.mu.Lock()
	reserved, err := g.state.reserve(time.Now().UnixMilli(), defaultLeaseSize)
	g.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return batchValuesWithState(reserved, g.state, businessID), nil
}

func batchValues(reserved sequenceRange, machineID int64) ext.Vec[int64] {
	state := &idState{layout: defaultBitLayout, machineID: machineID}
	return batchValuesWithState(reserved, state, 0)
}

func batchValuesWithState(reserved sequenceRange, state *idState, businessID int64) ext.Vec[int64] {
	values := make(ext.Vec[int64], defaultLeaseSize)
	prefix := state.layout.prefix(reserved.timestamp, state.machineID, businessID)
	for index := range values {
		values[index] = prefix | (reserved.start + int64(index))
	}
	return values
}
