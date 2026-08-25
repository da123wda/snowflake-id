package id

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/lee-ext/go-extend/ext"
)

// MutexGenerator 使用互斥锁保护号段预留，Next 从号段本地并发取号。
type MutexGenerator struct {
	state   *idState
	mu      sync.Mutex
	current atomic.Pointer[lease]
}

// NewMutex 创建 MutexGenerator。
// 同一个 machineID 在同一时刻只能分配给一个生成器。
func NewMutex(machineID int64) (*MutexGenerator, error) {
	return newMutexGenerator(machineID, time.Now().UnixMilli())
}

// newMutexGenerator 使用指定的初始 Unix 毫秒时间戳创建 MutexGenerator。
func newMutexGenerator(machineID, initialUnixMilliseconds int64) (*MutexGenerator, error) {
	state, err := newIDState(machineID, initialUnixMilliseconds)
	if err != nil {
		return nil, err
	}
	return &MutexGenerator{state: state}, nil
}

// Next 从内部号段返回下一个全局唯一的 ID，号段会自动续租。
// 时钟回拨在号段预留时检测；缓存尚可用时 Next 可能继续成功。
func (g *MutexGenerator) Next() (int64, error) {
	for {
		current := g.current.Load()
		if current != nil {
			if value, ok := current.take(); ok {
				return value, nil
			}
		}
		if err := g.replaceLease(current); err != nil {
			return 0, err
		}
	}
}

func (g *MutexGenerator) replaceLease(observed *lease) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.current.Load() != observed {
		return nil
	}
	segment, err := g.state.lease(time.Now().UnixMilli(), defaultLeaseSize)
	if err != nil {
		return err
	}
	g.current.Store(segment)
	return nil
}

func (g *MutexGenerator) reserveBatch() (sequenceRange, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state.reserve(time.Now().UnixMilli(), defaultLeaseSize)
}

// NextBatch 一次返回 64 个严格递增的 ID。
// 批内 ID 具有相同时间戳和 machineID；与 Next 并发混用时保证唯一，但不保证按完成顺序全局单调。
func (g *MutexGenerator) NextBatch() (ext.Vec[int64], error) {
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
