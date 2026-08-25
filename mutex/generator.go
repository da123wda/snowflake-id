package mutex

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/lee-ext/go-extend/ext"
)

// MutexGenerator 使用互斥锁保护号段预留，Next 从当前号段原子取号。
type MutexGenerator struct {
	state   *idState
	mu      sync.Mutex
	current atomic.Pointer[lease]
}

// NewMutex 使用自定义纪元创建独立的互斥锁生成器。
// 同一个 machineID 在所有进程和生成器中必须保持独占。
func NewMutex(machineID int64, epoch time.Time) (*MutexGenerator, error) {
	return newMutexGenerator(machineID, epoch.UnixMilli(), time.Now().UnixMilli())
}

func newMutexGenerator(machineID, epochMilliseconds, initialUnixMilliseconds int64) (*MutexGenerator, error) {
	state, err := newIDState(machineID, epochMilliseconds, initialUnixMilliseconds)
	if err != nil {
		return nil, err
	}
	return &MutexGenerator{state: state}, nil
}

// Next 返回下一个全局唯一 ID。热路径无锁，号段耗尽时由一个调用者加锁续租。
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

// NextBatch 返回 64 个严格递增且属于同一毫秒的 ID。
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
