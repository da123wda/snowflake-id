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
	return newMutex(defaultBitLayout, machineID, epoch)
}

// NewMutexWithBits 使用自定义时间戳、机器、业务和序列位数创建生成器。业务 ID 在取号时指定。
func NewMutexWithBits(timestampBits, machineIDBits, businessBits, sequenceBits uint8, machineID int64, epoch time.Time) (*MutexGenerator, error) {
	layout, err := newBitLayout(timestampBits, machineIDBits, businessBits, sequenceBits)
	if err != nil {
		return nil, err
	}
	return newMutex(layout, machineID, epoch)
}

func newMutex(layout bitLayout, machineID int64, epoch time.Time) (*MutexGenerator, error) {
	state, err := newIDStateWithLayout(layout, machineID, epoch.UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		return nil, err
	}
	return &MutexGenerator{state: state}, nil
}

// Next 返回下一个全局唯一 ID。热路径无锁，号段耗尽时由一个调用者加锁续租。
func (g *MutexGenerator) Next() (int64, error) {
	return g.next(0)
}

// NextWithBusinessID 使用本次调用的业务 ID 返回下一个全局唯一 ID。
func (g *MutexGenerator) NextWithBusinessID(businessID int64) (int64, error) {
	if businessID < 0 || businessID > g.state.layout.businessMask {
		return 0, ErrInvalidBusinessID
	}
	return g.next(businessID << g.state.layout.businessShift)
}

func (g *MutexGenerator) next(businessBits int64) (int64, error) {
	for {
		current := g.current.Load()
		if current != nil {
			if value, ok := current.take(); ok {
				return value | businessBits, nil
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

// NextBatch 返回 64 个严格递增且属于同一毫秒的 ID。
func (g *MutexGenerator) NextBatch() (ext.Vec[int64], error) {
	return g.nextBatch(0)
}

// NextBatchWithBusinessID 使用本次调用的业务 ID 返回 64 个 ID。
func (g *MutexGenerator) NextBatchWithBusinessID(businessID int64) (ext.Vec[int64], error) {
	if businessID < 0 || businessID > g.state.layout.businessMask {
		return nil, ErrInvalidBusinessID
	}
	return g.nextBatch(businessID)
}

func (g *MutexGenerator) nextBatch(businessID int64) (ext.Vec[int64], error) {
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
