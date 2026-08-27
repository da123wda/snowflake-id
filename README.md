# snowflake-id

`snowflake-id` 是一个并发安全、可解析的 Go `int64` ID 生成器，提供两套彼此独立的实现：

| 包 | 取号方式 | 续租方式 | 适用场景 |
| --- | --- | --- | --- |
| [`actor`](./actor) | 从 4096-ID 环形缓存原子取号 | 后台 `ext.Actor` 回填 | 关注调用延迟，生成器长期存活 |
| [`mutex`](./mutex) | 从当前 64-ID 号段原子取号 | 号段耗尽时互斥锁同步续租 | 关注实现简单、实例较多或负载较低 |

两个包都支持：

- 非负 63 位 `int64` ID；
- 默认 Snowflake 布局；
- 自定义字段位数；Actor 序列固定为 12 位，Mutex 序列可配置；
- 取号时动态传入业务 ID；
- 单个取号与固定 64-ID 批量取号；
- 使用生成器自身配置解析 ID；
- 多 goroutine 并发共享。

## 安装

项目要求 Go 1.27 或更高版本。

```bash
go get github.com/da123wda/snowflake-id/v2/actor
go get github.com/da123wda/snowflake-id/v2/mutex
```

只需安装实际使用的包。

## 默认布局

`NewActor` 和 `NewMutex` 使用相同的默认布局：

```text
 62                    22 21          12 11             0
+------------------------+--------------+----------------+
| 41-bit timestamp delta | machine ID   | sequence       |
+------------------------+--------------+----------------+
          41 bit             10 bit          12 bit
```

```text
ID = ((unixMilliseconds - epoch.UnixMilli()) << 22)
   | (machineID << 12)
   | sequence
```

| 字段 | 位数 | 容量 |
| --- | ---: | ---: |
| 时间戳差值 | 41 | 约 69.7 年 |
| 机器 ID | 10 | 1024 台，取值 `0~1023` |
| 业务 ID | 0 | 只能为 `0` |
| 毫秒内序列 | 12 | 4096 ID/ms |

## 快速开始

### Mutex

```go
package main

import (
	"fmt"
	"log"
	"time"

	mutexid "github.com/da123wda/snowflake-id/v2/mutex"
)

func main() {
	epoch := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	generator, err := mutexid.NewMutex(42, epoch)
	if err != nil {
		log.Fatal(err)
	}

	value, err := generator.Next()
	if err != nil {
		log.Fatal(err)
	}

	parsed, err := generator.Parse(value)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value, parsed.V0, parsed.V1, parsed.V2, parsed.V3)
}
```

### Actor

Actor 的后台回填可能暂时跟不上消费速度，此时 `Next` 返回可重试的 `ErrLeaseUnavailable`：

```go
func nextActor(generator *actorid.ActorGenerator) (int64, error) {
	for {
		value, err := generator.Next()
		if errors.Is(err, actorid.ErrLeaseUnavailable) {
			runtime.Gosched()
			continue
		}
		return value, err
	}
}
```

创建方式与 Mutex 相同：

```go
generator, err := actorid.NewActor(42, epoch)
value, err := nextActor(generator)
parsed, err := generator.Parse(value)
```

`ActorGenerator` 面向服务进程中的长生命周期实例，目前不提供关闭后台 Actor 的公开方法。

## 自定义位布局

ID 从高位到低位统一为：

```text
| timestamp | machine ID | business ID | sequence |
```

### Actor：序列固定为 12 位

Actor 只允许配置时间戳、机器和业务位数：

```go
generator, err := actorid.NewActorWithBits(
	40, // timestampBits
	7,  // machineIDBits
	4,  // businessBits
	12, // machineID
	epoch,
)
```

约束：

```text
timestampBits >= 1
timestampBits + machineIDBits + businessBits = 51
sequenceBits = 12
```

Actor 始终保留完整的 4096-ID 序列空间和 `64 × 64` 环形缓存。

### Mutex：四段均可配置

```go
generator, err := mutexid.NewMutexWithBits(
	40, // timestampBits
	9,  // machineIDBits
	4,  // businessBits
	10, // sequenceBits
	12, // machineID
	epoch,
)
```

约束：

```text
timestampBits >= 1
sequenceBits >= 6
timestampBits + machineIDBits + businessBits + sequenceBits = 63
```

序列至少为 6 位，是因为 `NextBatch` 必须返回 64 个属于同一毫秒的 ID。机器位或业务位可以是 0；对应位数为 0 时，字段值只能是 0。

### 容量计算

| 字段 | 容量 |
| --- | ---: |
| 时间范围 | `(2^timestampBits - 1)` 毫秒 |
| 机器数量 | `2^machineIDBits` |
| 业务数量 | `2^businessBits` |
| 每台机器每毫秒总容量 | `2^sequenceBits` |

例如 `40/7/4/12` 表示约 34.8 年、128 台机器、16 个业务、每台机器总计 4096 ID/ms。

## 动态业务 ID

业务 ID 不在创建生成器时绑定，而是在每次取号时传入：

```go
value, err := generator.NextWithBusinessID(3)
batch, err := generator.NextBatchWithBusinessID(3)
```

需要注意：

- `businessID` 必须在 `0~2^businessBits-1` 范围内；
- `Next()` 和 `NextBatch()` 等价于业务 ID 为 0；
- 同一生成器的所有业务共享时间戳和序列状态，不会因业务数量增加总吞吐；
- 业务位高于序列位，连续调用使用不同业务 ID 时，ID 数值不保证单调递增；
- `machineID` 必须在所有同时运行的生成器之间保持全局唯一，不能只依赖业务 ID 隔离。

## 批量取号

两个生成器都支持：

```go
batch, err := generator.NextBatch()
batch, err := generator.NextBatchWithBusinessID(3)
```

批量接口的契约：

- 固定返回 64 个 ID；
- 批内时间戳、机器 ID 和业务 ID 相同；
- 批内严格递增，相邻 ID 相差 1；
- 可以与单个取号并发调用且不会重复；
- 每批创建一个 64 元素切片，即 512 B、1 次分配。

## 解析 ID

推荐使用生成器方法解析：

```go
parsed, err := generator.Parse(value)
```

生成器会自动使用自身保存的位布局和纪元，返回 `ext.T4[time.Time, int64, int64, int64]`：

| 字段 | 含义 |
| --- | --- |
| `V0` | 号段预留时间 |
| `V1` | 机器 ID |
| `V2` | 业务 ID |
| `V3` | 毫秒内序列号 |

两个包仍保留包级 `Parse(value, epoch)`，用于解析默认 `41/10/0/12` 布局。它返回 `ext.T3`，依次为时间、机器 ID 和序列号。

ID 本身不保存布局或纪元。相同 ID 命名空间必须统一使用一套位布局；使用错误的生成器或纪元解析会得到错误字段。

Actor 会提前预留 ID，因此解析时间表示号段预留时间，不一定等于实际取号时间。以 5 万 ID/s 消费完整 4096-ID 缓存时，队尾时间最多可能比取号时刻早约 81.92 ms。

## 并发、唯一性与时钟

- `ActorGenerator` 和 `MutexGenerator` 都可以被多个 goroutine 共享；
- 唯一性由机器 ID、时间戳和生成器内共享序列共同保证；
- 并发调用不保证按照 goroutine 完成顺序全局单调；
- 自定义纪元按 `UnixMilli()` 截断，不能晚于当前时间；
- 当前时间超出配置的时间戳范围时返回 `ErrInvalidTimestamp`；
- 检测到时钟回拨时停止预留新号段并返回 `ErrInvalidTimestamp`；
- 已预留的 Actor 缓存或 Mutex 当前号段仍可能在下一次续租检测前继续消费。

序列不足时，生成器等待下一毫秒。距离边界较远时先休眠，接近边界后使用 `runtime.Gosched()`；这避免持续忙等，但实际吞吐会受操作系统调度精度影响。

## Actor 与 Mutex 的实现差异

### Actor

```text
64 slots × 64 IDs = 4096 buffered IDs
```

- `lease.next.CompareAndSwap` 负责号段内取号；
- `active.CompareAndSwap` 负责切换环形槽位；
- 后台唯一 Actor 回填已耗尽槽位；
- `Next` 的正常取号和槽位切换不使用互斥锁；
- 回填暂未发布时可能返回 `ErrLeaseUnavailable`。

### Mutex

```text
读取当前号段
→ CAS 取号
→ 号段耗尽时加锁
→ 同步预留并发布新的 64-ID 号段
```

- 当前号段有值时不进入互斥锁；
- 续租由触发耗尽的调用者同步完成；
- 不会返回 `ErrLeaseUnavailable`。

## API

### actor

```go
func NewActor(machineID int64, epoch time.Time) (*ActorGenerator, error)
func NewActorWithBits(timestampBits, machineIDBits, businessBits uint8, machineID int64, epoch time.Time) (*ActorGenerator, error)

func (g *ActorGenerator) Next() (int64, error)
func (g *ActorGenerator) NextWithBusinessID(businessID int64) (int64, error)
func (g *ActorGenerator) NextBatch() (ext.Vec[int64], error)
func (g *ActorGenerator) NextBatchWithBusinessID(businessID int64) (ext.Vec[int64], error)
func (g *ActorGenerator) Parse(value int64) (ext.T4[time.Time, int64, int64, int64], error)

func Parse(value int64, epoch time.Time) (ext.T3[time.Time, int64, int64], error)
```

### mutex

```go
func NewMutex(machineID int64, epoch time.Time) (*MutexGenerator, error)
func NewMutexWithBits(timestampBits, machineIDBits, businessBits, sequenceBits uint8, machineID int64, epoch time.Time) (*MutexGenerator, error)

func (g *MutexGenerator) Next() (int64, error)
func (g *MutexGenerator) NextWithBusinessID(businessID int64) (int64, error)
func (g *MutexGenerator) NextBatch() (ext.Vec[int64], error)
func (g *MutexGenerator) NextBatchWithBusinessID(businessID int64) (ext.Vec[int64], error)
func (g *MutexGenerator) Parse(value int64) (ext.T4[time.Time, int64, int64, int64], error)

func Parse(value int64, epoch time.Time) (ext.T3[time.Time, int64, int64], error)
```

### 默认布局常量

| 常量 | 值 |
| --- | ---: |
| `TimestampBits` | 41 |
| `MachineIDBits` | 10 |
| `BusinessIDBits` | 0 |
| `SequenceBits` | 12 |
| `MaxMachineID` | 1023 |
| `MaxBusinessID` | 0 |
| `IDsPerMillisecond` | 4096 |
| `MaxTimestampDeltaMilliseconds` | `2^41-1` |

### 错误

| 错误 | Actor | Mutex | 含义 |
| --- | :---: | :---: | --- |
| `ErrInvalidBitLayout` | ✓ | ✓ | 自定义位数不满足布局约束 |
| `ErrInvalidMachineID` | ✓ | ✓ | 机器 ID 超出配置范围 |
| `ErrInvalidBusinessID` | ✓ | ✓ | 业务 ID 超出配置范围 |
| `ErrInvalidTimestamp` | ✓ | ✓ | 纪元、时间范围或时钟回拨无效 |
| `ErrInvalidID` | ✓ | ✓ | 解析负数 ID |
| `ErrLeaseUnavailable` | ✓ | — | Actor 缓存暂时不可用，可重试 |

## 性能

12 位序列的理论持续吞吐上限是：

```text
4096 ID/ms = 4,096,000 ID/s
1 second / 4,096,000 ≈ 244.14 ns/ID
```

测试环境：AMD Ryzen 7 4800H、Windows/amd64、`GOMAXPROCS=16`。每项运行 1 秒、重复 5 次取中位数：

| 公开 API | 中位数 | 实测吞吐 | 分配 |
| --- | ---: | ---: | ---: |
| Actor `Next()` | 248.0 ns/ID | 约 403.2 万 ID/s | 0 B/op |
| Actor `NextWithBusinessID()` | 248.7 ns/ID | 约 402.1 万 ID/s | 0 B/op |
| Mutex `Next()` | 247.9 ns/ID | 约 403.4 万 ID/s | 0 B/op |
| Mutex `NextWithBusinessID()` | 245.6 ns/ID | 约 407.2 万 ID/s | 0 B/op |

这些是包含毫秒翻页等待的持续吞吐结果。理论上限假设每个毫秒边界都能精确唤醒；实际值会受到 Windows 调度、`Gosched`、号段续租和并发竞争影响。业务 ID 路径与默认路径的细小差异属于测量噪声。

## 测试与基准

所有测试位于各实现自己的 `test` 子包：

```text
actor/test/
mutex/test/
```

运行全部测试和静态检查：

```bash
go test ./...
go vet ./...
```

运行四种 4096 ID/ms 公开 API 基准：

```bash
go test ./actor/test ./mutex/test \
  -run "^$" \
  -bench "4096PerMillisecond$" \
  -benchmem -benchtime=1s -count=5
```

测试覆盖默认与自定义布局、动态业务 ID、字段校验、解析、并发唯一性、单个与批量取号不重叠，以及持续吞吐。

## 项目结构

```text
snowflake-id/
├── actor/
│   ├── test/     # Actor 公开 API 与性能测试
│   └── *.go
├── mutex/
│   ├── test/     # Mutex 公开 API 与性能测试
│   └── *.go
├── go.mod
└── README.md
```

项目地址：[github.com/da123wda/snowflake-id](https://github.com/da123wda/snowflake-id)
