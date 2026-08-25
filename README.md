# snowflake-id

`snowflake-id` 是一个并发安全、可解析的 Go `int64` ID 生成器。项目提供两个完全独立的实现包：

| 包 | 取号方式 | 号段续租 | 适合场景 |
| --- | --- | --- | --- |
| [`actor`](./actor) | 环形队列内原子 CAS 取号 | 唯一后台 `ext.Actor` 回填 | 关注调用方延迟、希望号段无缝切换 |
| [`mutex`](./mutex) | 当前号段内原子 CAS 取号 | 号段耗尽时互斥锁同步续租 | 关注实现简单、实例数量较多或负载较低 |

两个包拥有各自独立的状态、错误、解析和测试，不会相互调用。

## 重要约束

- `machineID` 的有效范围是 `0~1023`。
- 同一时刻，每个生成器必须使用全局独占的 `machineID`。Actor 与 Mutex 混用时也不能重复。
- 构造生成器和解析 ID 必须传入同一个自定义纪元 `time.Time`。
- 自定义纪元按 `UnixMilli()` 截断到毫秒，不能晚于当前时间。
- 当前时间与纪元之间的跨度不能超过 41 位毫秒范围，约 69.7 年。
- 单个 `machineID` 每毫秒最多生成 4096 个 ID，即格式上限约为 409.6 万 ID/秒。
- 并发调用保证唯一，但不保证按照 goroutine 完成顺序全局单调递增。

## ID 布局

生成的 ID 使用 63 位非负 `int64`：

```text
 62                    22 21          12 11             0
+------------------------+--------------+----------------+
| 41-bit timestamp delta | machine ID   | sequence       |
+------------------------+--------------+----------------+
          41 bit             10 bit          12 bit
```

编码公式：

```text
ID = ((unixMilliseconds - epoch.UnixMilli()) << 22)
   | (machineID << 12)
   | sequence
```

| 字段 | 位数 | 范围 |
| --- | ---: | ---: |
| 纪元后的毫秒差 | 41 | `0 ~ 2^41-1` |
| 机器 ID | 10 | `0 ~ 1023` |
| 毫秒内序列号 | 12 | `0 ~ 4095` |

## 安装

项目要求 Go 1.27 或更高版本。

只使用 Actor：

```bash
go get github.com/da123wda/snowflake-id/actor
```

只使用 Mutex：

```bash
go get github.com/da123wda/snowflake-id/mutex
```

## Actor 使用示例

Actor 版在初始化时预留 64 个号段，每段 64 个 ID，总缓存容量为 4096 个 ID。每个生成器实例只创建一个 Actor，邮箱容量固定为 64。

```go
package main

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"time"

	actorid "github.com/da123wda/snowflake-id/actor"
)

var epoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func nextID(generator *actorid.ActorGenerator) (int64, error) {
	for {
		value, err := generator.Next()
		if errors.Is(err, actorid.ErrLeaseUnavailable) {
			// 后台 Actor 尚未填好下一槽，稍后重试。
			runtime.Gosched()
			continue
		}
		return value, err
	}
}

func main() {
	// machineID=42 必须在所有同时运行的生成器中保持全局独占。
	generator, err := actorid.NewActor(42, epoch)
	if err != nil {
		log.Fatal(err)
	}

	value, err := nextID(generator)
	if err != nil {
		log.Fatal(err)
	}

	parsed, err := actorid.Parse(value, epoch)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf(
		"id=%d time=%s machine=%d sequence=%d\n",
		value,
		parsed.V0.UTC().Format(time.RFC3339Nano),
		parsed.V1,
		parsed.V2,
	)
}
```

`ErrLeaseUnavailable` 是 Actor 包的临时错误：当所有预填充号段都已耗尽，而 Actor 尚未发布下一号段时，`Next` 在内部让出调度权重试 10 次，随后返回该错误。调用方可以只对这个错误重试；`ErrInvalidTimestamp` 等其他错误应直接处理。

`ActorGenerator` 不提供关闭 Actor 的 API，适合作为服务进程中的长生命周期实例。创建多个 ID 生成器没有问题，每个实例各自拥有且仅拥有一个 Actor。

## Mutex 使用示例

Mutex 版不创建 Actor。`Next` 通常从当前 64-ID 号段原子取号，只有首次取号或当前号段耗尽时才加锁续租。

```go
package main

import (
	"fmt"
	"log"
	"time"

	mutexid "github.com/da123wda/snowflake-id/mutex"
)

func main() {
	epoch := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	generator, err := mutexid.NewMutex(43, epoch)
	if err != nil {
		log.Fatal(err)
	}

	value, err := generator.Next()
	if err != nil {
		log.Fatal(err)
	}

	parsed, err := mutexid.Parse(value, epoch)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("id=%d machine=%d sequence=%d\n", value, parsed.V1, parsed.V2)
}
```

Mutex 版没有 `ErrLeaseUnavailable`：号段耗尽时，负责续租的调用者会同步完成预留或返回具体错误。

## 批量取号

两个包都提供相同形式的 `NextBatch`：

```go
batch, err := generator.NextBatch()
if err != nil {
	return err
}

fmt.Println(len(batch))       // 64
fmt.Println(batch[0])         // 第一个 ID
fmt.Println(batch[len(batch)-1])
```

`NextBatch()` 返回 `ext.Vec[int64]`，其行为如下：

- 固定返回 64 个 ID；
- 批内 ID 属于同一个毫秒、同一个 `machineID`；
- 批内严格递增，相邻 ID 相差 1；
- 可以和 `Next()` 并发调用且不会重复；
- 与其他批次或 `Next()` 按完成顺序观察时，不保证全局单调；
- 每批创建一个 64 元素切片，即 512 B、1 次分配。

## 解析 ID

Actor 与 Mutex 各自提供 `Parse`，调用时必须使用生成 ID 时的纪元：

```go
parsed, err := actorid.Parse(value, epoch)
// 或：mutexid.Parse(value, epoch)
```

返回类型为 `ext.T3[time.Time, int64, int64]`：

| 字段 | 含义 |
| --- | --- |
| `V0` | 号段预留时间 |
| `V1` | 机器 ID |
| `V2` | 毫秒内序列号 |

ID 中不会保存纪元本身，因此传错纪元会得到错误的时间。这属于调用方配置契约；机器 ID 和序列号不受纪元影响。

解析出的时间表示号段预留时刻，不一定等于 `Next()` 的实际调用时刻。Actor 版会预填充 4096 个 ID：以 5 万 ID/秒消费时，队尾 ID 的时间戳可能比实际取号时间早约 81.92 ms；长时间空闲后可能更早。

## 并发使用

`ActorGenerator` 和 `MutexGenerator` 都可以被多个 goroutine 并发共享：

```go
var wg sync.WaitGroup

for range 100 {
	wg.Go(func() {
		value, err := generator.Next()
		if err != nil {
			log.Printf("generate ID: %v", err)
			return
		}
		consume(value)
	})
}

wg.Wait()
```

唯一性由内部原子取号和号段预留保证。若业务要求严格按照调用完成顺序递增，必须在业务层串行化。

## API 参考

### actor 包

```go
func NewActor(machineID int64, epoch time.Time) (*ActorGenerator, error)
func (g *ActorGenerator) Next() (int64, error)
func (g *ActorGenerator) NextBatch() (ext.Vec[int64], error)
func Parse(value int64, epoch time.Time) (ext.T3[time.Time, int64, int64], error)
```

### mutex 包

```go
func NewMutex(machineID int64, epoch time.Time) (*MutexGenerator, error)
func (g *MutexGenerator) Next() (int64, error)
func (g *MutexGenerator) NextBatch() (ext.Vec[int64], error)
func Parse(value int64, epoch time.Time) (ext.T3[time.Time, int64, int64], error)
```

### 公开常量

两个包分别公开相同数值的布局常量：

| 常量 | 值 | 含义 |
| --- | ---: | --- |
| `MachineIDBits` | 10 | 机器 ID 位数 |
| `MaxMachineID` | 1023 | 最大机器 ID |
| `IDsPerMillisecond` | 4096 | 单机器每毫秒序列容量 |
| `MaxTimestampDeltaMilliseconds` | `2^41-1` | 纪元后最大毫秒差 |

### 错误

| 错误 | Actor | Mutex | 含义 |
| --- | :---: | :---: | --- |
| `ErrInvalidMachineID` | ✓ | ✓ | `machineID` 不在 `0~1023` |
| `ErrInvalidTimestamp` | ✓ | ✓ | 纪元无效、时钟回拨或超过 41 位时间范围 |
| `ErrInvalidID` | ✓ | ✓ | 解析负数 ID |
| `ErrLeaseUnavailable` | ✓ | — | Actor 队列暂时没有可消费号段，可重试 |

## 实现方式

### Actor：固定环形队列 + 唯一回填 Actor

Actor 生成器持有 64 个固定槽位，每个槽位是一个 64-ID 号段：

```text
64 slots × 64 IDs = 4096 buffered IDs
```

核心流程：

```text
读取 active generation
→ 从 generation % 64 对应号段 CAS 取号
→ 号段有值：立即返回
→ 号段耗尽：检查下一 generation
→ CAS 切换 active
→ CAS 赢家把旧槽位提交给唯一 Actor
→ Actor 在后台预留新号段并原子发布到旧槽位
```

实现要点：

1. `lease.next.CompareAndSwap` 是逐 ID 取号的线性化点；
2. `active.CompareAndSwap` 保证并发耗尽时只有一个调用者切换 generation；
3. generation 单调递增，索引使用 `generation % 64`，避免环形复用的 ABA 问题；
4. 每个槽位使用 `refilling` 原子状态，最多只有一个在途回填；
5. 每个槽位的 Actor 函数在初始化时创建并复用，调用路径不创建临时闭包；
6. Actor 邮箱固定为 64，和槽位数量一致；
7. `Next()` 的逐 ID 取号和槽位切换不使用互斥锁；
8. Actor 回填与 `NextBatch()` 共享同一个状态锁，保证预留区间不重叠。

### Mutex：原子取号 + 耗尽时加锁续租

Mutex 版只保存一个当前号段：

```text
读取 current lease
→ CAS 取号成功：立即返回
→ current 不存在或耗尽：获取互斥锁
→ 锁内再次比较 current，避免重复续租
→ 预留新的 64-ID 号段
→ 原子发布为 current
```

互斥锁不会进入有可用号段的热路径。`NextBatch()` 使用同一把锁直接预留新的 64-ID 区间，因此可以与 `Next()` 并发混用。

### 时间、序列和回拨处理

两个包的状态逻辑相同，但代码完全独立：

- 当前毫秒仍有空间时，从下一个序列号开始预留；
- 当前毫秒不足以容纳完整号段时，整个号段移动到下一毫秒；
- 距离下一毫秒超过 200 µs 时先休眠，接近边界后使用 `runtime.Gosched()`；
- 当前时间小于最后预留时间时返回 `ErrInvalidTimestamp`；
- Actor 已缓存的号段、Mutex 当前尚未耗尽的号段仍可继续消费，因此回拨在下一次预留时被发现。

## 性能测试结果

测试环境：

| 项目 | 值 |
| --- | --- |
| CPU | AMD Ryzen 7 4800H，8 核 16 线程 |
| OS/架构 | Windows / amd64 |
| Go | 1.27.0 |
| `GOMAXPROCS` | 16 |
| 采样 | 每项 1 秒，重复 5 次取中位数 |

### `Next()`

| 场景 | Actor | Mutex | 分配说明 |
| --- | ---: | ---: | --- |
| 当前号段缓存命中 | 5.37 ns/ID | 5.32 ns/ID | 两者调用路径均为 0 分配 |
| 1000-ID 短批，包含号段切换 | 5.98 ns/ID | 7.40 ns/ID | Actor 调用路径 0 分配；Mutex 每 1000 ID 为 384 B、16 allocs |
| 并行持续生成 | 244.0 ns/ID | 244.0 ns/ID | 均达到格式上限约 409.6 万 ID/秒 |

说明：Actor 的号段对象由后台回填创建；“调用路径 0 分配”不表示整个进程永远不分配。Mutex 的号段创建发生在负责同步续租的调用方。

### `NextBatch()`

| 场景 | Actor | Mutex | 分配 |
| --- | ---: | ---: | ---: |
| 单批 64 ID | 1.389 µs/批，21.70 ns/ID | 1.408 µs/批，21.99 ns/ID | 512 B、1 alloc/批 |
| 并行持续生成 | 244.2 ns/ID | 244.0 ns/ID | 512 B、1 alloc/批 |

### 如何理解 244 ns/ID

12 位序列号决定每个机器 ID 每毫秒最多生成 4096 个 ID：

```text
1 second / 4,096,000 IDs ≈ 244.14 ns/ID
```

因此持续基准中的约 244 ns/ID 是格式上限，不代表 CPU 取一个 ID 需要 244 ns。排除等待下一毫秒后，实际调用成本约为 5~7 ns/ID。

在 5 万 ID/秒的目标负载下，两种实现都远低于格式上限。

## 测试与验证

运行全部测试：

```bash
go test ./...
```

运行静态检查：

```bash
go vet ./...
```

Actor 代表性基准：

```bash
go test ./actor -run "^$" \
  -bench "BenchmarkActor(NextCachedHotPath|NextShortBatch|NextSustained|NextBatchSingleCost|NextBatchParallelSustained)$" \
  -benchmem -benchtime=1s -count=5
```

当前测试覆盖：

- 自定义纪元及毫秒截断；
- 机器 ID、时间戳和最大 `int64` 边界；
- 时钟回拨及跨毫秒等待；
- 并发取号唯一性；
- Actor 环形切换、断供重试和失败恢复；
- `Next()` 与 `NextBatch()` 并发混用；
- Actor 与 Mutex 独立包兼容性。

## 项目结构

```text
snowflake-id/
├── actor/   # 环形队列 + 唯一 ext.Actor
├── mutex/   # 原子取号 + 耗尽互斥锁续租
├── go.mod
└── README.md
```

项目地址：[github.com/da123wda/snowflake-id](https://github.com/da123wda/snowflake-id)
