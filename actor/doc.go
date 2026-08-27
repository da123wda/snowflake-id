// Package actor 提供可自定义时间戳、机器和业务位数的 int64 ID 生成与解析能力，序列固定为 12 位。
// 默认布局是 41 位时间戳、10 位机器 ID、0 位业务 ID 和 12 位序列号。
// 全局唯一性要求每个 machineID 在所有进程和生成器中被独占使用；businessID 在取号时指定。
// 调用方在构造生成器和解析 ID 时提供同一个自定义纪元。
// ActorGenerator.Next 从容量为 4096 的内部环形号段队列无锁取号，唯一后台 Actor 负责回填。
// NextBatch 一次返回固定 64 个 ID。系统时钟回拨在初始化或 Actor 回填时检测；
// 已预留的队列号段仍可供 Next 继续取号。
package actor
