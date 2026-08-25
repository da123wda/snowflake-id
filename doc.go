// Package id 提供基于时间戳、10 位机器 ID 和 12 位序列号的 int64 ID 生成与解析能力。
// 全局唯一性要求每个 machineID 在所有进程和生成器中被独占使用。
// MutexGenerator.Next 会在内部自动预留和续租号段，NextBatch 一次返回固定 64 个 ID。
// 系统时钟回拨在初次预留或号段续租时检测；已预留的当前号段仍可供 Next 继续取号。
package id
