// Package mutex 提供基于互斥锁续租的独立 Snowflake ID 生成器。
// 调用方在构造生成器和解析 ID 时提供同一个自定义纪元。
// Next 从当前 64-ID 号段原子取号，仅在号段不存在或耗尽时加锁续租。
package mutex
