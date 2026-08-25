// Package mutex 提供基于互斥锁续租的独立 Snowflake ID 生成器。
// Next 从当前 64-ID 号段原子取号，仅在号段不存在或耗尽时加锁续租。
package mutex
