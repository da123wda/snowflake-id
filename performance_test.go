package id

import (
	"testing"
	"time"
)

const performanceTargetPerSecond = 50_000

func TestMutexGenerates50000IDsPerSecond(t *testing.T) {
	generator := mustNewMutex(1)
	startedAt := time.Now()

	for range performanceTargetPerSecond {
		if _, err := generator.Next(); err != nil {
			t.Fatalf("Mutex Next() error: %v", err)
		}
	}

	elapsed := time.Since(startedAt)
	qps := float64(performanceTargetPerSecond) / elapsed.Seconds()
	t.Logf("Mutex 生成 %d 个 ID 耗时 %v，吞吐 %.0f ID/秒", performanceTargetPerSecond, elapsed, qps)
	if elapsed > time.Second {
		t.Fatalf("Mutex 吞吐 %.0f ID/秒，低于目标 %d ID/秒", qps, performanceTargetPerSecond)
	}
}
