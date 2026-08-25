package id

import (
	"errors"
	"runtime"
	"testing"
	"time"
)

const performanceTargetPerSecond = 50_000

func TestMutexGenerates50000IDsPerSecond(t *testing.T) {
	generator := mustNewMutex(t, 1)
	startedAt := time.Now()

	for generated := 0; generated < performanceTargetPerSecond; {
		if _, err := generator.Next(); errors.Is(err, ErrLeaseUnavailable) {
			runtime.Gosched()
			continue
		} else if err != nil {
			t.Fatalf("Mutex Next() error: %v", err)
		}
		generated++
	}

	elapsed := time.Since(startedAt)
	qps := float64(performanceTargetPerSecond) / elapsed.Seconds()
	t.Logf("Mutex 生成 %d 个 ID 耗时 %v，吞吐 %.0f ID/秒", performanceTargetPerSecond, elapsed, qps)
	if elapsed > time.Second {
		t.Fatalf("Mutex 吞吐 %.0f ID/秒，低于目标 %d ID/秒", qps, performanceTargetPerSecond)
	}
}
