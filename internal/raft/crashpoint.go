package raft

import (
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// 命名停点给进程级故障注入用：MINIRAFT_CRASH_POINT 选中一个点，
// MINIRAFT_CRASH_PAUSE_MS 给出停顿毫秒数。不设或名字不匹配时不睡，
// 进程行为与没有这段代码时一致。快照三处提交点仍走 snapshot.go 的
// MINIRAFT_SNAPSHOT_PAUSE_MS，两套开关互不影响。
var (
	crashPointName = os.Getenv("MINIRAFT_CRASH_POINT")
	crashPointMS   = func() int {
		ms, err := strconv.Atoi(os.Getenv("MINIRAFT_CRASH_PAUSE_MS"))
		if err != nil || ms < 0 {
			return 0
		}
		return ms
	}()
	// crashAfterMS 让停点在进程起来一小段时间后才生效：用例先在预热期做完已确认的写，
	// 之后的目标写才停得进指定的那个窗口。
	crashAfterMS = func() int {
		ms, err := strconv.Atoi(os.Getenv("MINIRAFT_CRASH_AFTER_MS"))
		if err != nil || ms < 0 {
			return 0
		}
		return ms
	}()
	crashStart = time.Now()
	// 停点只触发一次：复制重试会让同一个窗口反复进入，反复停会把节点一直按在锁里。
	crashFired atomic.Bool
)

func crashPoint(name string) {
	if crashPointMS > 0 && crashPointName == name &&
		time.Since(crashStart) >= time.Duration(crashAfterMS)*time.Millisecond &&
		crashFired.CompareAndSwap(false, true) {
		time.Sleep(time.Duration(crashPointMS) * time.Millisecond)
	}
}
