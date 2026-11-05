package kvstore

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 8 写 8 读跑一秒：无竞态、读到的值必属于某次已发生的 Apply、
// map 终态的每个值也都属于某次 Apply 且覆盖所有被写过的 key。
// Apply 只改内存，落盘边界在 raft 日志那条 Sync 上（docs/8 §二），对账只看 map。
func TestConcurrentWritersAndReaders(t *testing.T) {
	k, _ := openKV(t, kvConfig(t))

	const (
		writers = 8
		readers = 8
		perKey  = 4
	)
	keys := make([]string, writers*perKey)
	for w := 0; w < writers; w++ {
		for i := 0; i < perKey; i++ {
			keys[w*perKey+i] = fmt.Sprintf("key-%d-%d", w, i)
		}
	}

	var (
		deadline = time.Now().Add(time.Second)
		wg       sync.WaitGroup

		appliedMu sync.Mutex
		applied   = map[[2]string]bool{}
		appliedN  int64

		observedMu sync.Mutex
		observed   [][2]string

		bad int64
	)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				if time.Now().After(deadline) {
					return
				}
				key := keys[w*perKey+i%perKey]
				value := fmt.Sprintf("val-%d-%d", w, i)
				k.Apply(key, value)
				appliedMu.Lock()
				applied[[2]string{key, value}] = true
				appliedN++
				appliedMu.Unlock()
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; ; i++ {
				if time.Now().After(deadline) {
					return
				}
				key := keys[i%len(keys)]
				value, err := k.Get(key)
				switch {
				case err == nil:
					observedMu.Lock()
					observed = append(observed, [2]string{key, value})
					observedMu.Unlock()
				case errors.Is(err, ErrKeyNotFound):
					// key 还没被任何写者碰到，属正常控制流。
				default:
					atomic.AddInt64(&bad, 1)
					t.Errorf("reader %d Get(%q) = %v", r, key, err)
					return
				}
			}
		}(r)
	}

	wg.Wait()
	if got := atomic.LoadInt64(&bad); got != 0 {
		t.Fatalf("%d operations returned a non-sentinel error", got)
	}
	if appliedN == 0 {
		t.Fatalf("no applies in one second")
	}

	// 读侧观察到的每个值，都必须是某个写者已经执行过的 Apply。
	for _, obs := range observed {
		if !applied[obs] {
			t.Fatalf("read %q=%q, which no Apply performed", obs[0], obs[1])
		}
	}

	k.mu.RLock()
	got := make(map[string]string, len(k.data))
	for key, value := range k.data {
		got[key] = value
	}
	k.mu.RUnlock()

	// 终态的每个值都属于某次 Apply（无幻影写），且每个被写过的 key 都在（无静默丢写）。
	for key, value := range got {
		if !applied[[2]string{key, value}] {
			t.Errorf("map[%q] = %q, which no Apply performed", key, value)
		}
	}
	wantKeys := map[string]bool{}
	for pair := range applied {
		wantKeys[pair[0]] = true
	}
	if len(got) != len(wantKeys) {
		t.Fatalf("map holds %d keys, applies touched %d", len(got), len(wantKeys))
	}
	for key := range wantKeys {
		if _, ok := got[key]; !ok {
			t.Errorf("key %q was applied but is missing from the map", key)
		}
	}

	if err := k.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
}
