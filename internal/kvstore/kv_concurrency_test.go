package kvstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 8 写 8 读跑一秒：无竞态、读到的值必属于某次已成功的写、写成功数等于 WAL 记录数、
// map 终态等于按序重放 WAL 的结果。四条一起构成对「先追加后改 map」这条硬约束的对账。
func TestConcurrentWritersAndReaders(t *testing.T) {
	cfg := kvConfig(t)
	k, _ := openKV(t, cfg)

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

		ackedMu sync.Mutex
		acked   = map[[2]string]bool{}
		ackedN  int64

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
				if err := k.Put(context.Background(), key, value); err != nil {
					atomic.AddInt64(&bad, 1)
					t.Errorf("writer %d Put(%q) = %v", w, key, err)
					return
				}
				ackedMu.Lock()
				acked[[2]string{key, value}] = true
				ackedN++
				ackedMu.Unlock()
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
	if ackedN == 0 {
		t.Fatalf("no successful writes in one second")
	}

	// 读侧观察到的每个值，都必须属于某次已返回成功的写。
	for _, obs := range observed {
		if !acked[obs] {
			t.Fatalf("read %q=%q, which no successful Put returned", obs[0], obs[1])
		}
	}

	// 成功写数等于落盘记录数：一次 Put 只留一条记录，不多写也不漏写。
	onDisk := recordsOnDisk(t, cfg)
	if len(onDisk) != int(ackedN) {
		t.Fatalf("wal holds %d records for %d successful puts", len(onDisk), ackedN)
	}

	// map 终态等于按序重放 WAL。
	want := map[string]string{}
	for _, rec := range onDisk {
		want[rec[0]] = rec[1]
	}
	k.mu.RLock()
	got := make(map[string]string, len(k.data))
	for key, value := range k.data {
		got[key] = value
	}
	k.mu.RUnlock()
	if len(got) != len(want) {
		t.Fatalf("map holds %d keys, replay holds %d", len(got), len(want))
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("map[%q] = %q, replay says %q", key, got[key], value)
		}
	}

	if err := k.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	reopened, _ := openKV(t, cfg)
	reopened.mu.RLock()
	defer reopened.mu.RUnlock()
	for key, value := range want {
		if reopened.data[key] != value {
			t.Errorf("after restart map[%q] = %q, want %q", key, reopened.data[key], value)
		}
	}
}

// 并发写 + 频繁换段：段名序、段内序和 map 三者对得上，重启后终态不变。
func TestConcurrentWritesAcrossManySegments(t *testing.T) {
	cfg := kvConfig(t)
	// 一条记录约 30B，64B 的段上限让每次换段都发生在写压力下。
	cfg.MaxFileSize = 64
	k, _ := openKV(t, cfg)

	const (
		writers  = 4
		perWrite = 50
	)
	var (
		wg      sync.WaitGroup
		ackedMu sync.Mutex
		acked   = map[[2]string]bool{}
		bad     int64
	)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWrite; i++ {
				key := fmt.Sprintf("key-%d-%02d", w, i%5)
				value := fmt.Sprintf("value-%d-%02d", w, i)
				if err := k.Put(context.Background(), key, value); err != nil {
					atomic.AddInt64(&bad, 1)
					t.Errorf("Put(%q,%q) = %v", key, value, err)
					return
				}
				ackedMu.Lock()
				acked[[2]string{key, value}] = true
				ackedMu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if got := atomic.LoadInt64(&bad); got != 0 {
		t.Fatalf("%d puts failed", got)
	}

	onDisk := recordsOnDisk(t, cfg)
	if len(onDisk) != writers*perWrite {
		t.Fatalf("wal holds %d records, want %d", len(onDisk), writers*perWrite)
	}
	segs, err := listSegments(cfg)
	if err != nil {
		t.Fatalf("listSegments = %v", err)
	}
	if len(segs) < 2 {
		t.Fatalf("got %d segments under a %d-byte limit, want several", len(segs), cfg.MaxFileSize)
	}
	// 段号必须连续递增，且每一段都非空——空段说明换段点了两次，记录会被读到两次或漏掉。
	for i, s := range segs {
		if s.seq != uint64(i+1) {
			t.Fatalf("segment %d is named %d, want %d", i, s.seq, i+1)
		}
		if i < len(segs)-1 && s.size == 0 {
			t.Errorf("committed segment %d is empty", s.seq)
		}
	}

	want := map[string]string{}
	for _, rec := range onDisk {
		if !acked[rec] {
			t.Fatalf("wal holds %q=%q, which no successful Put returned", rec[0], rec[1])
		}
		want[rec[0]] = rec[1]
	}
	k.mu.RLock()
	got := make(map[string]string, len(k.data))
	for key, value := range k.data {
		got[key] = value
	}
	k.mu.RUnlock()
	if len(got) != len(want) {
		t.Fatalf("map holds %d keys, replay holds %d", len(got), len(want))
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("map[%q] = %q, replay says %q", key, got[key], value)
		}
	}

	if err := k.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	reopened, _ := openKV(t, cfg)
	for key, value := range want {
		if v, err := reopened.Get(key); err != nil || v != value {
			t.Errorf("after restart %q = %q, %v, want %q", key, v, err, value)
		}
	}
}
