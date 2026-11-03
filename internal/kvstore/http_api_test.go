package kvstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"go.uber.org/zap/zaptest/observer"
)

type httpFixture struct {
	k       *KV
	srv     *httptest.Server
	records *observer.ObservedLogs
	quit    atomic.Int64
	// bodyRead 数服务端从请求体里真正读走的字节。客户端一侧的读数没有上界：
	// 体被写进 socket 缓冲就算「读走」了，多少能塞进去由调度决定，判据落不到那一边。
	bodyRead atomic.Int64
}

// countedBody 把每一次读的字节数记进读数的表。
type countedBody struct {
	io.ReadCloser
	n *atomic.Int64
}

func (c countedBody) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.n.Add(int64(n))
	return n, err
}

func newHTTPFixture(t *testing.T, cfg Config) *httpFixture {
	t.Helper()
	lg, records := observerLogger()
	k, err := Open(cfg, lg)
	if err != nil {
		t.Fatalf("Open = %v", err)
	}
	f := &httpFixture{k: k, records: records}
	h := NewHTTPHandler(k, lg, func() { f.quit.Add(1) })
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = countedBody{ReadCloser: r.Body, n: &f.bodyRead}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(f.srv.Close)
	t.Cleanup(func() { _ = k.Close() })
	return f
}

type httpReply struct {
	status int
	body   map[string]any
}

func (r httpReply) string(key string) string {
	v, _ := r.body[key].(string)
	return v
}

func (f *httpFixture) request(t *testing.T, method, path string, body io.Reader) httpReply {
	t.Helper()
	req, err := http.NewRequest(method, f.srv.URL+path, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := f.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	reply := httpReply{status: resp.StatusCode, body: map[string]any{}}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &reply.body); err != nil {
			t.Fatalf("body %q is not JSON: %v", data, err)
		}
	}
	return reply
}

func (f *httpFixture) put(t *testing.T, key, value string) httpReply {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"key": key, "value": value})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return f.request(t, http.MethodPost, "/put", bytes.NewReader(payload))
}

func (f *httpFixture) get(t *testing.T, key string) httpReply {
	t.Helper()
	return f.request(t, http.MethodGet, "/get?key="+url.QueryEscape(key), nil)
}

// infiniteReader 吐无限个 'x'：这条请求体要多长由 io.LimitReader 说了算，不由它说了算。
type infiniteReader struct{}

func (r infiniteReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

func TestHTTPPutGetRoundTrip(t *testing.T) {
	f := newHTTPFixture(t, kvConfig(t))

	reply := f.put(t, "k1", "v1")
	if reply.status != http.StatusOK {
		t.Fatalf("POST /put = %d %v, want 200", reply.status, reply.body)
	}
	if reply.body["ok"] != true {
		t.Errorf("put response = %v, want ok=true", reply.body)
	}
	if reply.string("req_id") == "" {
		t.Fatalf("put response has no req_id: %v", reply.body)
	}
	// req_id 必须一路带到 kv 的写日志里，否则排障时串不起一次请求（2.1 §8）。
	applied := f.records.FilterMessage("put applied").All()
	if len(applied) != 1 {
		t.Fatalf("put applied logged %d times, want 1", len(applied))
	}
	if got := applied[0].ContextMap()["req_id"]; got != reply.string("req_id") {
		t.Errorf("kv log req_id = %v, response says %v", got, reply.string("req_id"))
	}

	reply = f.get(t, "k1")
	if reply.status != http.StatusOK || reply.string("value") != "v1" || reply.string("key") != "k1" {
		t.Fatalf("GET /get = %d %v, want 200 and k1=v1", reply.status, reply.body)
	}

	// 空串 key 与空串 value 在 HTTP 层也必须能存能读，且与不存在区分。
	if reply := f.put(t, "", ""); reply.status != http.StatusOK {
		t.Fatalf("put empty pair = %d %v", reply.status, reply.body)
	}
	if reply := f.get(t, ""); reply.status != http.StatusOK || reply.string("value") != "" {
		t.Errorf("get empty key = %d %v, want 200 with an empty value", reply.status, reply.body)
	}
	if reply := f.get(t, "absent"); reply.status != http.StatusNotFound {
		t.Errorf("get absent key = %d, want 404", reply.status)
	}
}

func TestHTTPStatusMapping(t *testing.T) {
	f := newHTTPFixture(t, kvConfig(t))

	// key 不存在 → 404。
	reply := f.get(t, "absent")
	if reply.status != http.StatusNotFound {
		t.Fatalf("get absent = %d, want 404", reply.status)
	}
	if reply.string("code") != "key_not_found" || reply.string("req_id") == "" {
		t.Errorf("404 body = %v, want code key_not_found and a req_id", reply.body)
	}

	// 超限 → 413，且状态没被改动。
	reply = f.put(t, "big", strings.Repeat("v", MaxValueSize+1))
	if reply.status != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized put = %d %v, want 413", reply.status, reply.body)
	}
	if reply.string("code") != "too_large" {
		t.Errorf("413 body = %v, want code too_large", reply.body)
	}
	if reply := f.get(t, "big"); reply.status != http.StatusNotFound {
		t.Errorf("the rejected key is stored: %d %v", reply.status, reply.body)
	}

	// IO 错 → 500，带错误类别。
	if reply := f.put(t, "k1", "v1"); reply.status != http.StatusOK {
		t.Fatalf("setup put = %d %v", reply.status, reply.body)
	}
	restore := detachWALToPipe(t, f.k.wal)
	f.k.wal.cfg.BatchSize = 1
	reply = f.put(t, "k2", "v2")
	if reply.status != http.StatusInternalServerError {
		t.Fatalf("put with a broken fsync = %d %v, want 500", reply.status, reply.body)
	}
	if reply.string("code") != "io_error" {
		t.Errorf("500 body = %v, want code io_error", reply.body)
	}
	// 失效之后的读仍然可用（D5）。
	if reply := f.get(t, "k1"); reply.status != http.StatusOK {
		t.Errorf("get after the write path closed = %d %v, want the read path still serving", reply.status, reply.body)
	}
	restore()
}

// 错误类别到状态码的映射表，包含只有恢复链路才会产生的坏记录错误（2.1 §4）。
func TestStatusCodeTable(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{ErrKeyNotFound, 404, "key_not_found"},
		{ErrTooLarge, 413, "too_large"},
		{ErrIOFrom("append", 12, io.ErrClosedPipe), 500, "io_error"},
		{ErrCorruptAt(4096, "crc mismatch"), 500, "corrupt_record"},
		{ErrLockedFrom("data/LOCK", io.ErrClosedPipe), 500, "locked"},
		{fmt.Errorf("surprise: %w", io.ErrUnexpectedEOF), 500, "internal"},
	}
	for _, c := range cases {
		status, code := statusFor(c.err)
		if status != c.status || code != c.code {
			t.Errorf("statusFor(%v) = %d %q, want %d %q", c.err, status, code, c.status, c.code)
		}
	}
	offset, ok := CorruptOffset(ErrCorruptAt(4096, "crc mismatch"))
	if !ok || offset != 4096 {
		t.Errorf("CorruptOffset = %d %v, want 4096", offset, ok)
	}
	if _, ok := CorruptOffset(ErrKeyNotFound); ok {
		t.Errorf("CorruptOffset found an offset on a not-found error")
	}
}

// 非法请求体：4xx，且不改已有状态。
func TestHTTPRejectsBadBodies(t *testing.T) {
	f := newHTTPFixture(t, kvConfig(t))
	f.put(t, "keep", "me")

	cases := []struct {
		name string
		body string
	}{
		{"not json", `hello`},
		{"empty body", ``},
		{"wrong shape", `["k","v"]`},
		{"two objects", `{"key":"a","value":"b"} {"key":"c","value":"d"}`},
	}
	for _, c := range cases {
		reply := f.request(t, http.MethodPost, "/put", strings.NewReader(c.body))
		if reply.status != http.StatusBadRequest {
			t.Errorf("%s: POST /put = %d %v, want 400", c.name, reply.status, reply.body)
		}
		if reply.string("code") != "bad_request" {
			t.Errorf("%s: body = %v, want code bad_request", c.name, reply.body)
		}
	}

	// 请求体超过 entry 上限：413，且服务端不炸。
	oversized := `{"key":"huge","value":"` + strings.Repeat("x", MaxEntrySize+1024) + `"}`
	reply := f.request(t, http.MethodPost, "/put", strings.NewReader(oversized))
	if reply.status != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized body = %d %v, want 413", reply.status, reply.body)
	}
	// 上限在读体时就生效，不该走到写路径上去撞 entry 限制。
	if n := f.records.FilterMessage("put rejected").Len(); n != 0 {
		t.Errorf("the oversized body reached the kv write path %d times", n)
	}

	// 永不收尾的超限请求体：服务端读走的字节被上限卡住，整份请求不会被拉进内存。
	// 这条比上一条更严——去掉体大小上限后，服务端会读完全部字节再报坏 JSON。
	unterminated := io.MultiReader(strings.NewReader(`{"key":"huge","value":"`),
		io.LimitReader(infiniteReader{}, 24<<20))
	before := f.bodyRead.Load()
	reply = f.request(t, http.MethodPost, "/put", unterminated)
	if reply.status != http.StatusRequestEntityTooLarge || reply.string("code") != "too_large" {
		t.Errorf("unterminated oversized body = %d %v, want 413 too_large", reply.status, reply.body)
	}
	// MaxBytesReader 的口径是最多读到上限再多一个字节就报错，所以 +1 是理论上界。
	if pulled := f.bodyRead.Load() - before; pulled > MaxEntrySize+1 {
		t.Errorf("the server read %d bytes from an oversized body, want at most %d", pulled, MaxEntrySize+1)
	}

	// 上面这些都不许动到已有状态。
	if reply := f.get(t, "keep"); reply.status != http.StatusOK || reply.string("value") != "me" {
		t.Errorf("state changed by rejected requests: %d %v", reply.status, reply.body)
	}
	for _, key := range []string{"a", "c", "huge"} {
		if reply := f.get(t, key); reply.status != http.StatusNotFound {
			t.Errorf("%q was written by a rejected request: %d %v", key, reply.status, reply.body)
		}
	}
}

func TestHTTPRouteAndMethodGuards(t *testing.T) {
	f := newHTTPFixture(t, kvConfig(t))

	cases := []struct {
		method, path string
		status       int
		code         string
	}{
		{http.MethodGet, "/put", http.StatusMethodNotAllowed, "method_not_allowed"},
		{http.MethodPost, "/get", http.StatusMethodNotAllowed, "method_not_allowed"},
		{http.MethodGet, "/nope", http.StatusNotFound, "not_found"},
		{http.MethodDelete, "/put", http.StatusMethodNotAllowed, "method_not_allowed"},
		{http.MethodGet, "/get", http.StatusBadRequest, "bad_request"},
	}
	for _, c := range cases {
		reply := f.request(t, c.method, c.path, nil)
		if reply.status != c.status || reply.string("code") != c.code {
			t.Errorf("%s %s = %d %v, want %d %q", c.method, c.path, reply.status, reply.body, c.status, c.code)
		}
	}
}

// 并发请求：每个请求的 req_id 唯一，并且进了日志；序号从 1 起连续。
func TestHTTPReqIDUniqueUnderConcurrency(t *testing.T) {
	f := newHTTPFixture(t, kvConfig(t))

	const n = 64
	var wg sync.WaitGroup
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reply := f.put(t, fmt.Sprintf("k%02d", i), "v")
			if reply.status != http.StatusOK {
				t.Errorf("put %d = %d %v", i, reply.status, reply.body)
				return
			}
			ids[i] = reply.string("req_id")
		}(i)
	}
	wg.Wait()

	seen := map[string]bool{}
	for _, id := range ids {
		if id == "" {
			t.Fatalf("a response carried no req_id")
		}
		if seen[id] {
			t.Fatalf("req_id %q reused", id)
		}
		seen[id] = true
	}
	if len(seen) != n {
		t.Fatalf("%d distinct ids for %d requests", len(seen), n)
	}
	logged := f.records.FilterMessage("http request served").All()
	if len(logged) != n {
		t.Fatalf("access log has %d lines, want %d", len(logged), n)
	}
	for _, e := range logged {
		if e.ContextMap()["req_id"] == "" {
			t.Fatalf("access log line without req_id: %v", e.ContextMap())
		}
	}
}

// 写路径失效只通知入口层一次，后续请求继续返回 500 而不是重复触发退出。
func TestHTTPNotifiesShutdownOnceOnDisabledWrites(t *testing.T) {
	cfg := kvConfig(t)
	cfg.BatchSize = 1
	f := newHTTPFixture(t, cfg)

	f.put(t, "k1", "v1")
	restore := detachWALToPipe(t, f.k.wal)
	defer restore()

	for i := 0; i < 5; i++ {
		if reply := f.put(t, fmt.Sprintf("k%d", i+2), "v"); reply.status != http.StatusInternalServerError {
			t.Fatalf("put %d = %d %v, want 500", i, reply.status, reply.body)
		}
	}
	if got := f.quit.Load(); got != 1 {
		t.Errorf("quit called %d times, want exactly 1", got)
	}
	if n := f.records.FilterMessage("write path disabled, exiting").Len(); n != 1 {
		t.Errorf("exit notice logged %d times, want 1", n)
	}
}

// 入口的日志必须带 component，字段名保持小写下划线。
func TestHTTPLogFields(t *testing.T) {
	f := newHTTPFixture(t, kvConfig(t))

	f.put(t, "k1", "v1")
	f.get(t, "absent")

	served := f.records.FilterMessage("http request served").All()
	failed := f.records.FilterMessage("http request failed").All()
	if len(served) != 1 || len(failed) != 1 {
		t.Fatalf("logged %d served and %d failed lines, want one each", len(served), len(failed))
	}
	fields := failed[0].ContextMap()
	if fields["component"] != ComponentKV {
		t.Errorf("component = %v, want %q", fields["component"], ComponentKV)
	}
	if fields["route"] != "/get" || fields["status"] != int64(404) || fields["code"] != "key_not_found" {
		t.Errorf("failure fields = %v", fields)
	}
	if _, ok := fields["duration_ms"]; !ok {
		t.Errorf("no duration_ms in %v", fields)
	}
	for key := range fields {
		if strings.ToLower(key) != key {
			t.Errorf("field %q is not lowercase", key)
		}
	}
	// value 不许出现在任何日志行里。
	for _, e := range append(served, failed...) {
		line := e.Message + fmt.Sprint(e.ContextMap())
		if strings.Contains(line, "v1") {
			t.Errorf("a log line carries the raw value: %s", line)
		}
	}
}
