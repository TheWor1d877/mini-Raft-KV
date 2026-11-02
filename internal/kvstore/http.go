package kvstore

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// HTTP JSON 入口：只做参数校验、错误映射和 req_id 生成，业务判断一律留在 kv（D10）。
type httpHandler struct {
	k  *KV
	lg *zap.Logger
	// 进程内自增，保证同一份日志里 req_id 不重复；跨进程靠各自的日志文件分开。
	seq      atomic.Uint64
	quit     func()
	quitting atomic.Bool
}

// onWriteDisabled 在写路径因 fsync 失败永久关闭后被调用一次，由入口层结束进程（D5）。
func NewHTTPHandler(k *KV, lg *zap.Logger, onWriteDisabled func()) http.Handler {
	return &httpHandler{k: k, lg: WithComponent(lg, ComponentKV), quit: onWriteDisabled}
}

const (
	routePut = "/put"
	routeGet = "/get"
)

func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqID := "req-" + strconv.FormatUint(h.seq.Add(1), 10)
	ctx := WithRequestID(r.Context(), reqID)

	switch {
	case r.URL.Path == routePut && r.Method == http.MethodPost:
		h.handlePut(ctx, w, r, reqID, start)
	case r.URL.Path == routeGet && r.Method == http.MethodGet:
		h.handleGet(w, r, reqID, start)
	case r.URL.Path == routePut || r.URL.Path == routeGet:
		h.fail(w, r.URL.Path, reqID, http.StatusMethodNotAllowed,
			"method_not_allowed", "only POST /put and GET /get are served", 0, start)
	default:
		h.fail(w, r.URL.Path, reqID, http.StatusNotFound,
			"not_found", "unknown route", 0, start)
	}
}

type putRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type getResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	ReqID string `json:"req_id"`
}

type errorResponse struct {
	Error  string `json:"error"`
	Code   string `json:"code"`
	ReqID  string `json:"req_id"`
	Offset int64  `json:"offset,omitempty"`
}

func (h *httpHandler) handlePut(ctx context.Context, w http.ResponseWriter, r *http.Request, reqID string, start time.Time) {
	var body putRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxEntrySize))
	if err := dec.Decode(&body); err != nil {
		var over *http.MaxBytesError
		if errors.As(err, &over) {
			h.fail(w, routePut, reqID, http.StatusRequestEntityTooLarge,
				"too_large", "request body over the entry limit", 0, start)
			return
		}
		h.fail(w, routePut, reqID, http.StatusBadRequest,
			"bad_request", "body must be one JSON object with key and value", 0, start)
		return
	}
	// 第二个对象说明客户端把多条写塞进一个请求；直接拒掉，不能只取第一条。
	if err := dec.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		h.fail(w, routePut, reqID, http.StatusBadRequest,
			"bad_request", "body must hold exactly one JSON object", 0, start)
		return
	}

	err := h.k.Put(ctx, body.Key, body.Value)
	if err != nil {
		if errors.Is(err, ErrIO) && h.k.WriteDisabled() && h.quitting.CompareAndSwap(false, true) {
			// D5：写路径已经永久关闭，进程留着只会继续返回错误，交给入口层退出。
			h.lg.Error("write path disabled, exiting", zap.String("req_id", reqID))
			if h.quit != nil {
				go h.quit()
			}
		}
		h.failWithError(w, routePut, reqID, err, start)
		return
	}
	h.ok(w, routePut, reqID, map[string]any{"ok": true, "req_id": reqID}, start)
}

func (h *httpHandler) handleGet(w http.ResponseWriter, r *http.Request, reqID string, start time.Time) {
	key := r.URL.Query().Get("key")
	if !r.URL.Query().Has("key") {
		h.fail(w, routeGet, reqID, http.StatusBadRequest,
			"bad_request", "query parameter key is required", 0, start)
		return
	}
	value, err := h.k.Get(key)
	if err != nil {
		h.failWithError(w, routeGet, reqID, err, start)
		return
	}
	h.ok(w, routeGet, reqID, getResponse{Key: key, Value: value, ReqID: reqID}, start)
}

// 状态码映射见 2.1 §4，只按哨兵错误判，不看错误文本。
func (h *httpHandler) failWithError(w http.ResponseWriter, route, reqID string, err error, start time.Time) {
	status, code := statusFor(err)
	offset, _ := CorruptOffset(err)
	h.fail(w, route, reqID, status, code, err.Error(), offset, start)
}

func statusFor(err error) (int, string) {
	switch {
	case errors.Is(err, ErrKeyNotFound):
		return http.StatusNotFound, "key_not_found"
	case errors.Is(err, ErrTooLarge):
		return http.StatusRequestEntityTooLarge, "too_large"
	case errors.Is(err, ErrCorruptRecord):
		return http.StatusInternalServerError, "corrupt_record"
	case errors.Is(err, ErrIO):
		return http.StatusInternalServerError, "io_error"
	case errors.Is(err, ErrLocked):
		return http.StatusInternalServerError, "locked"
	default:
		return http.StatusInternalServerError, "internal"
	}
}

func (h *httpHandler) fail(w http.ResponseWriter, route, reqID string, status int, code, detail string, offset int64, start time.Time) {
	h.lg.Warn("http request failed",
		zap.String("req_id", reqID),
		zap.String("route", route),
		zap.Int("status", status),
		zap.String("code", code),
		zap.Int64("duration_ms", durationMS(time.Since(start))))
	writeJSON(w, status, errorResponse{Error: detail, Code: code, ReqID: reqID, Offset: offset})
}

func (h *httpHandler) ok(w http.ResponseWriter, route, reqID string, payload any, start time.Time) {
	h.lg.Debug("http request served",
		zap.String("req_id", reqID),
		zap.String("route", route),
		zap.Int("status", http.StatusOK),
		zap.Int64("duration_ms", durationMS(time.Since(start))))
	writeJSON(w, http.StatusOK, payload)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// 响应已经写了头，编码失败只能截断在这一条上，没有别的兜底位置。
	_ = json.NewEncoder(w).Encode(payload)
}
