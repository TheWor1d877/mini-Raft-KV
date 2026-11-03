package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/TheWor1d877/mini-Raft-KV/internal/kvstore"
)

// 退出码：0 收到 SIGINT/SIGTERM 正常收尾；1 打开数据目录失败、写路径失效或收尾出错；2 配置与日志起不来。
// 窗口只用在途请求上，沉默连接在收尾开始时就被关掉（2.1 §4），所以不必比 net/http 内部那 5 秒更长。
const shutdownGrace = 5 * time.Second

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	cfg, err := kvstore.Load(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "miniraft-kv: %v\n", err)
		return 2
	}
	lg, err := kvstore.NewLogger(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "miniraft-kv: log: %v\n", err)
		return 2
	}
	defer lg.Close()

	entry := kvstore.WithComponent(lg.Logger, kvstore.ComponentEntry)
	k, err := kvstore.Open(cfg, lg.Logger)
	if err != nil {
		// Open 内部已经打过 open failed；数据目录本身坏了的时候日志也写不出去，这里补一条 stderr。
		fmt.Fprintf(os.Stderr, "miniraft-kv: %v\n", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// D5：写路径失效由 HTTP 入口触发退出，读请求在此期间继续服务直到进程真的关掉。
	var writeFailed atomic.Bool
	quit := func() {
		writeFailed.Store(true)
		stop()
	}

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		entry.Error("listen failed", zap.String("addr", cfg.Addr), zap.Error(err))
		fmt.Fprintf(os.Stderr, "miniraft-kv: listen %s: %v\n", cfg.Addr, err)
		closeStore(k, entry)
		return 1
	}
	// 收尾不等「连上了但没发出第一个请求头」的连接：net/http 把它们当忙连接，
	// 要等各自满 5 秒才肯当空闲（Issue 22682），正好把整个收尾窗口烧光（2.1 §4）。
	var silentMu sync.Mutex
	silentConns := map[net.Conn]struct{}{}
	// 一条连接只在 StateNew 待着的时候才算沉默；请求头一开始读就转 StateActive，之后由 Shutdown 管。
	connState := func(c net.Conn, state http.ConnState) {
		silentMu.Lock()
		defer silentMu.Unlock()
		if state == http.StateNew {
			silentConns[c] = struct{}{}
			return
		}
		delete(silentConns, c)
	}
	srv := &http.Server{
		Handler:           kvstore.NewHTTPHandler(k, lg.Logger, quit),
		ReadHeaderTimeout: 5 * time.Second,
		ConnState:         connState,
	}
	entry.Info("server started",
		zap.String("addr", ln.Addr().String()),
		zap.String("data_dir", cfg.DataDir))

	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(ln) }()

	code := 0
	select {
	case err := <-serveDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			entry.Error("serve failed", zap.Error(err))
			code = 1
		}
	case <-ctx.Done():
	}

	// 沉默连接先关掉：它们没有请求在途，留到 Shutdown 里只会占用收尾窗口。
	silentMu.Lock()
	silentCount := len(silentConns)
	for c := range silentConns {
		_ = c.Close()
		delete(silentConns, c)
	}
	silentMu.Unlock()
	if silentCount > 0 {
		entry.Info("closed connections that never sent a request", zap.Int("count", silentCount))
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		entry.Error("http shutdown failed", zap.Error(err))
		code = 1
	}
	if err := k.Close(); err != nil {
		entry.Error("store close failed", zap.Error(err))
		code = 1
	}
	if writeFailed.Load() {
		code = 1
	}
	entry.Info("server stopped", zap.Bool("write_disabled", writeFailed.Load()), zap.Int("exit_code", code))
	return code
}

func closeStore(k *kvstore.KV, entry *zap.Logger) {
	if err := k.Close(); err != nil {
		entry.Error("store close failed", zap.Error(err))
	}
}
