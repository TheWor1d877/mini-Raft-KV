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
	"github.com/TheWor1d877/mini-Raft-KV/internal/raft"
)

// 退出码沿用 2.1 §4：0 收到 SIGINT/SIGTERM 正常收尾；1 打不开数据目录、写路径失效、
// serve 或收尾出错；2 配置非法或日志起不来。
// 收尾窗口只用在途请求上：沉默连接一进来就被关掉，所以不必比 net/http 内部那 5 秒更长。
const shutdownGrace = 5 * time.Second

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	cfg, kcfg, err := raft.Load(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "miniraft-node: %v\n", err)
		return 2
	}
	lg, err := raft.NewNodeLogger(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "miniraft-node: log: %v\n", err)
		return 2
	}
	defer lg.Close()

	entry := kvstore.WithComponent(lg.Logger, kvstore.ComponentEntry)
	kv, err := kvstore.Open(kcfg, lg.Logger)
	if err != nil {
		// Open 内部已经打过 open failed；数据目录本身坏了的时候日志也写不出去，这里补一条 stderr。
		fmt.Fprintf(os.Stderr, "miniraft-node: %v\n", err)
		return 1
	}

	n, err := raft.NewNode(cfg, kv, raft.NewTransport(cfg), lg.Logger)
	if err != nil {
		entry.Error("node start failed",
			zap.String("node_id", cfg.NodeID), zap.String("data_dir", cfg.DataDir), zap.Error(err))
		fmt.Fprintf(os.Stderr, "miniraft-node: %v\n", err)
		closeKV(kv, entry)
		return 1
	}
	if err := n.Start(); err != nil {
		entry.Error("node start failed", zap.String("node_id", cfg.NodeID), zap.Error(err))
		fmt.Fprintf(os.Stderr, "miniraft-node: %v\n", err)
		_ = n.Close()
		closeKV(kv, entry)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// D5：raft 日志的写路径失效由 handler 触发退出，读请求在此期间继续服务直到进程真的关掉。
	var writeFailed atomic.Bool
	quit := func() {
		writeFailed.Store(true)
		stop()
	}

	ln, err := net.Listen("tcp", kcfg.Addr)
	if err != nil {
		entry.Error("listen failed", zap.String("addr", kcfg.Addr), zap.Error(err))
		fmt.Fprintf(os.Stderr, "miniraft-node: listen %s: %v\n", kcfg.Addr, err)
		closeNode(n, entry)
		closeKV(kv, entry)
		return 1
	}
	// net/http 把「连上了但没发出第一个请求头」的连接当忙连接，要等各自满 5 秒才肯当空闲，
	// 正好把整个收尾窗口烧光（2.1 §4），所以收尾时先把它们关掉。
	var silentMu sync.Mutex
	silentConns := map[net.Conn]struct{}{}
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
		Handler:           raft.NewHandler(n, lg.Logger, quit),
		ReadHeaderTimeout: 5 * time.Second,
		ConnState:         connState,
	}

	// Node 自己不读墙上时间：选举截止与心跳周期全靠这里递过去的每一次 Tick（D16、D23）。
	tick := time.NewTicker(time.Duration(cfg.TickMS) * time.Millisecond)
	defer tick.Stop()
	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		for {
			select {
			case <-tick.C:
				n.Tick()
			case <-ctx.Done():
				return
			}
		}
	}()

	entry.Info("node started",
		zap.String("node_id", cfg.NodeID),
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

	// 先停 tick：Node.Close 之后再来一次 Tick 只会往已经关掉的日志上写。
	tick.Stop()
	<-tickDone

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
	// 顺序是 3.1 §4 的 Close 契约：关在途提案 → Flush KV → 落 state → 关 raft 日志，最后才是 KV 自己的收尾。
	if err := n.Close(); err != nil {
		entry.Error("node close failed", zap.Error(err))
		code = 1
	}
	if err := kv.Close(); err != nil {
		entry.Error("store close failed", zap.Error(err))
		code = 1
	}
	if writeFailed.Load() {
		code = 1
	}
	entry.Info("node stopped",
		zap.String("node_id", cfg.NodeID),
		zap.Bool("write_disabled", writeFailed.Load()),
		zap.Int("exit_code", code))
	return code
}

func closeNode(n *raft.Node, entry *zap.Logger) {
	if err := n.Close(); err != nil {
		entry.Error("node close failed", zap.Error(err))
	}
}

func closeKV(kv *kvstore.KV, entry *zap.Logger) {
	if err := kv.Close(); err != nil {
		entry.Error("store close failed", zap.Error(err))
	}
}
