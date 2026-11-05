package main

import (
	"os"

	"github.com/TheWor1d877/mini-Raft-KV/internal/nodeapp"
	"github.com/TheWor1d877/mini-Raft-KV/internal/raft"
)

// miniraft-kv 是单节点集群：成员表只有进程自己，自票就是多数派，
// 写路径每条一次 Sync，读默认线性一致（docs/8 §二）。
// 它是 D21 单机对照臂与 M6/T13 崩溃注入的进程载体。
func main() {
	os.Exit(nodeapp.Run("miniraft-kv", raft.LoadSingleNode, os.Args[1:]))
}
