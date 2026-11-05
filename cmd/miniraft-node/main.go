package main

import (
	"os"

	"github.com/TheWor1d877/mini-Raft-KV/internal/nodeapp"
	"github.com/TheWor1d877/mini-Raft-KV/internal/raft"
)

// miniraft-node 是集群里的一个节点：成员表由 -peers 给，选举与心跳由外层每 -tick-ms 一次 Tick 驱动。
func main() {
	os.Exit(nodeapp.Run("miniraft-node", raft.Load, os.Args[1:]))
}
