#!/usr/bin/env bash
# 起一套 5 节点的本地集群：成员表按「起始端口 + 序号」生成，一条命令把 5 个进程拉起来并逐个探活。
# 用法：先 go build -o bin/miniraft-node ./cmd/miniraft-node
#       scripts/cluster_up.sh [起始端口=8801] [数据根目录=data] [日志级别=info]
# 产出：每个节点一行 node_id pid addr，同时写进 <数据根目录>/cluster.up；
#       cluster_down.sh 与 leader_failover.sh 都读这份清单，所以别手工删它。
set -euo pipefail

PORT0="${1:-8801}"
LEVEL="${3:-info}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DATA="${2:-$ROOT/data}"
BIN="$ROOT/bin/miniraft-node"
# 进程日志与 stderr 的落点只有这一处口径（3.1 §6 末段：一个进程一份 miniraft-node.log）。
LOG_DIR_REL="log"
LOG_REL="$LOG_DIR_REL/miniraft-node.log"
STDERR_REL="$LOG_DIR_REL/stderr.txt"
if [[ ! -x "$BIN" ]]; then
	echo "找不到 $BIN，先跑：go build -o bin/miniraft-node ./cmd/miniraft-node" >&2
	exit 1
fi

STATE="$ROOT_DATA/cluster.up"
if [[ -f "$STATE" ]]; then
	alive=0
	while read -r id pid addr; do
		[[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null && alive=$((alive + 1))
	done <"$STATE"
	if [[ "$alive" -gt 0 ]]; then
		echo "已有 $alive 个节点在跑（清单 $STATE），先跑 scripts/cluster_down.sh" >&2
		exit 1
	fi
	rm -f "$STATE"
fi

# 成员清单只有这一处：5 项、id 固定 node-1..node-5、地址按端口递增，5 个进程拿到的是同一串。
NODES=5
PEERS=""
for ((i = 1; i <= NODES; i++)); do
	[[ -n "$PEERS" ]] && PEERS+=","
	PEERS+="node-$i=127.0.0.1:$((PORT0 + i - 1))"
done

mkdir -p "$ROOT_DATA"
: >"$STATE"

wait_ready() {
	local addr="$1" dir="$2" code
	for _ in $(seq 1 400); do
		code="$(curl -s -o /dev/null -w '%{http_code}' "http://$addr/get?key=__probe__&consistency=local" || true)"
		# 探活走 local 档：404 与 200 都是 handler 起来了。默认档在无 Leader 期间回 503，
		# 那一档要的是领导权确认，探活不该被它挡住（4.1 §4、D24）。
		[[ "$code" == "404" || "$code" == "200" ]] && return 0
		sleep 0.05
	done
	echo "node 在 $addr 上没有就绪" >&2
	tail -5 "$dir/$LOG_REL" >&2 || true
	return 1
}

printf 'node_id\tpid\taddr\n'
for ((i = 1; i <= NODES; i++)); do
	id="node-$i"
	addr="127.0.0.1:$((PORT0 + i - 1))"
	dir="$ROOT_DATA/$id"
	mkdir -p "$dir/$LOG_DIR_REL"
	# 节点的运行日志全在 miniraft-node.log 里，stdout 必须丢掉：留着管道口，调用方的 `| tail` 之类的就永远等不到 EOF。
	# stderr 单独收进 log/stderr.txt：panic 和 Go runtime 的 fatal 只走 stderr，丢了就没有现场。
	"$BIN" -node-id "$id" -peers "$PEERS" -data-dir "$dir" -log-level "$LEVEL" >/dev/null 2>>"$dir/$STDERR_REL" &
	pid=$!
	if ! wait_ready "$addr" "$dir"; then
		# 已经起来的节点留在这儿，清单里也留着：排查完用同一条 cluster_down.sh 收。
		echo "第 $i 个节点没起来，其余进程与清单保留" >&2
		exit 1
	fi
	printf '%s\t%s\t%s\n' "$id" "$pid" "$addr" >>"$STATE"
	printf '%s\t%s\t%s\n' "$id" "$pid" "$addr"
done

echo "成员表：$PEERS" >&2
echo "清单：$STATE" >&2
