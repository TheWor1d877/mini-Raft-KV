#!/usr/bin/env bash
# 收掉 cluster_up.sh 起的 5 个进程：按清单逐个 SIGTERM，等各自退出，最后删掉清单。
# 用法：scripts/cluster_down.sh [数据根目录=data]
# SIGTERM 走的是 2.1 §4 的正常收尾（停 tick → 关在途提案 → Flush → 落盘 → 关文件）。
# 这些进程不是本脚本的子进程（cluster_up.sh 一退出就被收养），拿不到 wait 的退出码，
# 所以退出码读日志里最后一条 node stopped 的 exit_code 字段——2.1 §8 的「入口起停」就是为此而打。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DATA="${1:-$ROOT/data}"
STATE="$ROOT_DATA/cluster.up"

if [[ ! -f "$STATE" ]]; then
	echo "没有清单 $STATE，集群本来就没在跑" >&2
	exit 0
fi

total=0
dirty=0
printf 'node_id\tpid\texit_code\n'
while read -r id pid addr; do
	[[ -z "${pid:-}" ]] && continue
	total=$((total + 1))
	if ! kill -0 "$pid" 2>/dev/null; then
		printf '%s\t%s\t%s\n' "$id" "$pid" "already-gone"
		dirty=$((dirty + 1))
		continue
	fi
	kill -TERM "$pid"
	# 收尾窗口本身是 5 秒（shutdownGrace），15 秒的余量留给 fsync 与慢盘。
	for _ in $(seq 1 300); do
		kill -0 "$pid" 2>/dev/null || break
		sleep 0.05
	done
	if kill -0 "$pid" 2>/dev/null; then
		echo "node $id 在 15 秒内没退出，kill -9" >&2
		kill -9 "$pid"
		printf '%s\t%s\t%s\n' "$id" "$pid" "killed"
		dirty=$((dirty + 1))
		continue
	fi
	code="$(grep -o '"exit_code":[0-9-]*' "$ROOT_DATA/$id/log/miniraft-node.log" | tail -1 | cut -d: -f2)"
	if [[ -z "$code" ]]; then
		printf '%s\t%s\t%s\n' "$id" "$pid" "no-exit-record"
		dirty=$((dirty + 1))
		continue
	fi
	printf '%s\t%s\t%s\n' "$id" "$pid" "$code"
	[[ "$code" -ne 0 ]] && dirty=$((dirty + 1))
done <"$STATE"

rm -f "$STATE"
if [[ "$dirty" -gt 0 ]]; then
	echo "$dirty/$total 个节点的收尾不干净" >&2
	exit 1
fi
