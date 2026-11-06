#!/usr/bin/env bash
# 自动降级的可用性与写延迟对照：5 个节点，停掉一个 voter，等它被降成 learner，
# 记降级前后两段写的 acked 与 p99，以及降级点上的 config index。
# 用法：go build -o bin/miniraft-node ./cmd/miniraft-node
#       scripts/demote_lagging.sh [写入条数=2000] [learner-demote-ms=3000] [起始端口=8901]
# stdout 两组一行：phase acked attempted p50_ms p99_ms voters learners；
# 每个数字的日志原文行进 stderr 的「证据」块（6.2 §2）。
set -euo pipefail

WRITES="${1:-2000}"
DEMOTE_MS="${2:-3000}"
PORT0="${3:-8901}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin/miniraft-node"
DATA="$ROOT/data/demote"
LOG_REL="log/miniraft-node.log"
NODES=5

if [[ ! -x "$BIN" ]]; then
	echo "找不到 $BIN，先跑：go build -o bin/miniraft-node ./cmd/miniraft-node" >&2
	exit 1
fi
if [[ -e "$DATA" ]]; then
	echo "数据目录 $DATA 还在，先删掉或换一个：脚本不覆盖上一次的现场" >&2
	exit 1
fi

PEERS=""
for ((i = 1; i <= NODES; i++)); do
	[[ -n "$PEERS" ]] && PEERS+=","
	PEERS+="node-$i=127.0.0.1:$((PORT0 + i - 1))"
done

PIDS=()
cleanup() {
	local pid
	for pid in "${PIDS[@]:-}"; do
		kill "$pid" 2>/dev/null || true
	done
	wait 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$DATA"
for ((i = 1; i <= NODES; i++)); do
	id="node-$i"
	dir="$DATA/$id"
	mkdir -p "$dir/log"
	"$BIN" -node-id "$id" -peers "$PEERS" -data-dir "$dir" -log-level info \
		-learner-demote-ms "$DEMOTE_MS" -snapshot-exempt-entries 100 \
		>/dev/null 2>>"$dir/log/stderr.txt" &
	PIDS+=("$!")
done

addr_of() { echo "127.0.0.1:$((PORT0 + $1 - 1))"; }

wait_ready() {
	local addr="$1" code
	for _ in $(seq 1 400); do
		code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://$addr/get?key=__probe__&consistency=local" || true)"
		[[ "$code" == "404" || "$code" == "200" ]] && return 0
		sleep 0.05
	done
	return 1
}

for ((i = 1; i <= NODES; i++)); do
	if ! wait_ready "$(addr_of "$i")"; then
		echo "node-$i 没有就绪" >&2
		exit 1
	fi
done

# 找 Leader：只有 Leader 会回 200。
leader=""
for ((i = 1; i <= NODES; i++)); do
	code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 \
		-d '{"key":"__leader_probe__","value":"v"}' "http://$(addr_of "$i")/put" || true)"
	if [[ "$code" == "200" ]]; then
		leader="node-$i"
		break
	fi
done
if [[ -z "$leader" ]]; then
	echo "找不到 Leader" >&2
	exit 1
fi
leader_addr="$(addr_of "${leader#node-}")"

# 停掉一个不是 Leader 的 voter：它的复制游标停住，提交点继续向前，落后条数越过豁免阈值。
stopped=""
for ((i = 1; i <= NODES; i++)); do
	if [[ "node-$i" != "$leader" ]]; then
		stopped="node-$i"
		kill "${PIDS[$((i - 1))]}" 2>/dev/null || true
		wait "${PIDS[$((i - 1))]}" 2>/dev/null || true
		break
	fi
done

# measure 跑一段串行写：duration_ms 大于 0 时按时间截断，保证降级前的样本落在阈值之前；
# 0 表示跑满 n 条。lat 文件每行一条写耗时，p50／p99 按它算。
measure() {
	local label="$1" duration_ms="$2" n="$3" out="$DATA/lat-$label.txt" i code started ok
	: >"$out"
	ok=0
	started=$(date +%s%3N)
	for ((i = 1; i <= n; i++)); do
		if ((duration_ms > 0)) && ((( $(date +%s%3N) - started ) >= duration_ms)); then
			break
		fi
		code="$(curl -s -o /dev/null -w '%{http_code} %{time_total}' --max-time 5 \
			-d "{\"key\":\"$label-$i\",\"value\":\"v\"}" "http://$leader_addr/put" || true)"
		set -- $code
		[[ "${1:-}" == "200" ]] && ok=$((ok + 1))
		echo "${2:-0}" >>"$out"
	done
	sort -n "$out" | awk -v label="$label" -v ok="$ok" -v n="$n" '
		{ a[NR]=$1 }
		END {
			p50=a[int((NR+1)*0.50)]; p99=a[int((NR+1)*0.99)]
			if (p99=="") p99=0
			printf "%s\t%d\t%d\t%.2f\t%.2f\n", label, ok, n, p50*1000, p99*1000
		}'
}

# 降级前：一个 voter 已经停机，写仍由 4/5 的多数派提交。
# 这一段按阈值的一半截断，样本必须落在自动降级触发之前；条数上限仍是 WRITES。
measure before "$((DEMOTE_MS / 2))" "$WRITES" | tee "$DATA/before.tsv" >&2

# 等降级点：Leader 的成员视图里出现这个 learner。
deadline_ms=$((DEMOTE_MS + 15000))
waited=0
learners=""
while ((waited < deadline_ms)); do
	learners="$(curl -s --max-time 2 "http://$leader_addr/raft/membership" | jq -r '.learners | join(",")' 2>/dev/null || true)"
	if [[ ",$learners," == *",$stopped,"* ]]; then
		break
	fi
	sleep 0.2
	waited=$((waited + 200))
done
if [[ ",$learners," != *",$stopped,"* ]]; then
	echo "在 ${deadline_ms}ms 内没有等到 $stopped 降级：learners=$learners" >&2
	exit 1
fi

# 降级后：voter 从 5 缩到 4，同一个 Leader 继续提交。
measure after 0 "$WRITES" | tee "$DATA/after.tsv" >&2

voters="$(curl -s --max-time 2 "http://$leader_addr/raft/membership" | jq -r '.voters | length' 2>/dev/null || echo 0)"
learner_count="$(curl -s --max-time 2 "http://$leader_addr/raft/membership" | jq -r '.learners | length' 2>/dev/null || echo 0)"

printf 'phase\tacked\tattempted\tp50_ms\tp99_ms\tvoters\tlearners\n'
printf 'before\t%s\t%d\t0\n' "$(cat "$DATA/before.tsv")" "$((NODES - 1))"
printf 'after\t%s\t%s\t%s\n' "$(cat "$DATA/after.tsv")" "$voters" "$learner_count"

{
	echo "证据："
	echo "  $stopped 的降级点："
	grep -h 'learner auto demotion triggered' "$DATA"/*/"$LOG_REL" 2>/dev/null | tail -1 | sed 's/^/    /'
	grep -h 'membership change committed' "$DATA"/*/"$LOG_REL" 2>/dev/null | tail -1 | sed 's/^/    /'
	echo "  Leader $leader 的提交点与视图："
	grep -h 'membership change committed' "$DATA/$leader/$LOG_REL" 2>/dev/null | tail -1 | sed 's/^/    /'
	grep -h 'learner auto demotion triggered' "$DATA/$leader/$LOG_REL" 2>/dev/null | tail -1 | sed 's/^/    /'
	echo "  membership change proposed 次数：$(grep -h 'membership change proposed' "$DATA"/*/"$LOG_REL" 2>/dev/null | wc -l)"
} >&2
