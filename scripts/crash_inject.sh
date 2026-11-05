#!/usr/bin/env bash
# 崩溃注入：起单节点集群 → 写 N 条 → 在第 K 条之后 kill -9 → 重启 → 对账。
# 进程载体仍是 bin/miniraft-kv，但它现在是单节点 raft 集群（docs/8 §二）：
# 成员表只有自己，起来先停在 Follower，等满一个选举截止自票当选，
# 当选那一轮在日志尾部追加一条 no-op（3.1 §4），所以第一条客户端写落在 index 2。
# 用法：先 go build -o bin/miniraft-kv ./cmd/miniraft-kv
#       scripts/crash_inject.sh [写入条数=2000] [第几条后杀=全写完] [raft-fsync=true|false] [段上限=默认]
# KEEP=1 保留数据目录并在最后打印路径（默认随退出删掉）。
#
# 输出列名与列序沿用旧版，第一格的含义变了：KV WAL 取消后 -batch-size 这个开关没了，
# 那一格改成装 -raft-fsync 的取值（true/false）。
#   batch         本次跑的 -raft-fsync（旧口径是 KV 的批次 64）
#   acked         客户端拿到 200 的写条数
#   durable       重启后从日志里恢复出来的条目数（取数见下）
#   not_durable   acked - durable；写路径每条一次 Sync 时必须等于 0
#   recovered     逐条读回且值对上的条数
#   lost          acked - recovered；必须等于 0
#   boot_ms       重启到就绪的耗时
#   segments      段文件条数
#   stale_tmp     kill -9 之后、重启之前的 *.rlog.tmp 条数
#
# durable 的取数：只读重启那一段的进程日志（日志跨重启续写，所以用行号把区间卡住），
# 口径是「恢复出的 KV 写条数」，no-op 那一帧从头到尾都不进这个数：
# "state rebuilt from raft log" 的 replayed_entries 只数解出 key/value 的条目，
# 也就是 [1, lastApplied] 那段里的写；其后 (lastApplied, commitIndex] 的补 apply
# 只数 component=kv 的 "entry applied"（raft 侧那条同名的行对每个 index 都打，含 no-op）。
# 两者相加就是日志恢复出的写条数，与 acked 同量纲，not_durable 才等于「没落盘的确认写」。
#
# 持久性边界：Leader 每条一次 Sync（-raft-fsync=true），所以 kill -9 之后已确认的写一条都不该丢，
# D6 那个「丢写落在 0..batch-1 批次窗口里」的口径随 KV WAL 一起作废。
# -raft-fsync=false 时 Sync 整条跳过，只报数不断言丢写。
#
# boot_ms 现在是日志全量回放的耗时：没有快照，它随写入条数线性涨（docs/8 §四 第 5 步才补快照）。
# 读回用 ?consistency=local：默认档是线性一致读，无 Leader 期间回 503，会把对账变成读路径的测试。
set -euo pipefail

TOTAL="${1:-2000}"
KILL_AFTER="${2:-$TOTAL}"
FSYNC="${3:-true}"
MAX_SIZE="${4:-}"

case "$FSYNC" in
	true | false) ;;
	*)
		echo "第三个参数是 -raft-fsync 的开关，只能填 true 或 false（收到 $FSYNC）" >&2
		exit 1
		;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin/miniraft-kv"
if [[ ! -x "$BIN" ]]; then
	echo "找不到 $BIN，先跑：go build -o bin/miniraft-kv ./cmd/miniraft-kv" >&2
	exit 1
fi

# 目录名与扩展名各收在这里：step 4 改名（raft/log→raft/wal、.rlog→.wal）只动下面两行。
SEG_DIR_REL="raft/log"
SEG_EXT="rlog"
NODE_LOG_REL="log/miniraft-node.log"

DATA="$(mktemp -d -t miniraft-crash-XXXXXX)"
NODE_LOG="$DATA/$NODE_LOG_REL"
SEG_DIR="$DATA/$SEG_DIR_REL"
# 端口留在临时端口区间（32768 起）之外，避免和出站连接抢同一个端口。
ADDR="127.0.0.1:$((20000 + RANDOM % 12000))"
ARGS=(-data-dir "$DATA" -addr "$ADDR" -raft-fsync="$FSYNC" -log-level debug)
[[ -n "$MAX_SIZE" ]] && ARGS+=(-max-file-size "$MAX_SIZE")

PROC=""
cleanup() {
	if [[ -n "$PROC" ]] && kill -0 "$PROC" 2>/dev/null; then
		kill -9 "$PROC" 2>/dev/null || true
	fi
	if [[ "${KEEP:-0}" != "1" ]]; then
		rm -rf "$DATA"
	fi
}
trap cleanup EXIT

wait_ready() {
	for _ in $(seq 1 400); do
		# 探活走 local 档：404 与 200 都是 handler 起来了。默认档在无 Leader 期间回 503，
		# 探活不该被领导权确认挡住（4.1 §4、D24）。
		code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://$ADDR/get?key=__probe__&consistency=local" || true)"
		if [[ "$code" == "404" || "$code" == "200" ]]; then
			return 0
		fi
		sleep 0.05
	done
	echo "进程在 $ADDR 上没有就绪" >&2
	tail -5 "$NODE_LOG" >&2 || true
	return 1
}

put() { # $1 = key，$2 = value；回 503（not_leader）就按等待循环重试，不把它算成写完
	local key="$1" value="$2" code="" attempt
	for attempt in $(seq 1 60); do
		# 超时上界 10s：单节点档的写是本地一次追加，正常在毫秒级；拿不到回执就报出去，不留悬挂进程。
		code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "http://$ADDR/put" \
			-H 'Content-Type: application/json' \
			-d "{\"key\":\"$key\",\"value\":\"$value\"}")"
		[[ "$code" != "503" ]] && break
		sleep 0.05
	done
	printf '%s' "$code"
}

acked=0
"$BIN" "${ARGS[@]}" &
PROC=$!
wait_ready

for ((i = 0; i < TOTAL; i++)); do
	key="$(printf 'k%06d' "$i")"
	value="$(printf 'v%06d' "$i")"
	code="$(put "$key" "$value")"
	if [[ "$code" != "200" ]]; then
		echo "写入 $key 停下，HTTP $code" >&2
		break
	fi
	acked=$((acked + 1))
	if [[ "$acked" -eq "$KILL_AFTER" ]]; then
		break
	fi
done

kill -9 "$PROC"
wait "$PROC" 2>/dev/null || true
PROC=""

stale_tmp=$(find "$SEG_DIR" -name "*.$SEG_EXT.tmp" 2>/dev/null | wc -l | tr -d ' ')

started=$(date +%s.%N)
"$BIN" "${ARGS[@]}" &
PROC=$!
wait_ready
ended=$(date +%s.%N)
boot_ms="$(awk -v a="$started" -v b="$ended" 'BEGIN { printf "%.1f", (b - a) * 1000 }')"

# 节点日志是 O_APPEND 续写的，两次启动的行都在同一个文件里，所以区间下界取最后一次
# state rebuilt from raft log 的行号：重启那一次的读数全在它之后。
# 取行号而不是 kill 时刻的行数：kill 落在写日志的中间时，那一行会被重启的首行接上，成不了合法 JSON。
log_mark=$(grep -n '"msg":"state rebuilt from raft log"' "$NODE_LOG" | tail -1 | cut -d: -f1)
if [[ -z "$log_mark" ]]; then
	echo "日志里没有 state rebuilt from raft log，读数取不到" >&2
	exit 1
fi
replayed="$(sed -n "${log_mark}p" "$NODE_LOG" | jq -R 'fromjson? | .replayed_entries')"
if [[ -z "$replayed" || "$replayed" == "null" ]]; then
	echo "第 $log_mark 行的 state rebuilt from raft log 解不出 replayed_entries" >&2
	sed -n "${log_mark}p" "$NODE_LOG" >&2 || true
	exit 1
fi
# (lastApplied, commitIndex] 的补 apply 逐条打 entry applied，这一行的 replayed_entries 是 [1, lastApplied] 的写。
# 只数 component=kv 的那条：raft 侧同名的行按 index 逐条打，no-op 也占一条，算进 durable 就对不上 acked。
# jq -Rs 配 fromjson? 跳过解不动的行（比如首尾被截断的半行）。
extra="$(tail -n +"$((log_mark + 1))" "$NODE_LOG" |
	jq -Rs --arg m 'entry applied' 'split("\n") | map(fromjson?) |
		map(select(.msg == $m and .component == "kv")) | length')"
durable=$((replayed + extra))
not_durable=$((acked - durable))

recovered=0
for ((i = 0; i < acked; i++)); do
	key="$(printf 'k%06d' "$i")"
	want="$(printf 'v%06d' "$i")"
	body="$(curl -s -m 5 "http://$ADDR/get?key=$key&consistency=local")"
	got="$(printf '%s' "$body" | sed -n 's/.*"value":"\([^"]*\)".*/\1/p')"
	if [[ "$got" == "$want" ]]; then
		recovered=$((recovered + 1))
	elif [[ "$recovered" -eq 0 && "$i" -gt 3 ]]; then
		# 前缀就缺说明恢复链路读错了；-raft-fsync=false 时这一格是 Sync 跳过本身，只警告。
		if [[ "$FSYNC" == "true" ]]; then
			echo "第 $i 条就没恢复，前缀已经丢了：$body" >&2
			exit 1
		fi
		echo "第 $i 条没恢复（-raft-fsync=false 下允许）：$body" >&2
	fi
done

lost=$((acked - recovered))
segments=$(find "$SEG_DIR" -name "*.$SEG_EXT" 2>/dev/null | wc -l | tr -d ' ')

printf 'batch acked durable not_durable recovered lost boot_ms segments stale_tmp\n'
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
	"$FSYNC" "$acked" "$durable" "$not_durable" "$recovered" "$lost" "$boot_ms" "$segments" "$stale_tmp"

if [[ "$acked" -eq 0 ]]; then
	# acked=0 就没什么可对账：写路径一条回执都没给（HTTP 码在上面的 stderr 里）。
	echo "acked=0：一条写都没确认，后面的读数都是空的" >&2
	exit 1
fi
if [[ "$FSYNC" == "true" ]]; then
	# 单节点档的写边界：Leader 每条一次 Sync，已确认的一条都不该留在日志之外。
	if [[ "$not_durable" -ne 0 ]]; then
		echo "未落盘 $not_durable 条不等于 0：已确认的写没进日志，与每条一次 Sync 的边界不符" >&2
		exit 1
	fi
	if [[ "$lost" -ne 0 ]]; then
		echo "丢写 $lost 条不等于 0：已确认的写重启后读不回" >&2
		exit 1
	fi
else
	echo "  -raft-fsync=false：not_durable $not_durable、lost $lost 只报数，不断言丢写" >&2
fi
if [[ "$recovered" -gt "$acked" ]]; then
	echo "恢复 $recovered 条多于确认 $acked 条，出现了未确认的记录" >&2
	exit 1
fi
if [[ "${KEEP:-0}" == "1" ]]; then
	echo "数据目录保留在 $DATA"
fi
