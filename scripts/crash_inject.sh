#!/usr/bin/env bash
# 崩溃注入：起进程 → 写 N 条 → 在第 K 条之后 kill -9 → 重启 → 对账。
# 用法：先 go build -o bin/miniraft-kv ./cmd/miniraft-kv
#       scripts/crash_inject.sh [写入条数=2000] [第几条后杀=全写完] [批次=64] [段上限=默认]
# 产出一行表格：确认条数、恢复条数、丢写条数、重启耗时、段数。丢写只应落在最近 batch-1 条里（D6）。
set -euo pipefail

TOTAL="${1:-2000}"
KILL_AFTER="${2:-$TOTAL}"
BATCH="${3:-64}"
MAX_SIZE="${4:-}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin/miniraft-kv"
if [[ ! -x "$BIN" ]]; then
	echo "找不到 $BIN，先跑：go build -o bin/miniraft-kv ./cmd/miniraft-kv" >&2
	exit 1
fi

DATA="$(mktemp -d -t miniraft-crash-XXXXXX)"
# 端口留在临时端口区间（32768 起）之外，避免和出站连接抢同一个端口。
ADDR="127.0.0.1:$((20000 + RANDOM % 12000))"
ARGS=(-data-dir "$DATA" -addr "$ADDR" -batch-size "$BATCH" -log-level debug)
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
		code="$(curl -s -o /dev/null -w '%{http_code}' "http://$ADDR/get?key=__probe__" || true)"
		if [[ "$code" == "404" || "$code" == "200" ]]; then
			return 0
		fi
		sleep 0.05
	done
	echo "进程在 $ADDR 上没有就绪" >&2
	tail -5 "$DATA/log/miniraft-kv.log" >&2 || true
	return 1
}

put() {
	curl -s -o /dev/null -w '%{http_code}' -X POST "http://$ADDR/put" \
		-H 'Content-Type: application/json' \
		-d "{\"key\":\"$1\",\"value\":\"$2\"}"
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

tmp_before=$(find "$DATA/wal" -name '*.wal.tmp' | wc -l | tr -d ' ')
# 重启会往同一份日志续写，所以「杀掉那一刻已经 Sync 了多少条」必须在重启之前取。
durable=$(grep -o '"batch_count":[0-9]*' "$DATA/log/miniraft-kv.log" | awk -F: '{s += $2} END { print s + 0 }' || true)
not_durable=$((acked - durable))

"$BIN" "${ARGS[@]}" &
PROC=$!
started=$(date +%s.%N)
wait_ready
ended=$(date +%s.%N)
boot_ms="$(awk -v a="$started" -v b="$ended" 'BEGIN { printf "%.1f", (b - a) * 1000 }')"

recovered=0
for ((i = 0; i < acked; i++)); do
	key="$(printf 'k%06d' "$i")"
	want="$(printf 'v%06d' "$i")"
	body="$(curl -s "http://$ADDR/get?key=$key")"
	got="$(printf '%s' "$body" | sed -n 's/.*"value":"\([^"]*\)".*/\1/p')"
	if [[ "$got" == "$want" ]]; then
		recovered=$((recovered + 1))
	elif [[ "$recovered" -eq 0 && "$i" -gt 3 ]]; then
		# 丢写只可能落在最近一批里；开头就缺说明恢复链路读错了。
		echo "第 $i 条就没恢复，前缀已经丢了：$body" >&2
		exit 1
	fi
done

lost=$((acked - recovered))
segments_after=$(find "$DATA/wal" -name '*.wal' | wc -l | tr -d ' ')

printf 'batch acked durable not_durable recovered lost boot_ms segments stale_tmp\n'
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
	"$BATCH" "$acked" "$durable" "$not_durable" "$recovered" "$lost" "$boot_ms" "$segments_after" "$tmp_before"

if [[ "$not_durable" -lt 0 || "$not_durable" -gt $((BATCH - 1)) ]]; then
	echo "未落盘 $not_durable 条不在批次窗口 0..$((BATCH - 1)) 内，与 D6 的口径不符" >&2
	exit 1
fi
if [[ "$lost" -gt $((BATCH - 1)) ]]; then
	echo "丢写 $lost 条超过批次窗口 $((BATCH - 1))，与 D6 的承诺不符" >&2
	exit 1
fi
if [[ "$recovered" -gt "$acked" ]]; then
	echo "恢复 $recovered 条多于确认 $acked 条，出现了未确认的记录" >&2
	exit 1
fi
if [[ "${KEEP:-0}" == "1" ]]; then
	echo "数据目录保留在 $DATA"
fi
