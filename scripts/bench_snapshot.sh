#!/usr/bin/env bash
# 压缩前后的对照读数（M16、T35.1）：同一份负载打两档，一档全程不压缩，一档边写边压缩。
# 用法：先 go build -o bin/miniraft-kv ./cmd/miniraft-kv
#       scripts/bench_snapshot.sh [写入条数=5000] [起始端口=8951] [数据根目录=data/bench-snapshot]
# 环境变量 SNAP_ENTRIES 覆盖压缩档的触发阈值，默认取写入条数的十分之一。
# 两档的进程载体都是 bin/miniraft-kv：成员表只有自己的单节点 raft 集群（docs/8 §三），
# 写路径每条一次 Sync，快照与日志都落在这一份数据目录里。
#   no-snapshot  -snapshot-entries 0 -snapshot-bytes 0：自动触发全关（5.1 §7 触发），
#                日志从头留到尾，重启要全量回放。
#   snapshot     -snapshot-entries N/10 -snapshot-bytes 0 -snapshot-min-interval-ms 0：
#                Tick 里按条数触发，每次触发把快照点之前的前缀删掉（5.1 §5 的基线口径）。
#                写完再手动打一次 POST /raft/snapshot，把快照点推到本次负载的末尾。
# 口径：写延迟与压缩前后是同一把尺——单客户端、串行写、复用同一条连接，
#       两档拿到的是同一批 key 与 value（make_payloads 只有一处生成）。
# 每个数字的出处（5.4 T35.1 的判据）：
#   entries／snapshot_index／cost_ms 取本档写入窗口内最后一条 "snapshot created"（compact.go 打的），
#   snapshots 是这一档写入窗口内 "snapshot created" 的条数，
#   replayed_entries 取重启窗口里唯一的 "state rebuilt from wal"（node.go 打的），
#   boot_ms 是重启到就绪的墙钟：恢复链在 net.Listen 之前跑完，就绪那一刻回放已经结束，
#   所以这一栏不含选举窗口那 500~1000ms，它对应的就是 replayed_entries 那一段回放（3.1 §6）。
#   log_bytes／snap_bytes 是 kill -9 之后 raft/wal 与 raft/snapshot 两个目录的字节数。
# 每一档跑完把这几行日志原文打到 stderr，读数和原文行在同一处对得上。
# stdout 一行一档：mode writes acked p99_ms snapshots entries snapshot_index cost_ms
#                  log_bytes snap_bytes boot_ms replayed_entries。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/bench_single_vs_raft.sh
source "$ROOT/scripts/bench_single_vs_raft.sh"

N="${1:-5000}"
PORT0="${2:-8951}"
ROOT_DATA="${3:-$ROOT/data/bench-snapshot}"
KV_ADDR="127.0.0.1:$PORT0"
SNAP_DIR_REL="raft/snapshot"
MODE_LABEL=""   # 当前档名，只出现在报错里
MODE_DIR=""     # 当前档的数据目录
NODE_LOG=""     # 当前档的进程日志
PAYLOAD_DIR=""  # 负载目录，两档共用同一批 key
# 中途失败时目录里还留着进程：退出前统一收一次，不留悬挂进程占着 8951。
trap 'stop_all' EXIT
# 一档的日志窗口边界：kill -9 之前记下的行数，重启那一轮的读数全在它之后。
LOG_MARK=0

# 目录字节数：目录不存在按 0 算（未压缩那一档的 raft/snapshot 只有空目录）。
dir_bytes() { # $1 = 数据目录下的相对路径
	if [[ -d "$MODE_DIR/$1" ]]; then
		du -sb "$MODE_DIR/$1" | cut -f1
	else
		printf '0\n'
	fi
}

# 只等到 handler 就绪：恢复链在 net.Listen 之前跑完，就绪那一刻回放已经结束。
start_kv() { # $@ = 追加到命令行的节点参数
	local -a extra=("$@")
	mkdir -p "$MODE_DIR/$LOG_DIR_REL"
	# 端口段留给本脚本自己用：8901 那段是 bench_single_vs_raft.sh 的，两把尺不同时跑。
	"$BIN_KV" -data-dir "$MODE_DIR" -addr "$KV_ADDR" -log-level info \
		"${extra[@]}" >/dev/null 2>>"$MODE_DIR/$STDERR_REL" &
	KV_PID=$!
	wait_ready "$KV_ADDR" "$MODE_DIR" "$LOG_REL" || { echo "$MODE_LABEL 档起进程没就绪" >&2; return 1; }
}

# 一档的完整流程：起进程 → 写完 N 条 →（压缩档手动补一次快照）→ kill -9 → 量磁盘 → 重启量 boot_ms。
run_mode() { # $1 = 档名，$2 = 手动补快照（yes/no），$3.. = 节点参数
	local mode="$1" manual="$2"
	shift 2
	MODE_LABEL="$mode"
	MODE_DIR="$ROOT_DATA/$mode"
	NODE_LOG="$MODE_DIR/$LOG_REL"
	rm -rf "$MODE_DIR"
	mkdir -p "$MODE_DIR"

	if ! start_kv "$@"; then return 1; fi
	if ! wait_leader "$KV_ADDR"; then
		echo "$mode 档没能选出 Leader" >&2
		stop_all
		return 1
	fi

	local client
	client="$(run_client "$KV_ADDR" "$PAYLOAD_DIR")" || { echo "$mode 档的写客户端跑失败" >&2; return 1; }
	if [[ "$(printf '%s' "$client" | jq -r .acked)" != "$N" ]]; then
		echo "$mode 档只写进 $(printf '%s' "$client" | jq -r .acked) / $N 条：$(printf '%s' "$client" | jq -c .failed)" >&2
		return 1
	fi
	# 手动那一次把快照点推到负载末尾：自动触发按阈值走，末尾那一段留在日志里删不掉。
	# 写完的条数已经全部 apply（Propose 在返回前调 applyCommitted），快照点就是负载末尾。
	if [[ "$manual" == "yes" ]]; then
		local reply code
		reply="$(curl -s -m 60 -w '\n%{http_code}' -X POST "http://$KV_ADDR/raft/snapshot" || true)"
		code="${reply##*$'\n'}"
		if [[ "$code" != "200" ]]; then
			echo "$mode 档的手动快照没成：HTTP $code ${reply%$'\n'*}" >&2
			return 1
		fi
	fi

	# 读数要在 kill 之前圈定写入窗口：日志跨重启续写，这一段全在文件头到 LOG_MARK 之间。
	LOG_MARK="$(wc -l <"$NODE_LOG")"
	kill -9 "$KV_PID" 2>/dev/null
	wait "$KV_PID" 2>/dev/null || true
	KV_PID=""

	local log_bytes snap_bytes snapshots snap_line entries=0 snap_index=0 cost_ms=0
	log_bytes="$(dir_bytes "$SEG_DIR_REL")"
	snap_bytes="$(dir_bytes "$SNAP_DIR_REL")"
	snapshots="$(sed -n "1,${LOG_MARK}p" "$NODE_LOG" |
		jq -Rs --arg m 'snapshot created' 'split("\n") | map(fromjson?) | map(select(.msg == $m)) | length')"
	snap_line="$(sed -n "1,${LOG_MARK}p" "$NODE_LOG" |
		jq -Rs -c --arg m 'snapshot created' 'split("\n") | map(fromjson?) | map(select(.msg == $m)) | last // empty')"
	if [[ -n "$snap_line" ]]; then
		entries="$(printf '%s' "$snap_line" | jq -r .entries)"
		snap_index="$(printf '%s' "$snap_line" | jq -r .snapshot_index)"
		cost_ms="$(printf '%s' "$snap_line" | jq -r .cost_ms)"
	fi

	local started ended boot_ms rebuilt replayed=0
	started="$(date +%s.%N)"
	if ! start_kv "$@"; then return 1; fi
	ended="$(date +%s.%N)"
	boot_ms="$(awk -v a="$started" -v b="$ended" 'BEGIN { printf "%.1f", (b - a) * 1000 }')"
	rebuilt="$(tail -n +"$((LOG_MARK + 1))" "$NODE_LOG" |
		jq -Rs -c --arg m 'state rebuilt from wal' 'split("\n") | map(fromjson?) | map(select(.msg == $m)) | last // empty')"
	[[ -n "$rebuilt" ]] && replayed="$(printf '%s' "$rebuilt" | jq -r .replayed_entries)"

	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$mode" "$N" "$(printf '%s' "$client" | jq -r .acked)" "$(printf '%s' "$client" | jq -r .p99_ms)" \
		"$snapshots" "$entries" "$snap_index" "$cost_ms" "$log_bytes" "$snap_bytes" "$boot_ms" "$replayed"

	# 三个读数的日志原文：条数与 cost_ms、boot_ms 对应的回放各自指回这一行。
	echo "  $mode 档（$*）的日志：$NODE_LOG" >&2
	echo "    snapshot created 共 $snapshots 条，最后一条：${snap_line:-（没有）}" >&2
	echo "    state rebuilt from wal（重启窗口）：${rebuilt:-（没有）}" >&2
	echo "    raft/wal $log_bytes 字节，raft/snapshot $snap_bytes 字节：$MODE_DIR" >&2

	stop_all
	return 0
}

main() {
	local snap_entries="${SNAP_ENTRIES:-$((N / 10))}"
	if [[ ! -x "$BIN_KV" ]]; then
		echo "找不到 $BIN_KV，先跑：go build -o bin/miniraft-kv ./cmd/miniraft-kv" >&2
		exit 1
	fi
	if [[ "$snap_entries" -le 0 ]]; then
		echo "SNAP_ENTRIES=$snap_entries 必须大于 0：压缩档的阈值就是它" >&2
		exit 1
	fi

	PAYLOAD_DIR="$(mktemp -d)"
	make_payloads "$PAYLOAD_DIR"

	printf 'mode\twrites\tacked\tp99_ms\tsnapshots\tentries\tsnapshot_index\tcost_ms\tlog_bytes\tsnap_bytes\tboot_ms\treplayed_entries\n'
	if ! run_mode no-snapshot no -snapshot-entries 0 -snapshot-bytes 0; then exit 1; fi
	if ! run_mode snapshot yes -snapshot-entries "$snap_entries" -snapshot-bytes 0 -snapshot-min-interval-ms 0; then exit 1; fi

	rm -rf "$PAYLOAD_DIR"
	exit 0
}

main "$@"
