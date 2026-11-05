#!/usr/bin/env bash
# 单节点集群与 5 节点集群的写吞吐对照：同一份负载打三档，D19（每条 Sync）与单机开销的差值从这里算。
# 用法：scripts/bench_single_vs_raft.sh [写入条数=2000] [起始端口=8901] [数据根目录=data/bench]
# 三档：single-node-fsync 单进程 miniraft-kv，即单节点 raft 集群：成员表只有自己，
#                       等满一个选举截止自票当选，写路径每条一次 Sync（docs/8 §三）
#       raft-fsync        5 进程 miniraft-node，每条日志 Sync
#       raft-nofsync      同上但 -raft-fsync=false：只跳过日志的 Sync
# 档 1 跑的是 raft 的写路径，一帧的字节数是 8 字节帧头 + op/term/index 17B + command（3.1 §5）。
# D21 的对照读数在三档上重跑，本脚本不抄数字进来。
# 口径：单客户端、串行写、一条一个 HTTP 请求、复用同一条连接（curl 每请求起进程的话，
#       进程开销会盖掉 fsync 的差，对照就不成立了）。
# 每档用全新的数据目录，互不继承；stdout 每档一行 mode writes acked elapsed_ms ops_per_sec p50_ms p99_ms。
# 本文件同时是 scripts/bench_read_modes.sh 的公共库：被 source 时只定义函数、不跑主流程，
# 所以 start_raft_cluster、make_payloads、stop_all 这些只写一份。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# 目录名与扩展名各收在这里：一份日志只有这一处口径（docs/8 §一）。
SEG_DIR_REL="raft/wal"
SEG_EXT="wal"
# 进程日志与 stderr 的落点只有这一处口径（3.1 §6 末段：一个进程一份 miniraft-node.log）。
LOG_DIR_REL="log"
LOG_REL="$LOG_DIR_REL/miniraft-node.log"
STDERR_REL="$LOG_DIR_REL/stderr.txt"
KV_VAL_LEN=128
BIN_KV="$ROOT/bin/miniraft-kv"
BIN_NODE="$ROOT/bin/miniraft-node"

# 下面三个是本脚本三档的运行参数。被 source 时（bench_read_modes.sh）由那边覆盖，
# 这里的默认值只在直接执行本脚本时生效。
N="${N:-2000}"
PORT0="${PORT0:-8901}"
ROOT_DATA="${ROOT_DATA:-$ROOT/data/bench}"
RAFT_PIDS=()
KV_PID=""

# 负载只用一处生成：三档拿到的是同样的 key 与 value 长度。
make_payloads() { # $1 = 输出目录
	python3 - "$1" "$N" "$KV_VAL_LEN" <<'PY'
import os, sys
out, n, vlen = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
for i in range(1, n + 1):
    body = '{"key":"bench-%06d","value":"%s"}' % (i, "v" * vlen)
    with open(os.path.join(out, "%06d.json" % i), "w") as f:
        f.write(body)
PY
}

# 一个连接上串行打完 N 条，返回 acked／总耗时／p50／p99。
run_client() { # $1 = host:port，$2 = payload 目录
	python3 - "$1" "$2" <<'PY'
import glob, http.client, json, os, sys, time
addr, payloads = sys.argv[1], sys.argv[2]
host, port = addr.split(":")
files = sorted(glob.glob(os.path.join(payloads, "*.json")))
conn = http.client.HTTPConnection(host, int(port), timeout=30)
lat, acked, failed = [], 0, []
t0 = time.perf_counter()
for path in files:
    body = open(path, "rb").read()
    s = time.perf_counter()
    conn.request("POST", "/put", body=body, headers={"Content-Type": "application/json"})
    resp = conn.getresponse()
    resp.read()
    lat.append((time.perf_counter() - s) * 1000)
    if resp.status == 200:
        acked += 1
    else:
        failed.append(resp.status)
elapsed = (time.perf_counter() - t0) * 1000
lat.sort()
p50 = lat[len(lat) // 2]
p99 = lat[min(len(lat) - 1, int(len(lat) * 0.99))]
print(json.dumps({"acked": acked, "failed": sorted(set(failed)),
                  "elapsed_ms": round(elapsed, 1),
                  "ops_per_sec": round(acked * 1000 / elapsed, 1) if elapsed else 0,
                  "p50_ms": round(p50, 2), "p99_ms": round(p99, 2)}))
PY
}

wait_ready() { # $1 = addr，$2 = 数据目录，$3 = 日志相对路径
	local addr="$1" dir="$2" rel="$3" code
	for _ in $(seq 1 600); do
		# 探活显式带 consistency=local：默认档的 /get 在没有 Leader 期间回 503，
		# 探活一直失败就起不来集群了；local 档在任意节点上都是 404/200 这个形状（D30）。
		code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://$addr/get?key=__probe__&consistency=local" || true)"
		[[ "$code" == "404" || "$code" == "200" ]] && return 0
		sleep 0.05
	done
	echo "$addr 没有就绪" >&2
	tail -5 "$dir/$rel" >&2 || true
	return 1
}

# 等本节点真的当上 Leader：成员表只有自己时多数派是 1，但进程起来先停在 Follower，
# 要等满一个选举截止（[T,2T) = 500~1000ms）才自票当选（docs/8 §三）。
# 判据用缺省档的 /get：Follower 一律 503，Leader 上这个 key 不存在是 404（D24）。
# 不等这一条，客户端头几百个请求会成串拿 503，档位对照就变成在测选举窗口。
wait_leader() { # $1 = addr
	local addr="$1" code
	for _ in $(seq 1 600); do
		code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "http://$addr/get?key=__probe__" || true)"
		[[ "$code" == "404" || "$code" == "200" ]] && return 0
		sleep 0.05
	done
	return 1
}

# 现任 Leader：各节点最后一条 role changed 里 term 最大的那个 leader。
find_leader() { # $1 = 集群根目录，$2 = 节点 id 列表（空格分隔）
	local base="$1" id line ts term role best_term=-1 best_ns=-1 best_id="" ns
	for id in $2; do
		# 窗口取 2000 行：debug 级别下心跳与 RPC 的日志会把 role changed 挤出 400 行。
		line="$(tail -n 2000 "$base/$id/$LOG_REL" 2>/dev/null |
			jq -r 'select(.msg=="role changed") | [.ts, (.term|tostring), .new_role] | @tsv' | tail -1)"
		[[ -z "$line" ]] && continue
		ts="$(printf '%s' "$line" | cut -f1)"
		term="$(printf '%s' "$line" | cut -f2)"
		role="${line##*$'\t'}"
		[[ "$role" == "leader" ]] || continue
		ns="$(date -d "$ts" +%s%N)"
		if ((term > best_term)) || { ((term == best_term)) && ((ns > best_ns)); }; then
			best_term=$term best_ns="$ns" best_id="$id"
		fi
	done
	[[ -n "$best_id" ]] || return 1
	printf '%s\n' "$best_id"
}

start_raft_cluster() { # $1 = 目录前缀，$2 = fsync 参数（空或 -raft-fsync=false），$3 = 追加到 5 个进程命令行尾部的节点参数（一组词，可空）
	local prefix="$1" fsync_flag="$2" node_args="$3" peers="" i id addr dir
	local extra=()
	# 第三参数是一组词（例如 "-read-mode readindex"），拆成独立的 argv 追加到命令行尾部。
	if [[ -n "$node_args" ]]; then
		read -r -a extra <<<"$node_args"
	fi
	for ((i = 1; i <= 5; i++)); do
		[[ -n "$peers" ]] && peers+=","
		peers+="node-$i=127.0.0.1:$((PORT0 + i - 1))"
	done
	for ((i = 1; i <= 5; i++)); do
		id="node-$i"
		addr="127.0.0.1:$((PORT0 + i - 1))"
		dir="$ROOT_DATA/$prefix/$id"
		mkdir -p "$dir/$LOG_DIR_REL"
		# 每档自己的端口段，8801 上那套 cluster_up.sh 起的集群不受影响。
		"$BIN_NODE" -node-id "$id" -peers "$peers" -data-dir "$dir" -log-level info \
			$fsync_flag ${extra[@]+"${extra[@]}"} >/dev/null 2>>"$dir/$STDERR_REL" &
		RAFT_PIDS+=("$!")
		wait_ready "$addr" "$dir" "$LOG_REL" || return 1
	done
	# 选举窗口 [T,2T) = 500~1000ms，多等一轮重选的余量。
	for _ in $(seq 1 60); do
		if LEADER_ID="$(find_leader "$ROOT_DATA/$prefix" "node-1 node-2 node-3 node-4 node-5")"; then
			LEADER_ADDR="127.0.0.1:$((PORT0 + ${LEADER_ID#node-} - 1))"
			return 0
		fi
		sleep 0.05
	done
	return 1
}

stop_all() {
	local pid
	for pid in "${RAFT_PIDS[@]}" "${KV_PID:-}"; do
		[[ -n "$pid" ]] && kill -TERM "$pid" 2>/dev/null
	done
	# 三档复用同一段端口，必须等真退出：下一档 bind 不上就是 already in use 退出码 1。
	for pid in "${RAFT_PIDS[@]}" "${KV_PID:-}"; do
		[[ -z "$pid" ]] && continue
		for _ in $(seq 1 300); do
			kill -0 "$pid" 2>/dev/null || break
			sleep 0.05
		done
		kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
	done
	RAFT_PIDS=()
	KV_PID=""
}

report() { # $1 = mode，$2 = json，$3 = 这一档 Leader 的 WAL 字节
	local acked elapsed ops p50 p99
	acked="$(printf '%s' "$2" | jq -r .acked)"
	elapsed="$(printf '%s' "$2" | jq -r .elapsed_ms)"
	ops="$(printf '%s' "$2" | jq -r .ops_per_sec)"
	p50="$(printf '%s' "$2" | jq -r .p50_ms)"
	p99="$(printf '%s' "$2" | jq -r .p99_ms)"
	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$N" "$acked" "$elapsed" "$ops" "$p50" "$p99" "$3"
	if [[ "$(printf '%s' "$2" | jq -r '.failed | length')" != "0" ]]; then
		echo "  $1 档有非 200 响应：$(printf '%s' "$2" | jq -c .failed)（acked $acked / $N）" >&2
	fi
}

# 这一档的日志落了多少字节：段目录里没有 *.$SEG_EXT 就是没落盘，算跑失败。
# wal_bytes 只有一栏：一份日志就是 raft/wal，写路径不再有第二份要计的落盘对象。
seg_bytes() { # $1 = 节点数据目录
	local dir="$1" n
	n="$(find "$dir/$SEG_DIR_REL" -name "*.$SEG_EXT" 2>/dev/null | wc -l | tr -d ' ')"
	if [[ "$n" == "0" ]]; then
		echo "档 $dir 的 $SEG_DIR_REL 里没有 *.$SEG_EXT 段文件" >&2
		return 1
	fi
	du -sb "$dir/$SEG_DIR_REL" | cut -f1
}

main() {
	local b mode fsync_flag out wal_bytes payload_dir KV_ADDR SINGLE_MODE
	# 这三个必须是全局：make_payloads／start_raft_cluster／report 都从全局取，
	# 被 source 的 bench_read_modes.sh 用的也是同一套全局。
	N="${1:-2000}"
	PORT0="${2:-8901}"
	ROOT_DATA="${3:-$ROOT/data/bench}"
	SINGLE_MODE="single-node-fsync"

	for b in "$BIN_KV" "$BIN_NODE"; do
		if [[ ! -x "$b" ]]; then
			echo "找不到 $b，先跑：go build -o bin/miniraft-kv ./cmd/miniraft-kv && go build -o bin/miniraft-node ./cmd/miniraft-node" >&2
			exit 1
		fi
	done

	payload_dir="$(mktemp -d)"
	make_payloads "$payload_dir"

	printf 'mode\twrites\tacked\telapsed_ms\tops_per_sec\tp50_ms\tp99_ms\twal_bytes\n'

	# 档 1：单节点 raft 集群（bin/miniraft-kv 的进程载体），成员表只有自己，
	# 自票当选后写路径每条一次 Sync。单节点档 -node-id 与 -peers 可省，只给 -addr。
	KV_ADDR="127.0.0.1:$((PORT0 + 9))"
	rm -rf "$ROOT_DATA/$SINGLE_MODE"
	mkdir -p "$ROOT_DATA/$SINGLE_MODE/$LOG_DIR_REL"
	"$BIN_KV" -data-dir "$ROOT_DATA/$SINGLE_MODE" -addr "$KV_ADDR" -log-level info \
		>/dev/null 2>>"$ROOT_DATA/$SINGLE_MODE/$STDERR_REL" &
	KV_PID=$!
	if ! wait_ready "$KV_ADDR" "$ROOT_DATA/$SINGLE_MODE" "$LOG_REL" || ! wait_leader "$KV_ADDR"; then
		echo "$SINGLE_MODE 档没能选出 Leader" >&2
		stop_all
		rm -rf "$payload_dir"
		exit 1
	fi
	out="$(run_client "$KV_ADDR" "$payload_dir")" || { echo "$SINGLE_MODE 档跑失败" >&2; stop_all; exit 1; }
	wal_bytes="$(seg_bytes "$ROOT_DATA/$SINGLE_MODE")" || { stop_all; rm -rf "$payload_dir"; exit 1; }
	report "$SINGLE_MODE" "$out" "$wal_bytes"
	stop_all

	# 档 2 与档 3：5 节点集群，端口段复用，数据目录各自一份。
	for mode in raft-fsync raft-nofsync; do
		fsync_flag=""
		[[ "$mode" == "raft-nofsync" ]] && fsync_flag="-raft-fsync=false"
		rm -rf "$ROOT_DATA/$mode"
		mkdir -p "$ROOT_DATA/$mode"
		# 第三个位置参数本脚本自己的三档传空：行为与改动前完全一致。
		if ! start_raft_cluster "$mode" "$fsync_flag" ""; then
			echo "$mode 档起集群或选主失败" >&2
			stop_all
			rm -rf "$payload_dir"
			exit 1
		fi
		out="$(run_client "$LEADER_ADDR" "$payload_dir")" || { echo "$mode 档跑失败（Leader $LEADER_ADDR）" >&2; stop_all; exit 1; }
		wal_bytes="$(seg_bytes "$ROOT_DATA/$mode/$LEADER_ID")" || { stop_all; rm -rf "$payload_dir"; exit 1; }
		report "$mode" "$out" "$wal_bytes"
		echo "  $mode 的 Leader 是 $LEADER_ID（$LEADER_ADDR），5 个节点各写一份 raft 日志" >&2
		stop_all
	done

	rm -rf "$payload_dir"
	exit 0
}

# 直接执行本文件才跑主流程；被 source 时（bench_read_modes.sh）只拿函数。
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	main "$@"
fi
