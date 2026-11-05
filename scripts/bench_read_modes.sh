#!/usr/bin/env bash
# 一致性读的三档延迟对照（M14、T30.5）：lease／readindex／local 三种读法，外加一档 lease + 放宽选举超时。
# 用法：scripts/bench_read_modes.sh [读取条数=2000] [起始端口=8931] [数据根目录=data/bench-read]
# 四档：
#   lease       5 进程集群，节点参数 "-read-mode lease"：线性一致读走租约
#   readindex   5 进程集群，节点参数 "-read-mode readindex"：每条读走一轮确认
#   local       请求级参数 "?consistency=local"，跑在同一份 lease 集群上，这一档不发一次节点间 RPC
#   lease-t1000 5 进程集群，节点参数 "-read-mode lease -election-timeout-ms 1000"：租约窗口 950ms，用来对照命中率
# 环境变量 BENCH_LEASE_EXTRA 会追加到 lease-t1000 那一档的节点参数后面（例如 "-read-timeout-ms 500"）。
# 默认的 -read-timeout-ms 1000ms 盖不住任何一档的租约窗口（450ms／950ms），
# 租约只会被授予、不会被读复用（read.go 的 leaseServedLocked），要看到租约命中就得把它压到窗口以下。
# 每档的流程：起 5 进程集群（自己的数据目录）→ 把同样的 N 条写进 Leader（POST /put）
#             → 在同一批 key 上读 N 次（GET /get）→ 收进程。写完再读，读的是自己刚写的那批。
# 计时口径：单进程 keep-alive 客户端（http.client.HTTPConnection），串行读完同一批 key，
#           与 bench_single_vs_raft.sh 的写吞吐同一套口径（4.2 §1 的数据层）。
# stdout 每档一行：mode acked ops p50_ms p99_ms rpc_per_read（制表符分隔，前面一行表头）。
# 公共部分（start_raft_cluster／make_payloads／stop_all／run_client）从 bench_single_vs_raft.sh
# source 进来，本文件不复制一份逻辑。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/bench_single_vs_raft.sh
source "$ROOT/scripts/bench_single_vs_raft.sh"

N="${1:-2000}"
PORT0="${2:-8931}"
ROOT_DATA="${3:-$ROOT/data/bench-read}"
CLUSTER_SIZE=5            # 集群节点数；一轮广播发 CLUSTER_SIZE-1 条 AppendEntries
FANOUT=$((CLUSTER_SIZE - 1))
NODE_ARGS="-log-level debug"  # 追加到 5 个进程命令行尾部：rpc request served 是 debug 级日志，
                              # 不开 debug 就数不出节点间 RPC 的条数
CURRENT_PREFIX=""         # 当前集群的数据目录前缀
CURRENT_DIR=""
ROW=""                    # read_phase 产出的一行读数
PAYLOAD_DIR=""            # 负载目录（写与读用同一批 key）
LOG_FROM=()               # 读区间开始前每个节点日志的行数（下标 0..4 对应 node-1..node-5）
LOG_TO=()                 # 读区间结束时每个节点日志的行数

# 二进制检查：两个都要在。
check_bins() {
	local b
	for b in "$BIN_KV" "$BIN_NODE"; do
		if [[ ! -x "$b" ]]; then
			echo "找不到 $b，先跑：go build -o bin/miniraft-kv ./cmd/miniraft-kv && go build -o bin/miniraft-node ./cmd/miniraft-node" >&2
			exit 1
		fi
	done
}

# 读一档：单进程 keep-alive 客户端串行读完 N 条，返回 JSON（acked／ops／p50／p99／形状不对的条数）。
read_client() { # $1 = host:port，$2 = 条数，$3 = 读法（linearizable 或 local）
	python3 - "$1" "$2" "$3" <<'PY'
import http.client, json, sys, time
addr, n, mode = sys.argv[1], int(sys.argv[2]), sys.argv[3]
host, port = addr.split(":")
conn = http.client.HTTPConnection(host, int(port), timeout=30)
lat, acked, failed, bad = [], 0, [], 0
t0 = time.perf_counter()
for i in range(1, n + 1):
    key = "bench-%06d" % i
    path = "/get?key=" + key + ("&consistency=local" if mode == "local" else "")
    s = time.perf_counter()
    conn.request("GET", path)
    resp = conn.getresponse()
    body = resp.read()
    lat.append((time.perf_counter() - s) * 1000)
    if resp.status != 200:
        failed.append(resp.status)
        continue
    acked += 1
    try:
        obj = json.loads(body)
    except ValueError:
        bad += 1
        continue
    # 形状检查（D24）：local 档 stale 为真、read_index 回 0；线性档 stale 为假、read_index 不为 0。
    if bool(obj.get("stale")) != (mode == "local"):
        bad += 1
    elif mode == "local" and obj.get("read_index") != 0:
        bad += 1
    elif mode != "local" and not obj.get("read_index"):
        bad += 1
elapsed = (time.perf_counter() - t0) * 1000
lat.sort()
p50 = lat[len(lat) // 2] if lat else 0.0
p99 = lat[min(len(lat) - 1, int(len(lat) * 0.99))] if lat else 0.0
print(json.dumps({"acked": acked, "failed": sorted(set(failed)), "bad": bad,
                  "elapsed_ms": round(elapsed, 1),
                  "ops_per_sec": round(acked * 1000 / elapsed, 1) if elapsed else 0,
                  "p50_ms": round(p50, 2), "p99_ms": round(p99, 2)}))
PY
}

# 数读区间里 5 个节点日志中某个 msg 的条数。区间用行号卡：读之前与读之后各记一次行数。
count_msg() { # $1 = msg 名，$2 = route（空串 = 不按 route 筛）
	local msg="$1" route="${2:-}" total=0 i from to f c
	for ((i = 0; i < CLUSTER_SIZE; i++)); do
		from=$((LOG_FROM[i] + 1))
		to="${LOG_TO[i]}"
		((to >= from)) || continue
		f="$CURRENT_DIR/node-$((i + 1))/$LOG_REL"
		c="$(sed -n "${from},${to}p" "$f" |
			jq -R -s --arg m "$msg" --arg r "$route" \
				'split("\n") | map(fromjson?) | map(select(.msg == $m and ($r == "" or .route == $r))) | length')"
		total=$((total + c))
	done
	printf '%s' "$total"
}

# 从节点参数组里取一个毫秒数的开关值：$1 = 参数组，$2 = 开关名，$3 = 缺省值。
flag_ms() { # $1 = 节点参数组，$2 = 开关名（例如 -election-timeout-ms），$3 = 缺省值
	local args="$1" name="$2" def="$3" i=0 v
	v="$def"
	local -a words=($args)
	for ((i = 0; i < ${#words[@]}; i++)); do
		if [[ "${words[i]}" == "$name" ]]; then
			v="${words[i + 1]:-$def}"
		fi
	done
	printf '%s' "$v"
}

# 租约窗口 = -election-timeout-ms 减 -clock-drift-ms（4.1 §6）。
lease_window_ms() { # $1 = 节点参数组
	local args="$1"
	printf '%s' "$(( $(flag_ms "$args" -election-timeout-ms 500) - $(flag_ms "$args" -clock-drift-ms 50) ))"
}

# 起一档的集群：$1 = 数据目录前缀，$2 = 追加到 5 个进程命令行尾部的节点参数（一组词，可空）。
start_cluster() { # $1 = 数据目录前缀，$2 = 节点参数组
	local prefix="$1" node_args="$2"
	CURRENT_PREFIX="$prefix"
	CURRENT_DIR="$ROOT_DATA/$prefix"
	rm -rf "$CURRENT_DIR"
	mkdir -p "$CURRENT_DIR"
	if ! start_raft_cluster "$prefix" "" "$node_args $NODE_ARGS"; then
		echo "$prefix 档起集群或选主失败" >&2
		stop_all
		exit 1
	fi
}

# 写一档：把同样的 N 条写进 Leader（POST /put），写完才算读完的前提成立。
write_phase() { # $1 = 档名
	local label="$1" out acked
	out="$(run_client "$LEADER_ADDR" "$PAYLOAD_DIR")" || { echo "$label 档写失败（Leader $LEADER_ADDR）" >&2; exit 1; }
	acked="$(printf '%s' "$out" | jq -r .acked)"
	if [[ "$acked" != "$N" ]]; then
		echo "$label 档只写进 $acked / $N 条：$(printf '%s' "$out" | jq -c .failed)" >&2
		exit 1
	fi
	echo "  $label 档写入 $acked 条（Leader $LEADER_ID / $LEADER_ADDR），elapsed $(printf '%s' "$out" | jq -r .elapsed_ms)ms" >&2
}

# 读一档：读区间内的日志计数 + 一行读数。成功时把这一行放进全局 ROW，失败返回 1。
read_phase() { # $1 = 档名，$2 = 读法（linearizable 或 local），$3 = 节点参数组
	local label="$1" mode="$2" node_args="$3"
	local i f out acked ops p50 p99 raw_rpc rounds read_rpc granted rejected settled served
	ROW=""
	LOG_FROM=()
	LOG_TO=()
	for ((i = 1; i <= CLUSTER_SIZE; i++)); do
		f="$CURRENT_DIR/node-$i/$LOG_REL"
		if [[ ! -f "$f" ]]; then
			echo "$label 档缺少节点日志：$f" >&2
			exit 1
		fi
		LOG_FROM+=("$(wc -l < "$f")")
	done
	out="$(read_client "$LEADER_ADDR" "$N" "$mode")" || { echo "$label 档的读客户端跑失败" >&2; exit 1; }
	for ((i = 1; i <= CLUSTER_SIZE; i++)); do
		f="$CURRENT_DIR/node-$i/$LOG_REL"
		LOG_TO+=("$(wc -l < "$f")")
	done

	acked="$(printf '%s' "$out" | jq -r .acked)"
	ops="$(printf '%s' "$out" | jq -r .ops_per_sec)"
	p50="$(printf '%s' "$out" | jq -r .p50_ms)"
	p99="$(printf '%s' "$out" | jq -r .p99_ms)"

	# 日志计数：读区间内 5 个节点日志里的三条读路径消息。
	granted="$(count_msg "lease granted")"
	rejected="$(count_msg "lease rejected")"
	settled="$(count_msg "read round settled")"
	served="$(count_msg "read served by lease")"
	rounds="$(count_msg "read round sent")"
	# 节点间 RPC：日志里 route=/raft/append-entries 的 "rpc request served"。
	# 这一栏含例行心跳，读触发的那部分是「每轮确认向 4 个同伴各发一条」＝ FANOUT×轮次。
	raw_rpc="$(count_msg "rpc request served" "/raft/append-entries")"
	read_rpc=$((rounds * FANOUT))

	echo "  $label 档（$node_args，读法 $mode）：Leader=$LEADER_ID（$LEADER_ADDR）" >&2
	echo "    读区间 append-entries RPC（日志 rpc request served）=$raw_rpc，其中读触发 $read_rpc（$rounds 轮 × $FANOUT），余下是例行心跳" >&2
	echo "    lease granted=$granted lease rejected=$rejected read round settled=$settled read served by lease=$served" >&2
	if ((granted > 0 && served == 0)); then
		# 租约被授予却没有被读复用：read.go 的 leaseServedLocked 要求「剩余租约盖得住这次读的超时上界」，
		# 默认 -read-timeout-ms 1000ms 盖不住租约窗口，所以租约只授予不复用（4.1 §7 租约命中那格）。
		echo "    注意：租约授予了但没被读复用：剩余租约要盖得住 -read-timeout-ms（$(flag_ms "$node_args" -read-timeout-ms 1000)ms），租约窗口只有 $(lease_window_ms "$node_args")ms" >&2
	fi
	echo "    日志：$CURRENT_DIR/node-*/$LOG_REL（5 个节点各一份）" >&2

	if [[ "$acked" != "$N" ]]; then
		echo "  $label 档只读成 $acked / $N 条：$(printf '%s' "$out" | jq -c .failed)" >&2
		return 1
	fi
	if [[ "$(printf '%s' "$out" | jq -r .bad)" != "0" ]]; then
		echo "  $label 档有 $(printf '%s' "$out" | jq -r .bad) 条响应形状不对（stale／read_index 与档位不符）" >&2
		return 1
	fi

	ROW="$(printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$label" "$acked" "$ops" "$p50" "$p99" \
		"$(awk -v a="$read_rpc" -v b="$N" 'BEGIN { printf "%.2f", (b ? a / b : 0) }')")"
	return 0
}

main() {
	local ROW_LEASE ROW_READINDEX ROW_LOCAL ROW_LEASE_T1000
	check_bins
	PAYLOAD_DIR="$(mktemp -d)"
	make_payloads "$PAYLOAD_DIR"

	# 档 1（lease）与档 3（local）：同一份集群，local 是请求级参数，不另起集群。
	start_cluster "lease" "-read-mode lease"
	write_phase "lease"
	if ! read_phase "lease" linearizable "-read-mode lease"; then exit 1; fi
	ROW_LEASE="$ROW"
	if ! read_phase "local" local "-read-mode lease"; then exit 1; fi
	ROW_LOCAL="$ROW"
	stop_all

	# 档 2（readindex）：另起一份集群，节点参数换成逐读确认。
	start_cluster "readindex" "-read-mode readindex"
	write_phase "readindex"
	if ! read_phase "readindex" linearizable "-read-mode readindex"; then exit 1; fi
	ROW_READINDEX="$ROW"
	stop_all

	# 档 4（lease + 选举超时 1000ms）：租约窗口 950ms，用来对照租约的命中率。
	local lease_t1000_args="-read-mode lease -election-timeout-ms 1000 ${BENCH_LEASE_EXTRA:-}"
	start_cluster "lease-t1000" "$lease_t1000_args"
	write_phase "lease-t1000"
	if ! read_phase "lease-t1000" linearizable "$lease_t1000_args"; then exit 1; fi
	ROW_LEASE_T1000="$ROW"
	stop_all

	printf 'mode\tacked\tops\tp50_ms\tp99_ms\trpc_per_read\n'
	printf '%s\n' "$ROW_LEASE" "$ROW_READINDEX" "$ROW_LOCAL" "$ROW_LEASE_T1000"
	rm -rf "$PAYLOAD_DIR"
	exit 0
}

main "$@"
