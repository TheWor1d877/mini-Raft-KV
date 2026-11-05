#!/usr/bin/env bash
# 重选主计时：kill -9 当前 Leader，取新 Leader 的 role changed 与 kill 时刻作差，跑满 N 轮。
# 用法：先 scripts/cluster_up.sh 把 5 个节点起好，再跑
#       scripts/leader_failover.sh [轮数=3] [数据根目录=data] [每轮在途写条数=20] [发出后隔多久杀=150ms]
# stdout 每轮一行 killed_leader elected_in_ms term_before term_after writes_inflight，表头在最前；
# 每个数字的日志原文行进 stderr 的「证据」块。
# 计时终点取新 Leader 的 role changed：当选后的那条 no-op 与第一轮 AppendEntries 就在同一次
# becomeLeader 的调用栈里排出去（3.1 §7、3.4 T22.3）。Follower 看见第一条心跳还要再加一跳 HTTP，
# 所以这个读数是「集群重新可用」的下界，不是 Follower 侧的观测时刻。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROUNDS="${1:-3}"
ROOT_DATA="${2:-$ROOT/data}"
INFLIGHT_WRITES="${3:-20}"
KILL_DELAY_MS="${4:-150}"
STATE="$ROOT_DATA/cluster.up"
LOG_REL="log/miniraft-node.log"
# stderr 的落点跟 cluster_up.sh 同一份口径：日志目录名只有这一处。
STDERR_REL="log/stderr.txt"
# 一轮最多等多久：选举超时 T 默认 500ms、随机窗口 [T,2T)，这里留够两轮重选的余量。
ELECT_DEADLINE_MS=20000

if [[ ! -f "$STATE" ]]; then
	echo "没有清单 $STATE，先跑 scripts/cluster_up.sh" >&2
	exit 1
fi

declare -A PID_OF=() ADDR_OF=()
while read -r id pid addr; do
	[[ -z "${id:-}" ]] && continue
	PID_OF[$id]="$pid"
	ADDR_OF[$id]="$addr"
done <"$STATE"
if [[ "${#PID_OF[@]}" -ne 5 ]]; then
	echo "清单 $STATE 里只有 ${#PID_OF[@]} 个节点，本脚本要 5 个都在跑" >&2
	exit 1
fi

node_ids() { printf '%s\n' "${!PID_OF[@]}" | sort -t- -k2 -n; }

alive_ids() {
	local id
	for id in $(node_ids); do
		if kill -0 "${PID_OF[$id]}" 2>/dev/null; then
			printf '%s\n' "$id"
		fi
	done
}

# ts 是 RFC3339Nano、固定 UTC（2.1 §8），换成纳秒整数才能和 date 取的 kill 时刻作差。
ts_ns() { date -d "$1" +%s%N; }

ns_clock() { date +%s%N; }

fmt_ms() { # 纳秒 → UTC 的 HH:MM:SS.mmm，只用于打印证据
	date -u -d "@$((($1) / 1000000000))" +'%H:%M:%S' | tr -d '\n'
	printf '.%03d' $(((${1:-0} / 1000000) % 1000))
}

# 每个存活节点最后一条 role changed 决定它现在的角色。重启过的节点留着上一任期的 leader 记录，
# 所以按 term 取最大：现任 Leader 的 term 一定高于任何被杀前任（term 单调不减，不变式 5）。
leader_now() {
	local id line ts term role best_term=-1 best_ns=-1 best_id="" best_ts="" ns
	for id in $(alive_ids); do
		line="$(tail -n 400 "$ROOT_DATA/$id/$LOG_REL" 2>/dev/null |
			jq -r 'select(.msg=="role changed") | [.ts, (.term|tostring), .new_role] | @tsv' | tail -1)"
		[[ -z "$line" ]] && continue
		ts="$(printf '%s' "$line" | cut -f1)"
		term="$(printf '%s' "$line" | cut -f2)"
		role="${line##*$'\t'}"
		[[ "$role" == "leader" ]] || continue
		ns="$(ts_ns "$ts")"
		if ((term > best_term)) || { ((term == best_term)) && ((ns > best_ns)); }; then
			best_term=$term best_ns="$ns" best_ts="$ts" best_id="$id"
		fi
	done
	[[ -z "$best_id" ]] && return 1
	printf '%s\t%s\t%s\n' "$best_id" "$best_ts" "$best_term"
}

wait_ready() {
	local addr="$1" dir="$2" code
	for _ in $(seq 1 400); do
		code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://$addr/get?key=__probe__&consistency=local" || true)"
		# 探活走 local 档：404 与 200 都是 handler 起来了。默认档在无 Leader 期间回 503，
		# 那一档要的是领导权确认，探活不该被它挡住（4.1 §4、D24）。
		[[ "$code" == "404" || "$code" == "200" ]] && return 0
		sleep 0.05
	done
	echo "重启后的节点在 $addr 上没有就绪" >&2
	tail -5 "$dir/$LOG_REL" >&2 || true
	return 1
}

printf 'killed_leader\telected_in_ms\tterm_before\tterm_after\twrites_inflight\n'

for ((round = 1; round <= ROUNDS; round++)); do
	leader_line="$(leader_now)" || { echo "第 $round 轮找不到 Leader，先跑 scripts/cluster_up.sh" >&2; exit 1; }
	killed="${leader_line%%$'\t'*}"
	killed_pid="${PID_OF[$killed]}"
	killed_log="$ROOT_DATA/$killed/$LOG_REL"
	leader_ts="$(printf '%s' "$leader_line" | cut -f2)"
	term_before="$(printf '%s' "$leader_line" | cut -f3)"

	# 重启要用原 argv：数据目录、成员表、日志级别都和 cluster_up.sh 给的一致，手工拼必漏。
	mapfile -td '' argv <"/proc/$killed_pid/cmdline"
	if [[ -z "${argv[0]:-}" ]]; then
		echo "读不到 $killed 的启动参数" >&2
		exit 1
	fi

	# 在途写：并发打一批 /put；响应码写进文件，kill 那一刻文件为空就是还没返回。
	outcome_dir="$(mktemp -d)"
	keys=()
	curl_pids=()
	for ((i = 1; i <= INFLIGHT_WRITES; i++)); do
		key="fail-r${round}-${i}"
		keys+=("$key")
		curl -s -o /dev/null -w '%{http_code}' --max-time 10 \
			-X POST -H 'Content-Type: application/json' \
			-d "{\"key\":\"${key}\",\"value\":\"$(printf 'v%.0s' $(seq 1 64))\"}" \
			"http://${ADDR_OF[$killed]}/put" >"$outcome_dir/$key" &
		curl_pids+=("$!")
	done

	# 这批写全部发出去之后再等 KILL_DELAY_MS，让前几条走完 ack 路径，剩下的就是在途。
	sleep "$(awk -v ms="$KILL_DELAY_MS" 'BEGIN{printf "%.3f", ms/1000}')"
	t0="$(ns_clock)"
	kill -9 "$killed_pid"
	inflight=0
	for f in "$outcome_dir"/*; do
		[[ -s "$f" ]] || inflight=$((inflight + 1))
	done
	# 只等这批 curl：裸 wait 会把重启节点的常驻子进程也算进来，那一轮就再也回不来。
	wait "${curl_pids[@]}"

	acked=0
	for f in "$outcome_dir"/*; do
		[[ "$(cat "$f")" == "200" ]] && acked=$((acked + 1))
	done

	# 等新 Leader：只认 term 高于 term_before、且 ts 不早于 kill 时刻的 role changed，取最早一条。
	elected_ns="" elected_id="" elected_term="" elected_ts=""
	deadline=$((t0 + ELECT_DEADLINE_MS * 1000000))
	while [[ -z "$elected_id" ]]; do
		for id in $(alive_ids); do
			log="$ROOT_DATA/$id/$LOG_REL"
			while IFS=$'\t' read -r ts term nid; do
				[[ -z "$ts" ]] && continue
				ns="$(ts_ns "$ts")"
				((ns < t0)) && continue
				if [[ -z "$elected_ns" || "$ns" -lt "$elected_ns" ]]; then
					elected_ns="$ns" elected_id="$nid" elected_term="$term" elected_ts="$ts"
				fi
			done < <(jq -r --argjson t "$term_before" \
				'select(.msg=="role changed" and .new_role=="leader" and .term > $t) | [.ts, (.term|tostring), .node_id] | @tsv' \
				"$log" 2>/dev/null)
		done
		if [[ -z "$elected_id" ]]; then
			if ((($(ns_clock)) > deadline)); then
				echo "第 $round 轮 ${ELECT_DEADLINE_MS}ms 内没选出新 Leader" >&2
				exit 1
			fi
			sleep 0.02
		fi
	done
	elected_in_ms=$(((elected_ns - t0) / 1000000))

	# 重选主后新 Leader 上逐条 /get：读得回的条数（在途那批里最终落盘的）。
	# 这一格查的是「那条写有没有落进状态机」，走 local 档：默认档要领导权确认，
	# 读的是值而不是领导权（4.1 §4、D24）。
	sleep 1
	landed=0
	for key in "${keys[@]}"; do
		code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
			"http://${ADDR_OF[$elected_id]}/get?key=${key}&consistency=local" || true)"
		[[ "$code" == "200" ]] && landed=$((landed + 1))
	done

	printf '%s\t%s\t%s\t%s\t%s\n' "$killed" "$elected_in_ms" "$term_before" "$elected_term" "$inflight"
	{
		echo "第 $round 轮 证据"
		echo "  kill 时刻（脚本时钟，与日志同一台机器）$(fmt_ms "$t0")Z"
		echo "  $killed 当选于 term=$term_before —— $killed_log"
		grep -F "\"ts\":\"$leader_ts\"" "$killed_log" | grep '"new_role":"leader"' | tail -1 | sed 's/^/    /'
		echo "  新 Leader $elected_id term=$elected_term —— $ROOT_DATA/$elected_id/$LOG_REL"
		grep -F "\"ts\":\"$elected_ts\"" "$ROOT_DATA/$elected_id/$LOG_REL" |
			grep '"new_role":"leader"' | head -1 | sed 's/^/    /'
		echo "  在途写：发出 ${#keys[@]} 条，kill 时未返回 $inflight 条，kill 前已 ack $acked 条，重选后读回 $landed 条"
		# ack 过的写必须读得回（3.1 §4：Propose 只在 apply 完成之后返回）；读不回就是 bug。
		if ((acked > landed)); then
			echo "  警告：$((acked - landed)) 条已 ack 的写重选后读不回" >&2
		fi
	} >&2

	# 放回被杀的节点，下一轮才是同样的 5 节点条件；清单同步成新 pid。
	# stderr 的落点和 cluster_up.sh 一致：panic 只写 stderr，丢了就没有现场。
	"${argv[@]}" >/dev/null 2>>"$ROOT_DATA/$killed/$STDERR_REL" &
	PID_OF[$killed]=$!
	if ! wait_ready "${ADDR_OF[$killed]}" "$ROOT_DATA/$killed"; then
		echo "第 $round 轮之后重启 $killed 失败，后续轮次不在 5 节点上跑" >&2
		exit 1
	fi
	: >"$STATE"
	for id in $(node_ids); do
		printf '%s\t%s\t%s\n' "$id" "${PID_OF[$id]}" "${ADDR_OF[$id]}" >>"$STATE"
	done
	rm -rf "$outcome_dir"
done
