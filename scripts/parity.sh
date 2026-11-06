#!/usr/bin/env bash
# 对拍驱动（docs/8 第七节）：两侧同参数起 5 对 5，逐档打同一串操作，中途按地址切断一台、
# 冻住主节点、同时 kill -9 两边的 Leader，读回收尾。
# 用法：ETCD_BIN=~/tools/etcd-v3.7.0/etcd scripts/parity.sh [mini 起始端口=8801] [etcd 起始端口=9801] [failover 条数=100]
# 档位选择：STAGES="freeze partition" scripts/parity.sh ... 只跑列出来的档；默认十一档全跑。
# 含 partition 或 prevote 时整轮重新 exec 进 unshare -rn 的网络命名空间：iptables 规则只作用于本轮 10 个进程，
# 主机上的其它 etcd（kind 之类）不受影响。
# 参数配平走 cluster_up.sh 的第四个参数与 etcd_up.sh 里的固定值：两侧都是心跳 100ms、选举基线 1000ms、快照阈值 10000。
# 数据落在 data/parity/<时间戳>/mini 与 .../etcd 下，一轮一份，不碰上一轮也不碰 data/node-i：kill -9 之后的数据目录留着看现场。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT0M="${1:-8801}"
PORT0E="${2:-9801}"
FO_OPS="${3:-100}"
BATCH_OPS=50
STAGES="${STAGES:-topology single batch stale retry bigvalue freeze partition prevote failover readback}"
FREEZE_MS=4000
ISO_MS=6000
# 两个集群各占一段地址。mini-Raft 每台绑自己的回环地址，出站源地址就是它自己，按地址成对切就够。
# etcd 走每台一个独立网络命名空间（etcd_up.sh 的 ETCD_NETNS=1），地址在 10.20.0.2x 上；
# 单命名空间里它的出站源地址会被路由塌缩成 127.0.0.1，按地址切必然误伤别的成员，理由见下面的隔离函数。
MINI_HOSTS=(127.0.0.11 127.0.0.12 127.0.0.13 127.0.0.14 127.0.0.15)
# 客户端绑一个不属于任何节点的源地址：它留在被切出去的那台旁边，照常打它，
# 量的是「少数派没有多数派可提交」而不是「客户端连不上」。etcd 那侧要绑网桥地址才路由得到 10.20.0.2x。
export PARITY_CLIENT_BIND="${PARITY_CLIENT_BIND:-127.0.0.100}"
if [[ "${PARITY_NETNS:-}" == "1" ]]; then
	ETCD_HOSTS=(10.20.0.21 10.20.0.22 10.20.0.23 10.20.0.24 10.20.0.25)
	export ETCD_NETNS=1 PARITY_CLIENT_BIND_ETCD="${PARITY_CLIENT_BIND_ETCD:-10.20.0.1}"
else
	ETCD_HOSTS=(127.0.0.21 127.0.0.22 127.0.0.23 127.0.0.24 127.0.0.25)
	export ETCD_NETNS=0
fi

have_stage() { [[ " $STAGES " == *" $1 "* ]]; }
if { have_stage partition || have_stage prevote; } && [[ "${PARITY_NETNS:-}" != "1" ]]; then
	exec unshare -rn env PARITY_NETNS=1 STAGES="$STAGES" ETCD_BIN="${ETCD_BIN:-}" bash "$0" "$@"
fi
[[ "${PARITY_NETNS:-}" == "1" ]] && ip link set lo up 2>/dev/null || true
RUN="$ROOT/data/parity/$(date +%Y%m%d-%H%M%S)"
MINI_DATA="$RUN/mini"
ETCD_DATA="$RUN/etcd"
BIN="$ROOT/bin/miniraft-parity"
MINI_EXTRA="-election-timeout-ms 1000 -snapshot-entries 10000"
ETCD_BIN="${ETCD_BIN:-$ROOT/bin/etcd}"

mkdir -p "$RUN"
MINI_ADDRS=""
ETCD_ADDRS=""
MINI_LIST=""
ETCD_LIST=""
for ((i = 0; i < 5; i++)); do
	ma="${MINI_HOSTS[$i]}:$((PORT0M + i))"
	ea="${ETCD_HOSTS[$i]}:$((PORT0E + i))"
	[[ -n "$MINI_ADDRS" ]] && MINI_ADDRS+="," && ETCD_ADDRS+="," && MINI_LIST+=" " && ETCD_LIST+=" "
	MINI_ADDRS+="$ma"
	ETCD_ADDRS+="$ea"
	MINI_LIST+="$ma"
	ETCD_LIST+="$ea"
done
MINI_IPS="$(printf '%s,' "${MINI_HOSTS[@]}")"
ETCD_IPS="$(printf '%s,' "${ETCD_HOSTS[@]}")"
MINI_IPS="${MINI_IPS%,}"
ETCD_IPS="${ETCD_IPS%,}"

{
	echo "run	$RUN"
	echo "mini_bin	$ROOT/bin/miniraft-node"
	echo "mini_extra	$MINI_EXTRA"
	echo "mini_addrs	$MINI_ADDRS"
	echo "mini_data	$MINI_DATA"
	echo "etcd_bin	$ETCD_BIN"
	echo "etcd_extra	--heartbeat-interval 100 --election-timeout 1000 --snapshot-count 10000"
	echo "etcd_addrs	$ETCD_ADDRS"
	echo "etcd_data	$ETCD_DATA"
	echo "etcd_netns	$ETCD_NETNS"
	echo "client_bind	$PARITY_CLIENT_BIND"
	echo "client_bind_etcd	${PARITY_CLIENT_BIND_ETCD:-}"
	echo "etcd_version	$("$ETCD_BIN" --version 2>/dev/null | head -1)"
} >"$RUN/topology.txt"

down() {
	"$ROOT/scripts/cluster_down.sh" "$MINI_DATA" >>"$RUN/teardown.txt" 2>&1 || true
	KEEP=1 "$ROOT/scripts/etcd_down.sh" "$RUN" >>"$RUN/teardown.txt" 2>&1 || true
}
trap down EXIT

if [[ ! -x "$ETCD_BIN" ]]; then
	echo "找不到 $ETCD_BIN，先按 docs/8 第三节编 etcd，或 ETCD_BIN=/path/to/etcd 指过去" >&2
	exit 1
fi

(cd "$ROOT" && go build -o bin/miniraft-node ./cmd/miniraft-node && go build -o bin/miniraft-parity ./cmd/miniraft-parity) || exit 1

echo "== 起 mini-Raft（$MINI_EXTRA）" >&2
NODE_IPS="$MINI_IPS" "$ROOT/scripts/cluster_up.sh" "$PORT0M" "$MINI_DATA" info "$MINI_EXTRA" >"$RUN/cluster-up-mini.tsv" || exit 1
echo "== 起 etcd" >&2
NODE_IPS="$ETCD_IPS" ETCD_BIN="$ETCD_BIN" "$ROOT/scripts/etcd_up.sh" "$PORT0E" "$RUN" >"$RUN/cluster-up-etcd.tsv" || exit 1

run_phase() {
	local p="$1"
	shift
	echo "== phase $p $*" >&2
	"$BIN" -phase "$p" -out "$RUN" -mini "$MINI_ADDRS" -etcd "$ETCD_ADDRS" "$@" 2>>"$RUN/parity-stderr.txt" |
		tee -a "$RUN/summary.tsv"
}

mini_leader() {
	local a id
	for a in $MINI_LIST; do
		id="$(curl -s --max-time 2 "http://$a/raft/membership" | tr -d '"' |
			sed -n 's/.*leader_id:node-\([0-9]*\).*/\1/p')"
		[[ -n "$id" ]] && { echo "$id"; return 0; }
	done
	return 1
}

# etcd_leader_index 定出 Leader 是第几台：status 只给 member_id，认不出地址；
# 逐台问自己的 header.member_id，与 leader 字段对上的那台就是 Leader。
etcd_leader_index() {
	local a lead id idx
	for a in $ETCD_LIST; do
		lead="$(curl -s --max-time 2 -XPOST "http://$a/v3/maintenance/status" -d '{}' |
			grep -o '"leader":"[0-9]*"' | cut -d'"' -f4)"
		[[ -n "$lead" ]] && break
	done
	[[ -z "${lead:-}" ]] && return 1
	idx=0
	for a in $ETCD_LIST; do
		idx=$((idx + 1))
		id="$(curl -s --max-time 2 -XPOST "http://$a/v3/maintenance/status" -d '{}' |
			grep -o '"member_id":"[0-9]*"' | head -1 | cut -d'"' -f4)"
		[[ "$id" == "$lead" ]] && { echo "$idx"; return 0; }
	done
	return 1
}

# leader_pid 读启动清单：mini 的清单在 <数据根>/cluster.up，etcd 的在 <运行目录>/etcd/etcd.up。
leader_pid() {
	local data="$1" id="$2" file=""
	for f in "$data/cluster.up" "$data/etcd/etcd.up"; do
		[[ -f "$f" ]] && { file="$f"; break; }
	done
	[[ -n "$file" ]] || return 1
	awk -F'\t' -v id="$id" '$1==id {print $2}' "$file"
}

# wait_leaders 等两侧都选出一个主再开始打：集群刚起时、或上一档杀完主之后，选举窗口还没走完。
wait_leaders() {
	for _ in $(seq 1 60); do
		if mini_leader >/dev/null 2>&1 && etcd_leader_index >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.25
	done
	echo "两侧没有在 15 秒内选出 Leader" >&2
	return 1
}

# mini-Raft 的出站连接绑自己的地址，按地址成对切断就把它和其余成员隔开了。
# 客户端地址 127.0.0.100 不是成员地址，不受影响，照常打被隔离的那台。
isolate_host() {
	local self="$1" x
	for x in "${MINI_HOSTS[@]}"; do
		[[ "$x" == "$self" ]] && continue
		iptables -w -A OUTPUT -s "$self" -d "$x" -j DROP
		iptables -w -A OUTPUT -s "$x" -d "$self" -j DROP
	done
}

heal_host() {
	local self="$1" x
	for x in "${MINI_HOSTS[@]}"; do
		[[ "$x" == "$self" ]] && continue
		iptables -w -D OUTPUT -s "$self" -d "$x" -j DROP 2>/dev/null || true
		iptables -w -D OUTPUT -s "$x" -d "$self" -j DROP 2>/dev/null || true
	done
}

# etcd 的隔离规则下在被隔离那台自己的命名空间里，按对端地址把进出都封上。
# 放在这里面有三点：一是只影响这一台（同一空间的其它成员照常互通），二是自带地址不受影响，
# 三是客户端在网桥那段、不是成员地址，照常打得到它。对端地址覆盖双向，回包和新建连接都堵住。
isolate_etcd_node() {
	local pid="$1" self="$2" x
	for x in "${ETCD_HOSTS[@]}"; do
		[[ "$x" == "$self" ]] && continue
		nsenter -t "$pid" -n -- iptables -w -A OUTPUT -d "$x" -j DROP
		nsenter -t "$pid" -n -- iptables -w -A INPUT -s "$x" -j DROP
	done
}

heal_etcd_node() {
	local pid="$1" self="$2" x
	for x in "${ETCD_HOSTS[@]}"; do
		[[ "$x" == "$self" ]] && continue
		nsenter -t "$pid" -n -- iptables -w -D OUTPUT -d "$x" -j DROP 2>/dev/null || true
		nsenter -t "$pid" -n -- iptables -w -D INPUT -s "$x" -j DROP 2>/dev/null || true
	done
}

# freeze：两侧同时 SIGSTOP 主节点 4 秒，客户端在冻住期间持续打它。
run_freeze_stage() {
	local mli eli mpid epid fp
	mli="$(mini_leader)" || { echo "freeze: mini 侧问不出 Leader" >&2; return 1; }
	eli="$(etcd_leader_index)" || { echo "freeze: etcd 侧问不出 Leader" >&2; return 1; }
	mpid="$(leader_pid "$MINI_DATA" "node-$mli")"
	epid="$(leader_pid "$RUN" "node-$eli")"
	if [[ -z "${mpid:-}" || -z "${epid:-}" ]]; then
		echo "freeze: 找不到主节点 pid（mini node-$mli / etcd node-$eli）" >&2
		return 1
	fi
	: >"$RUN/freezestamp"
	run_phase freeze -faultstamp "$RUN/freezestamp" -faultdur 9s -faultfor "${FREEZE_MS}ms" &
	fp=$!
	sleep 2
	date +%s%3N >"$RUN/freezestamp"
	kill -STOP "$mpid" "$epid"
	echo "freeze	stop	mini=node-$mli pid=$mpid etcd=node-$eli pid=$epid" | tee -a "$RUN/summary.tsv"
	sleep $((FREEZE_MS / 1000))
	kill -CONT "$mpid" "$epid"
	wait "$fp" || { echo "freeze: 客户端非零退出" >&2; return 1; }
}

# partition：把两侧主节点各自从其余成员之间切出去 6 秒；客户端照常打它，同时盯着多数派那一侧。
run_partition_stage() {
	local mli eli iso_m iso_e epid pp iso_stamp got kind
	if ! iptables -w -S >/dev/null 2>&1; then
		echo "partition: 当前环境没有 CAP_NET_ADMIN，跳过这一档（用 unshare -rn 跑整轮）" | tee -a "$RUN/summary.tsv" >&2
		return 0
	fi
	mli="$(mini_leader)" || { echo "partition: mini 侧问不出 Leader" >&2; return 1; }
	eli="$(etcd_leader_index)" || { echo "partition: etcd 侧问不出 Leader" >&2; return 1; }
	iso_m="${MINI_HOSTS[$((mli - 1))]}"
	iso_e="${ETCD_HOSTS[$((eli - 1))]}"
	epid="$(leader_pid "$RUN" "node-$eli")"
	[[ -n "${epid:-}" ]] || { echo "partition: 找不到 etcd node-$eli 的 pid" >&2; return 1; }
	: >"$RUN/isostamp"
	run_phase partition -faultstamp "$RUN/isostamp" -faultdur 11s -faultfor "${ISO_MS}ms" &
	pp=$!
	sleep 2
	isolate_host "$iso_m"
	isolate_etcd_node "$epid" "$iso_e"
	date +%s%3N >"$RUN/isostamp"
	echo "partition	isolate	mini=node-$mli host=$iso_m etcd=node-$eli host=$iso_e pid=$epid" | tee -a "$RUN/summary.tsv"
	sleep $((ISO_MS / 1000))
	heal_host "$iso_m"
	heal_etcd_node "$epid" "$iso_e"
	wait "$pp" || { echo "partition: 客户端非零退出" >&2; return 1; }
	iso_stamp="$(cat "$RUN/isostamp")"
	for kind in mini etcd; do
		got="$(first_leader_after "$kind" "$iso_stamp")"
		if [[ -n "$got" ]]; then
			echo "$kind	partition/election_log	after_iso_ms=$(( ${got%% *} - iso_stamp )) node=${got#* }" | tee -a "$RUN/summary.tsv"
		fi
	done
}

# prevote：把两侧各一台非主节点切出去 6 秒，看它自己的任期涨不涨（不实现 Pre-Vote 的每一轮竞选都加一），
# 解冻后再看集群是否为这台的任期让路而换届。
run_prevote_stage() {
	local mli eli mfi efi iso_m iso_e epid pp
	mli="$(mini_leader)" || { echo "prevote: mini 侧问不出 Leader" >&2; return 1; }
	eli="$(etcd_leader_index)" || { echo "prevote: etcd 侧问不出 Leader" >&2; return 1; }
	# 与客户端 follower() 同一条规则：下标最小的那个非主节点。
	mfi=1
	[[ "$mli" -eq 1 ]] && mfi=2
	efi=1
	[[ "$eli" -eq 1 ]] && efi=2
	iso_m="${MINI_HOSTS[$((mfi - 1))]}"
	iso_e="${ETCD_HOSTS[$((efi - 1))]}"
	epid="$(leader_pid "$RUN" "node-$efi")"
	[[ -n "${epid:-}" ]] || { echo "prevote: 找不到 etcd node-$efi 的 pid" >&2; return 1; }
	: >"$RUN/prevotestamp"
	run_phase prevote -faultstamp "$RUN/prevotestamp" -faultdur 12s -faultfor "${ISO_MS}ms" &
	pp=$!
	sleep 2
	isolate_host "$iso_m"
	isolate_etcd_node "$epid" "$iso_e"
	date +%s%3N >"$RUN/prevotestamp"
	echo "prevote	isolate	mini=node-$mfi host=$iso_m etcd=node-$efi host=$iso_e pid=$epid" | tee -a "$RUN/summary.tsv"
	sleep $((ISO_MS / 1000))
	heal_host "$iso_m"
	heal_etcd_node "$epid" "$iso_e"
	wait "$pp" || { echo "prevote: 客户端非零退出" >&2; return 1; }
}

# first_leader_after 从进程日志里取杀主之后第一行「当上主」的时刻：mini 看 role changed 的
# new_role=leader，etcd 看 became leader at term。输出 "<epoch_ms> node-<i>"，取不到就空。
first_leader_after() {
	local kind="$1" killms="$2" f line ts ms id best="" files=()
	if [[ "$kind" == mini ]]; then
		files=("$MINI_DATA"/node-*/log/miniraft-node.log)
	else
		files=("$ETCD_DATA"/node-*/stderr.txt)
	fi
	for f in "${files[@]}"; do
		[[ -f "$f" ]] || continue
		id="$(basename "$(dirname "$f")")"
		[[ "$kind" == mini ]] && id="$(basename "$(dirname "$(dirname "$f")")")"
		while IFS= read -r line; do
			ts="$(printf '%s' "$line" | sed -n 's/.*"ts":"\([^"]*\)".*/\1/p')"
			[[ -n "$ts" ]] || continue
			ms="$(date -d "$ts" +%s%3N 2>/dev/null)" || continue
			[[ -n "$ms" ]] || continue
			(( ms >= killms )) || continue
			if [[ -z "$best" ]] || (( ms < ${best%% *} )); then
				best="$ms $id"
			fi
		done < <(if [[ "$kind" == mini ]]; then grep -F '"new_role":"leader"' "$f"; else grep -F ' became leader at term ' "$f"; fi)
	done
	[[ -n "$best" ]] && echo "$best"
}

wait_leaders || exit 1

for p in topology single batch stale retry bigvalue; do
	have_stage "$p" || continue
	case "$p" in
	batch) run_phase "$p" -ops "$BATCH_OPS" ;;
	*) run_phase "$p" ;;
	esac
done

if have_stage freeze; then
	run_freeze_stage || exit 1
	wait_leaders || exit 1
fi
if have_stage partition; then
	run_partition_stage || exit 1
	wait_leaders || exit 1
fi
if have_stage prevote; then
	run_prevote_stage || exit 1
	wait_leaders || exit 1
fi

if have_stage failover; then
	: >"$RUN/killstamp"
	run_phase failover -ops "$FO_OPS" -cadence 100ms -killstamp "$RUN/killstamp" &
	FAIL_PIPE=$!
	sleep 2
	mli="$(mini_leader)" || { echo "mini 侧问不出 Leader" >&2; wait "$FAIL_PIPE"; exit 1; }
	eli="$(etcd_leader_index)" || { echo "etcd 侧问不出 Leader" >&2; wait "$FAIL_PIPE"; exit 1; }
	mpid="$(leader_pid "$MINI_DATA" "node-$mli")"
	epid="$(leader_pid "$RUN" "node-$eli")"
	if [[ -z "${mpid:-}" || -z "${epid:-}" ]]; then
		echo "kill: 找不到主节点 pid（mini node-$mli / etcd node-$eli）" >&2
		wait "$FAIL_PIPE"
		exit 1
	fi
	date +%s%3N >"$RUN/killstamp"
	killms="$(cat "$RUN/killstamp")"
	{
		echo "kill	leader mini=node-$mli pid=$mpid etcd=node-$eli pid=$epid"
		echo "kill_ms	$killms"
	} | tee -a "$RUN/summary.tsv"
	kill -9 "$mpid" "$epid"
	wait "$FAIL_PIPE"
	wait_leaders || exit 1
	for kind in mini etcd; do
		got="$(first_leader_after "$kind" "$killms")"
		if [[ -n "$got" ]]; then
			echo "$kind	election	after_kill_ms=$(( ${got%% *} - killms )) node=${got#* }" | tee -a "$RUN/summary.tsv"
		else
			echo "$kind	election	after_kill_ms=none" | tee -a "$RUN/summary.tsv"
		fi
	done
fi

# 被 kill 的两个进程不参与后续档位：readback 走剩下四台里的新 Leader。
have_stage readback && run_phase readback

echo "== 产物：$RUN" >&2
