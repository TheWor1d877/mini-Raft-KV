#!/usr/bin/env bash
# 起 5 个 etcd 进程作对拍照臂（docs/8 第二节、docs/2.3 D11）：同一个二进制、每节点独立数据目录与两个 URL、不起容器。
# 用法：ETCD_BIN=~/tools/etcd-v3.7.0/etcd scripts/etcd_up.sh [起始端口=9801] [数据根目录=data] [日志级别=info]
# NODE_IPS=127.0.0.1,127.0.0.2,... 让每台绑不同地址，缺省全绑 127.0.0.1。
# ETCD_NETNS=1：每台一个独立网络命名空间，用 veth 接进同一个网桥，地址取 10.20.0.21-25。
# 单命名空间里 etcd 的出站 peer 连接源地址会被路由塌缩成 127.0.0.1，没法按地址把某一台单独切出去
# （见 scripts/parity.sh 的隔离注释）；独立命名空间后隔离规则下在它自己的空间里跑，只影响这一台。
# 只在本轮 unshare -rn 里用，主机上其它 etcd 不受影响。
# 参数配平：--heartbeat-interval 100 --election-timeout 1000 --snapshot-count 10000，
#           与 scripts/cluster_up.sh 的 mini-Raft 侧同一组值；缺 advertise 会让 bootstrap 直接 fatal，所以三个 URL 全给。
# 产出：每节点一行 node_id pid client_addr，写进 <数据根目录>/etcd/etcd.up，etcd_down.sh 读这份清单。
set -euo pipefail

PORT0="${1:-9801}"
LEVEL="${3:-info}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DATA="${2:-$ROOT/data}"
BIN="${ETCD_BIN:-$ROOT/bin/etcd}"
PEER_PORT0=$((PORT0 + 100))

if [[ ! -x "$BIN" ]]; then
	echo "找不到 $BIN，先编 etcd（docs/8 第三节），或 ETCD_BIN=/path/to/etcd 指过去" >&2
	exit 1
fi

DIR="$ROOT_DATA/etcd"
STATE="$DIR/etcd.up"
mkdir -p "$DIR"
if [[ -f "$STATE" ]]; then
	alive=0
	while read -r id pid addr; do
		[[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null && alive=$((alive + 1))
	done <"$STATE"
	if [[ "$alive" -gt 0 ]]; then
		echo "已有 $alive 个 etcd 在跑（清单 $STATE），先跑 scripts/etcd_down.sh" >&2
		exit 1
	fi
	rm -f "$STATE"
fi

NODES=5
NETNS="${ETCD_NETNS:-0}"
HOSTS=()
for ((i = 1; i <= NODES; i++)); do
	host="127.0.0.1"
	if [[ -n "${NODE_IPS:-}" ]]; then
		host="$(printf '%s' "$NODE_IPS" | cut -d, -f"$i")"
	fi
	[[ "$NETNS" == "1" ]] && host="10.20.0.2$i"
	HOSTS+=("$host")
done
CLUSTER=""
for ((i = 1; i <= NODES; i++)); do
	[[ -n "$CLUSTER" ]] && CLUSTER+=","
	CLUSTER+="node-$i=http://${HOSTS[$((i - 1))]}:$((PEER_PORT0 + i - 1))"
done
if [[ "$NETNS" == "1" ]]; then
	ip link add br0 type bridge 2>/dev/null || true
	ip addr add 10.20.0.1/24 dev br0 2>/dev/null || true
	ip link set br0 up
fi

: >"$STATE"

wait_ready() {
	local addr="$1" dir="$2" code
	for _ in $(seq 1 400); do
		code="$(curl -s -o /dev/null -w '%{http_code}' "http://$addr/version" || true)"
		[[ "$code" == "200" ]] && return 0
		sleep 0.05
	done
	echo "etcd 在 $addr 上没有就绪" >&2
	tail -8 "$dir/stderr.txt" >&2 || true
	return 1
}

# 五个进程先全部拉起再逐个探活：5 台要 3 台才选得出主，一台一等就永远等不到。
# 独立命名空间模式多一层：先起一个 sleep 占住命名空间，把 veth 一端挪进去、配好地址，
# 再用 nsenter 在同一命名空间里 exec etcd（nsenter 直接 exec，记下的 pid 就是 etcd 的）。
PIDS=()
HOLDERS=()
if [[ "$NETNS" == "1" ]]; then
	trap '[[ "${UP_OK:-0}" == "1" ]] || kill "${HOLDERS[@]}" 2>/dev/null || true' EXIT
fi
for ((i = 1; i <= NODES; i++)); do
	addr="${HOSTS[$((i - 1))]}:$((PORT0 + i - 1))"
	peer="${HOSTS[$((i - 1))]}:$((PEER_PORT0 + i - 1))"
	dir="$DIR/node-$i"
	mkdir -p "$dir"
	ARGS=(--name "node-$i" --data-dir "$dir"
		--listen-client-urls "http://$addr" --advertise-client-urls "http://$addr"
		--listen-peer-urls "http://$peer" --initial-advertise-peer-urls "http://$peer"
		--initial-cluster "$CLUSTER" --initial-cluster-state new
		--heartbeat-interval 100 --election-timeout 1000 --snapshot-count 10000
		--log-level "$LEVEL")
	# stdout 丢掉、stderr 单独收：与 cluster_up.sh 同一条口径，panic 只走 stderr。
	if [[ "$NETNS" == "1" ]]; then
		unshare -n -- sleep 86400 &
		holder=$!
		HOLDERS+=("$holder")
		ip link add "ve$i" type veth peer name "vp$i"
		ip link set "vp$i" netns "$holder"
		ip link set "ve$i" master br0 up
		nsenter -t "$holder" -n -- ip addr add "${HOSTS[$((i - 1))]}/24" dev "vp$i"
		nsenter -t "$holder" -n -- ip link set "vp$i" up
		nsenter -t "$holder" -n -- ip link set lo up
		nsenter -t "$holder" -n -- "$BIN" "${ARGS[@]}" >/dev/null 2>>"$dir/stderr.txt" &
	else
		"$BIN" "${ARGS[@]}" >/dev/null 2>>"$dir/stderr.txt" &
	fi
	PIDS+=("$!")
done
[[ "$NETNS" == "1" ]] && printf '%s\n' "${HOLDERS[@]}" >"$DIR/etcd.holders"

printf 'node_id\tpid\tclient_addr\n'
for ((i = 1; i <= NODES; i++)); do
	id="node-$i"
	addr="${HOSTS[$((i - 1))]}:$((PORT0 + i - 1))"
	pid="${PIDS[$((i - 1))]}"
	if ! wait_ready "$addr" "$DIR/$id"; then
		echo "第 $i 个 etcd 没起来，其余进程与清单保留" >&2
		exit 1
	fi
	printf '%s\t%s\t%s\n' "$id" "$pid" "$addr" >>"$STATE"
	printf '%s\t%s\t%s\n' "$id" "$pid" "$addr"
done

UP_OK=1
echo "etcd 版本：$("$BIN" --version | head -1)" >&2
echo "成员表：$CLUSTER" >&2
echo "清单：$STATE" >&2
