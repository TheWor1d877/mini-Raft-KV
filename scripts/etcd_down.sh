#!/usr/bin/env bash
# 收掉 etcd_up.sh 起的 5 个进程：按清单逐个 SIGTERM，等各自退出，最后删掉清单。
# 用法：scripts/etcd_down.sh [数据根目录=data] ；KEEP=1 保留 data/etcd 下的数据目录
# etcd 的退出码没有落盘的日志可读（进程日志走 stderr），所以这里只报「已退出」与「15 秒内没退出」。
# 这些进程不是本脚本的子进程（etcd_up.sh 一退出就被收养），拿不到 wait 的退出码。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DATA="${1:-$ROOT/data}"
STATE="$ROOT_DATA/etcd/etcd.up"

if [[ ! -f "$STATE" ]]; then
	echo "没有清单 $STATE，etcd 本来就没在跑" >&2
	exit 0
fi

total=0
dirty=0
printf 'node_id\tpid\texit\n'
while read -r id pid addr; do
	[[ -z "${pid:-}" ]] && continue
	total=$((total + 1))
	if ! kill -0 "$pid" 2>/dev/null; then
		printf '%s\t%s\t%s\n' "$id" "$pid" "already-gone"
		dirty=$((dirty + 1))
		continue
	fi
	kill -TERM "$pid"
	for _ in $(seq 1 300); do
		kill -0 "$pid" 2>/dev/null || break
		sleep 0.05
	done
	if kill -0 "$pid" 2>/dev/null; then
		echo "etcd $id 在 15 秒内没退出，kill -9" >&2
		kill -9 "$pid"
		printf '%s\t%s\t%s\n' "$id" "$pid" "killed"
		dirty=$((dirty + 1))
		continue
	fi
	printf '%s\t%s\t%s\n' "$id" "$pid" "exited"
done <"$STATE"

rm -f "$STATE"
# 独立命名空间模式下每台还有一个 sleep 占着命名空间（etcd_up.sh 的 veth 一端在它里面），
# etcd 收掉后再收它；命名空间随最后一个进程退出一起消失。
HOLDERS="$ROOT_DATA/etcd/etcd.holders"
if [[ -f "$HOLDERS" ]]; then
	while read -r hp; do
		[[ -n "${hp:-}" ]] && kill -TERM "$hp" 2>/dev/null || true
	done <"$HOLDERS"
	rm -f "$HOLDERS"
fi
[[ "${KEEP:-0}" == "1" ]] || rm -rf "$ROOT_DATA/etcd"
if [[ "$dirty" -gt 0 ]]; then
	echo "$dirty/$total 个 etcd 的收尾不干净" >&2
	exit 1
fi
