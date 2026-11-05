#!/usr/bin/env bash
# 崩溃注入：起单节点集群 → 写 N 条 → kill -9 → 重启 → 对账。
# 进程载体是 bin/miniraft-kv：成员表只有自己的单节点 raft 集群（docs/8 §三），
# 起来先停在 Follower，等满一个选举截止自票当选，
# 当选那一轮在日志尾部追加一条 no-op（3.1 §4），所以第一条客户端写落在 index 2。
# 用法：先 go build -o bin/miniraft-kv ./cmd/miniraft-kv
#       scripts/crash_inject.sh [写入条数=2000] [第几条后杀=全写完] [raft-fsync=true|false] [段上限=默认] [落点=wal]
# KEEP=1 保留数据目录并在最后打印路径（默认随退出删掉）。
# SNAP_PAUSE_MS=1500 快照落点每处提交停的毫秒数，只在落点不是 wal 时用
#
#   landing         kill 停在哪个提交窗口里，取值见下面的落点段
#   fsync           本次跑的 -raft-fsync 取值（true/false）
#   acked           客户端拿到 200 的写条数
#   durable         重启后恢复出来的写条数（取数见下）
#   not_durable     acked - durable；写路径每条一次 Sync 时必须等于 0
#   recovered       逐条读回且值对上的条数
#   lost            acked - recovered；必须等于 0
#   boot_ms         重启到就绪的耗时
#   segments        重启后段文件条数
#   stale_tmp       kill -9 之后、重启之前 日志与快照两个目录里 *.tmp 的条数
#   snap_index      重启后元数据指针里的 snapshotIndex，没有指针时是 0
#   crash_commit    kill 之后 raft/state 里的 commitIndex
#   restart_applied 重启后 state rebuilt from wal 那行的 last_applied
#
# 三个快照落点（5.2 §5 缺口表里「真掉电造不出来」那一格）：
#   snapshot-tmp   快照文件的临时名已经落盘、改名之前。窗口里 raft/snapshot/ 下有一个 <index>.snap.tmp。
#   before-pointer 快照文件已在正式名上、元数据指针改名之前。窗口里有 current.json.tmp 而没有 current.json。
#   during-drop    指针已提交、日志段删到一半。窗口里 current.json 在，段文件比手动快照之前少但没有删完。
#   wal            原有那一格：第 K 条写确认之后立刻杀，不参与快照。
# 那三处窗口本来只有几毫秒，shell 轮询追不上，所以进程侧有一个 MINIRAFT_SNAPSHOT_PAUSE_MS：
# 它给一个正数，三处提交点各停那么久（internal/raft/snapshot.go 的 crashPointPause）。
# 窗口拉开的是杀的时刻，不是恢复链路——重启走的路径与没有这个开关时同一条。真掉电仍然造不出来。
# 快照落点跑在全部写完之后的那一次手动快照上：写途中杀进快照窗口会留下一条已追加未确认的条目，
# durable 就比 acked 多一条，与崩溃注入表的口径对不上。所以这三个落点把第二个参数作废成全部写完。
# 整个装置把自动触发关死（-snapshot-entries 0 -snapshot-bytes 0），wal 档也一样：
# 崩溃注入表测的是写路径的落盘边界与段滚动，回放是全量回放（docs/0 的 M6 那一节），
# 自动快照掺进来就把「段数」和「boot_ms」两列改写成快照的读数了。快照的收益另有 bench_snapshot.sh。
# during-drop 要段文件多于一个才停得进删除循环，所以那一档的段上限默认 4096。
# 杀之前先 SIGSTOP 冻住进程，签名还在才 kill -9：停进窗口这件事就此有据，不靠运气。
#
# durable 的取数：只读重启那一段的进程日志（日志跨重启续写，所以用行号把区间卡住），
# 口径是「恢复出的 KV 写条数」，no-op 那一帧从头到尾都不进这个数：
# 快照指针的 entries 是快照点之前状态机里的键值条数，
# "state rebuilt from wal" 的 replayed_entries 只数快照点之后解出 key/value 的条目，
# 也就是 (snapshotIndex, lastApplied] 那段里的写；其后 (lastApplied, commitIndex] 的补 apply
# 只数 component=kv 的 "entry applied"（raft 侧那条同名的行对每个 index 都打，含 no-op）。
# 三者相加就是重启恢复出的写条数，与 acked 同量纲，not_durable 才等于「没落盘的确认写」。
# 三段互不重叠：指针管 <= snapshotIndex，回放管 (snapshotIndex, lastApplied]，补 apply 管其后。
#
# 持久性边界：Leader 每条一次 Sync（-raft-fsync=true），kill -9 之后已确认的写一条都不丢，
# 断言按 not_durable=0、lost=0，再加游标两条：crash_commit=acked+1（那条 no-op）与 restart_applied=crash_commit。
# -raft-fsync=false 时 Sync 整条跳过，进程被杀之后页缓存归内核持有，重启读同一份文件仍能看到
# 全部已确认字节，所以这一档也是 not_durable 0、lost 0；它只报数、不断言，差别要掉电才兑现。
#
# boot_ms 是恢复链路的耗时，快照在场时日志前缀已经删掉，回放量只覆盖快照点之后那一段（docs/5）。
# 读回用 ?consistency=local：默认档是线性一致读，无 Leader 期间回 503，会把对账变成读路径的测试。
set -euo pipefail
shopt -s nullglob

TOTAL="${1:-2000}"
KILL_AFTER="${2:-$TOTAL}"
FSYNC="${3:-true}"
MAX_SIZE="${4:-}"
LANDING="${5:-wal}"
PAUSE_MS="${SNAP_PAUSE_MS:-1500}"
# 落点等待的上界：三处提交点各停 SNAP_PAUSE_MS，手动快照的请求排到队首还要时间。
LAND_TIMEOUT="${LAND_TIMEOUT:-30}"

case "$FSYNC" in
	true | false) ;;
	*)
		echo "第三个参数是 -raft-fsync 的开关，只能填 true 或 false（收到 $FSYNC）" >&2
		exit 1
		;;
esac
case "$LANDING" in
	wal | snapshot-tmp | before-pointer | during-drop) ;;
	*)
		echo "第五个参数是崩溃落点，只能填 wal、snapshot-tmp、before-pointer、during-drop（收到 $LANDING）" >&2
		exit 1
		;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin/miniraft-kv"
if [[ ! -x "$BIN" ]]; then
	echo "找不到 $BIN，先跑：go build -o bin/miniraft-kv ./cmd/miniraft-kv" >&2
	exit 1
fi

# 目录名与扩展名各收在这里：一份日志只有这一处口径（docs/8 §一）。
SEG_DIR_REL="raft/wal"
SEG_EXT="wal"
SNAP_DIR_REL="raft/snapshot"
NODE_LOG_REL="log/miniraft-node.log"
STATE_REL="raft/state"

DATA="$(mktemp -d -t miniraft-crash-XXXXXX)"
NODE_LOG="$DATA/$NODE_LOG_REL"
SEG_DIR="$DATA/$SEG_DIR_REL"
SNAP_DIR="$DATA/$SNAP_DIR_REL"
POINTER="$SNAP_DIR/current.json"
STATE_FILE="$DATA/$STATE_REL"
# 端口留在临时端口区间（32768 起）之外，避免和出站连接抢同一个端口。
ADDR="127.0.0.1:$((20000 + RANDOM % 12000))"
ARGS=(-data-dir "$DATA" -addr "$ADDR" -raft-fsync="$FSYNC" -log-level debug
	-snapshot-entries 0 -snapshot-bytes 0)
if [[ "$LANDING" != "wal" ]]; then
	# 自动触发关死后，快照只有手动那一次，落点才停得进那一次的两步之间。
	KILL_AFTER="$TOTAL"
	if [[ "$LANDING" == "during-drop" ]]; then
		MAX_SIZE="${MAX_SIZE:-4096}"
	fi
fi
if [[ -n "$MAX_SIZE" ]]; then
	ARGS+=(-max-file-size "$MAX_SIZE")
fi

PROC=""
SNAP_CURL=""
cleanup() {
	if [[ -n "$SNAP_CURL" ]] && kill -0 "$SNAP_CURL" 2>/dev/null; then
		kill -9 "$SNAP_CURL" 2>/dev/null || true
	fi
	if [[ -n "$PROC" ]] && kill -0 "$PROC" 2>/dev/null; then
		kill -9 "$PROC" 2>/dev/null || true
	fi
	if [[ "${KEEP:-0}" != "1" ]]; then
		rm -rf "$DATA"
	fi
}
trap cleanup EXIT

# 一个目录里匹配某个后缀的条数：只用 glob 与数组，不起进程也不开子 shell，
# 才追得上毫秒级的窗口。结果放在 GLOB_COUNT 里，轮询侧直接读它。
count_by_glob() { # $1 = 目录，$2 = glob → GLOB_COUNT
	local -a hits=()
	if [[ -d "$1" ]]; then
		# 模式在这里要当 glob 展开，两侧都不加引号；目录是 mktemp 给的，不含空格。
		hits=($1/$2)
	fi
	GLOB_COUNT=${#hits[@]}
}

count_tmp() { # 两个目录里的 *.tmp 之和 → TMP_COUNT
	count_by_glob "$SEG_DIR" "*.$SEG_EXT.tmp"
	local wal_tmp=$GLOB_COUNT
	count_by_glob "$SNAP_DIR" '*.tmp'
	TMP_COUNT=$((wal_tmp + GLOB_COUNT))
}

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

start_node() {
	if [[ "$LANDING" == "wal" ]]; then
		"$BIN" "${ARGS[@]}" &
	else
		MINIRAFT_SNAPSHOT_PAUSE_MS="$PAUSE_MS" "$BIN" "${ARGS[@]}" &
	fi
	PROC=$!
	wait_ready
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

# 落点签名：磁盘上看得见的状态，与那一处提交窗口一一对应。
landing_seen() {
	case "$1" in
		snapshot-tmp)
			count_by_glob "$SNAP_DIR" '*.snap.tmp'
			[[ $GLOB_COUNT -gt 0 ]]
			;;
		before-pointer)
			count_by_glob "$SNAP_DIR" 'current.json.tmp'
			[[ $GLOB_COUNT -gt 0 ]] && [[ ! -e "$POINTER" ]] || return 1
			count_by_glob "$SNAP_DIR" '*.snap'
			[[ $GLOB_COUNT -gt 0 ]]
			;;
		during-drop)
			[[ -e "$POINTER" ]] || return 1
			count_by_glob "$SEG_DIR" "*.$SEG_EXT"
			# 少了一个以上却没删完：只剩当前段时是 1 个，那时删除循环已经走完了。
			[[ $GLOB_COUNT -lt $SEG_BEFORE && $GLOB_COUNT -gt 1 ]]
			;;
	esac
}

acked=0
start_node

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

if [[ "$LANDING" == "wal" ]]; then
	kill -9 "$PROC"
	wait "$PROC" 2>/dev/null || true
	PROC=""
else
	# 手动那一次快照的快照点是日志末尾：全部写都已确认且 apply 完，指针里的 entries 就是 acked。
	count_by_glob "$SEG_DIR" "*.$SEG_EXT"
	SEG_BEFORE=$GLOB_COUNT
	if [[ "$LANDING" == "during-drop" && "$SEG_BEFORE" -lt 3 ]]; then
		echo "during-drop 需要至少 3 个段文件才停得进删除循环，现在是 $SEG_BEFORE 个：加大写入条数或压低第四个参数" >&2
		exit 1
	fi
	curl -s -m 120 -o /dev/null -X POST "http://$ADDR/raft/snapshot" &
	SNAP_CURL=$!
	deadline=$((SECONDS + LAND_TIMEOUT))
	until landing_seen "$LANDING"; do
		if [[ $SECONDS -ge $deadline ]]; then
			echo "等了 $LAND_TIMEOUT 秒没出现 ${LANDING} 的签名：没停进那个窗口" >&2
			exit 1
		fi
		sleep 0.02
	done
	# 冻住再核一遍：签名还在，才说明杀的一刻进程确实停在这个窗口里。
	kill -STOP "$PROC"
	if ! landing_seen "$LANDING"; then
		echo "冻住进程后 ${LANDING} 的签名消失了，落点没停稳" >&2
		kill -9 "$PROC" 2>/dev/null || true
		exit 1
	fi
	kill -9 "$PROC"
	wait "$PROC" 2>/dev/null || true
	PROC=""
	wait "$SNAP_CURL" 2>/dev/null || true
	SNAP_CURL=""
fi

count_tmp
stale_tmp=$TMP_COUNT
count_by_glob "$SNAP_DIR" "*.snap"
crash_snap_files=$GLOB_COUNT
count_by_glob "$SEG_DIR" "*.$SEG_EXT"
crash_segments=$GLOB_COUNT
snap_index_before="$(jq -r '.snapshotIndex // 0' "$POINTER" 2>/dev/null || printf '0')"
crash_commit="$(jq -r '.commitIndex // 0' "$STATE_FILE")"
# boot_ms 取重启到就绪为止：恢复链路在 net.Listen 之前跑完（nodeapp.Run 的顺序），
# handler 一应答就说明回放已经到底，选举那一段不在读数里。
started=$(date +%s.%N)
start_node
ended=$(date +%s.%N)
boot_ms="$(awk -v a="$started" -v b="$ended" 'BEGIN { printf "%.1f", (b - a) * 1000 }')"

# 节点日志是 O_APPEND 续写的，两次启动的行都在同一个文件里，所以区间下界取最后一次
# state rebuilt from wal 的行号：重启那一次的读数全在它之后。
# 取行号而不是 kill 时刻的行数：kill 落在写日志的中间时，那一行会被重启的首行接上，成不了合法 JSON。
# `|| true`：grep 无匹配时返回 1，set -e 会让这行赋值直接退出，下面的报错永远打不出来。
log_mark=$(grep -n '"msg":"state rebuilt from wal"' "$NODE_LOG" | tail -1 | cut -d: -f1 || true)
if [[ -z "$log_mark" ]]; then
	echo "日志里没有 state rebuilt from wal，读数取不到" >&2
	exit 1
fi
rebuilt_line="$(sed -n "${log_mark}p" "$NODE_LOG")"
replayed="$(printf '%s' "$rebuilt_line" | jq -R 'fromjson? | .replayed_entries')"
restart_applied="$(printf '%s' "$rebuilt_line" | jq -R 'fromjson? | .last_applied')"
if [[ -z "$replayed" || "$replayed" == "null" ]]; then
	echo "第 $log_mark 行的 state rebuilt from wal 解不出 replayed_entries" >&2
	printf '%s\n' "$rebuilt_line" >&2
	exit 1
fi
# (lastApplied, commitIndex] 的补 apply 逐条打 entry applied，这一行的 replayed_entries 是快照点之后那段的写。
# 只数 component=kv 的那条：raft 侧同名的行按 index 逐条打，no-op 也占一条，算进 durable 就对不上 acked。
# jq -Rs 配 fromjson? 跳过解不动的行（比如首尾被截断的半行）。
extra="$(tail -n +"$((log_mark + 1))" "$NODE_LOG" |
	jq -Rs --arg m 'entry applied' 'split("\n") | map(fromjson?) |
		map(select(.msg == $m and .component == "kv")) | length')"
# 指针在重启后还读得到才算数：落点一与落点二都没有提交指针，那一格是 0。
snap_entries="$(jq -r '.entries // 0' "$POINTER" 2>/dev/null || printf '0')"
snap_index="$(jq -r '.snapshotIndex // 0' "$POINTER" 2>/dev/null || printf '0')"
durable=$((snap_entries + replayed + extra))
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
count_by_glob "$SEG_DIR" "*.$SEG_EXT"
segments=$GLOB_COUNT
count_by_glob "$SNAP_DIR" "*.snap"
snap_files=$GLOB_COUNT
count_tmp
tmp_after_start=$TMP_COUNT

printf 'landing fsync acked durable not_durable recovered lost boot_ms segments stale_tmp snap_index crash_commit restart_applied\n'
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
	"$LANDING" "$FSYNC" "$acked" "$durable" "$not_durable" "$recovered" "$lost" \
	"$boot_ms" "$segments" "$stale_tmp" "$snap_index" "$crash_commit" "$restart_applied"

# 读数指向的原文行：每条落点都把证据打在 stderr 上，docs/0 的表从这里抄。
echo "  ${LANDING} 档的日志：$NODE_LOG" >&2
echo "    state rebuilt from wal（重启窗口第 $log_mark 行）：$rebuilt_line" >&2
echo "    快照指针：entries=$snap_entries snapshotIndex=$snap_index，目录里正式快照文件 $snap_files 份" >&2
echo "    kill 之后：raft/state 的 commitIndex=$crash_commit，*.tmp=$stale_tmp，重启后 *.tmp=$tmp_after_start" >&2
echo "    kill 之后：段文件 $crash_segments 个（手动快照之前 $SEG_BEFORE 个），正式快照文件 $crash_snap_files 份，指针 snapshotIndex=${snap_index_before:-0}" >&2
for event in 'snapshot committed' 'wal prefix dropped'; do
	line="$(grep "\"msg\":\"$event\"" "$NODE_LOG" | tail -1 || true)"
	echo "    $event：${line:-（没有）}" >&2
done

if [[ "$acked" -eq 0 ]]; then
	# acked=0 就没什么可对账：写路径一条回执都没给（HTTP 码在上面的 stderr 里）。
	echo "acked=0：一条写都没确认，后面的读数都是空的" >&2
	exit 1
fi
if [[ "$FSYNC" == "true" ]]; then
	# 单节点档的写边界：Leader 每条一次 Sync，已确认的一条都不该留在日志之外。
	if [[ "$not_durable" -ne 0 ]]; then
		echo "未落盘 $not_durable 条不等于 0：已确认的写没进日志或快照，与每条一次 Sync 的边界不符" >&2
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
# 游标两条：单节点档第一条客户端写落在 index 2，所以提交点是 acked+1；重启后的 apply 点不后退。
if [[ "$crash_commit" -ne $((acked + 1)) ]]; then
	echo "kill 后的 commitIndex $crash_commit 不等于 acked+1 = $((acked + 1))" >&2
	exit 1
fi
if [[ "$restart_applied" -ne "$crash_commit" ]]; then
	echo "重启后的 lastApplied $restart_applied 不等于 kill 时的 commitIndex $crash_commit：游标没对上" >&2
	exit 1
fi
# 崩溃留下的临时名文件与没有指针指向的快照文件，启动都要清掉（5.1 §7 启动恢复第 1 步）。
if [[ "$tmp_after_start" -ne 0 ]]; then
	echo "重启后还有 $tmp_after_start 个 *.tmp 没被启动清理删掉" >&2
	exit 1
fi
if [[ "$snap_files" -ne $((snap_index > 0 ? 1 : 0)) ]]; then
	echo "快照目录里 $snap_files 份正式文件，指针的 snapshotIndex=$snap_index：两者该同生同灭" >&2
	exit 1
fi
if [[ "$LANDING" != "wal" && "$snap_entries" -gt 0 && "$snap_entries" -ne "$acked" ]]; then
	echo "指针里的 entries $snap_entries 不等于确认条数 $acked：快照点没落在负载末尾" >&2
	exit 1
fi
if [[ "${KEEP:-0}" == "1" ]]; then
	echo "数据目录保留在 $DATA"
fi
