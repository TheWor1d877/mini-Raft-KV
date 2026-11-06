package main

// stats 档：把最近 N 轮 data/parity/<时间戳>/summary.tsv 合起来出分布。
// 每个 (目标, 档位, 字段) 收一列数：数值字段给 最小／中位／最大，文字字段给众数。
// 复跑多轮之后用它出数字，docs/8.1 的延迟与不可用时间取的就是这里的中位数。

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type acc struct {
	vals  []float64
	texts map[string]int
}

func runStats(base string, rounds int) {
	dirs, err := latestRuns(base, rounds)
	if err != nil {
		die("列出 %s: %v", base, err)
	}
	if len(dirs) == 0 {
		die("%s 下没有带 summary.tsv 的轮次", base)
	}
	m := map[string]*acc{}
	for _, d := range dirs {
		raw, err := os.ReadFile(filepath.Join(d, "summary.tsv"))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			parts := strings.Split(line, "\t")
			if len(parts) < 3 {
				continue
			}
			kind, phase := parts[0], parts[1]
			// 同一档的字段可能挤在一个 tab 格里、用空格分开（freeze/extra 那几行），先按空白拆再按等号拆。
			for _, field := range parts[2:] {
				for _, tok := range strings.Fields(field) {
					k, v, ok := strings.Cut(tok, "=")
					if !ok || k == "" || v == "" {
						continue
					}
					key := kind + "\t" + phase + "\t" + k
					a := m[key]
					if a == nil {
						a = &acc{texts: map[string]int{}}
						m[key] = a
					}
					if f, err := strconv.ParseFloat(v, 64); err == nil {
						a.vals = append(a.vals, f)
						continue
					}
					a.texts[v]++
				}
			}
		}
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Printf("rounds\t%d\t%s\n", len(dirs), strings.Join(dirs, ","))
	for _, k := range keys {
		a := m[k]
		if len(a.vals) > 0 {
			sort.Float64s(a.vals)
			n := len(a.vals)
			fmt.Printf("%s\tn=%d\tmin=%s\tmedian=%s\tmax=%s\n", k, n,
				numText(a.vals[0]), numText(a.vals[n/2]), numText(a.vals[n-1]))
			continue
		}
		var best string
		for v, c := range a.texts {
			if best == "" || c > a.texts[best] || (c == a.texts[best] && v < best) {
				best = v
			}
		}
		fmt.Printf("%s\tn=%d\tset=%s\tmode=%s\n", k, len(a.texts), textSet(a.texts), best)
	}
}

// latestRuns 取最新的 N 个带 summary.tsv 的轮次目录，按目录名的时间戳排序（名字就是时间戳）。
func latestRuns(base string, rounds int) ([]string, error) {
	ents, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}
	var dirs []string
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		d := filepath.Join(base, e.Name())
		if _, err := os.Stat(filepath.Join(d, "summary.tsv")); err == nil {
			dirs = append(dirs, d)
		}
	}
	sort.Strings(dirs)
	if rounds > 0 && len(dirs) > rounds {
		dirs = dirs[len(dirs)-rounds:]
	}
	return dirs, nil
}

func numText(f float64) string {
	if f == float64(int64(f)) {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'f', 2, 64)
}

// textSet 把文字字段的取值列成 k=v 串，按次数降序；取值太多时只列前四个。
func textSet(m map[string]int) string {
	type kv struct {
		k string
		c int
	}
	list := make([]kv, 0, len(m))
	for k, c := range m {
		list = append(list, kv{k, c})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].c != list[j].c {
			return list[i].c > list[j].c
		}
		return list[i].k < list[j].k
	})
	var b strings.Builder
	for i, e := range list {
		if i == 4 {
			b.WriteString(",...")
			break
		}
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "%s:%d", e.k, e.c)
	}
	return b.String()
}
