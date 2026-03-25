#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
解析 Flink TaskExecutor 日志，提取 L2 缓存统计指标
按 subtask 分组统计，避免交错混合导致的伪波动
"""

import os
import re
import glob
from collections import defaultdict


STRATEGY_PATTERN = re.compile(r'\[(\w[\w-]*)\]\s+(?:\[S\d\]\s+)?(?:L2缓存统计|subtask)')
SUBTASK_PATTERN = re.compile(r'\[S(\d+)\]')
HITRATE_PATTERN = re.compile(r'hitRate=([\d.]+)%')
HIT_PATTERN = re.compile(r'(?<!\w)hit=(\d+)')
MISS_PATTERN = re.compile(r'miss=(\d+)')
TIER_PATTERN = re.compile(r'T(\d)=([\d.]+)%\((\d+)/(\d+)\)')
FILENAME_STRATEGY_PATTERN = re.compile(
    r'^(fifo|lru-k|lru|lfu|w-tinylfu)_', re.IGNORECASE)


def is_l2_line(line):
    """判断是否是 L2 缓存相关的日志行"""
    if 'L2缓存统计' in line:
        return True
    if 'L2:' in line:
        if 'L3缓存' in line or 'L3:' in line:
            return False
        return True
    if 'subtask' in line and '关闭' in line and 'FeatureExtraction' in line:
        return True
    return False


def parse_single_log(filepath):
    """解析单个日志文件，按 subtask 分组"""
    strategy = None
    subtask_series = defaultdict(list)  # subtask_id → [hitRate, ...]
    subtask_final = []
    periodic_records = []

    basename = os.path.basename(filepath)
    fn_match = FILENAME_STRATEGY_PATTERN.match(basename)
    if fn_match:
        strategy = fn_match.group(1).upper()

    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if not is_l2_line(line):
                    continue

                if strategy is None:
                    s_match = STRATEGY_PATTERN.search(line)
                    if s_match:
                        strategy = s_match.group(1).upper()

                is_close_line = 'subtask' in line and '关闭' in line

                # 提取 subtask 编号
                st_match = SUBTASK_PATTERN.search(line)
                subtask_id = int(st_match.group(1)) if st_match else -1

                hr_match = HITRATE_PATTERN.search(line)
                hit_m = HIT_PATTERN.search(line)
                miss_m = MISS_PATTERN.search(line)
                tier_matches = TIER_PATTERN.findall(line)

                line_data = {
                    'hit': int(hit_m.group(1)) if hit_m else 0,
                    'miss': int(miss_m.group(1)) if miss_m else 0,
                    'hitRate': float(hr_match.group(1)) if hr_match else 0.0,
                    'subtask': subtask_id,
                    'tierHits': {'T1': 0, 'T2': 0, 'T3': 0, 'T4': 0},
                    'tierTotals': {'T1': 0, 'T2': 0, 'T3': 0, 'T4': 0},
                }
                for tier_num, rate_str, hits, total in tier_matches:
                    tier_key = f'T{tier_num}'
                    if tier_key in line_data['tierHits']:
                        line_data['tierHits'][tier_key] = int(hits)
                        line_data['tierTotals'][tier_key] = int(total)

                if is_close_line:
                    subtask_final.append(line_data)
                else:
                    if hr_match:
                        subtask_series[subtask_id].append(float(hr_match.group(1)))
                    periodic_records.append(line_data)

    except Exception as e:
        print(f"  [ERROR] 解析 {filepath} 失败: {e}")
        return None

    if not subtask_series and not subtask_final:
        return None

    # 计算按时间步平均的命中率序列
    averaged_series = []
    if subtask_series:
        valid_series = {k: v for k, v in subtask_series.items() if v}
        if valid_series:
            min_len = min(len(v) for v in valid_series.values())
            for i in range(min_len):
                avg = sum(v[i] for v in valid_series.values()) / len(valid_series)
                averaged_series.append(round(avg, 2))

    return {
        'strategy': strategy or 'UNKNOWN',
        'hitRates': averaged_series,
        'subtask_final': subtask_final,
        'periodic_records': periodic_records,
        'subtask_count': len(subtask_series),
    }


def parse_all_logs(results_dir):
    """解析目录下所有日志文件，按策略聚合"""
    all_files = glob.glob(os.path.join(results_dir, '*.log'))

    if not all_files:
        print(f"[WARN] 未找到日志文件: {results_dir}")
        return None

    print(f"找到 {len(all_files)} 个日志文件")

    strategy_data = defaultdict(lambda: {
        'hitRates_all': [],
        'subtask_finals': [],
        'periodic_records': [],
        'file_count': 0,
    })

    for filepath in sorted(all_files):
        result = parse_single_log(filepath)
        basename = os.path.basename(filepath)

        if result is None:
            print(f"  解析: {basename} → 无 L2 缓存统计数据")
            continue

        strategy = result['strategy']
        n_rates = len(result['hitRates'])
        n_close = len(result['subtask_final'])
        n_st = result['subtask_count']
        print(f"  解析: {basename} → 策略={strategy}, "
              f"subtask={n_st}, 平均序列={n_rates}点, 关闭记录={n_close}")

        data = strategy_data[strategy]
        data['hitRates_all'].extend(result['hitRates'])
        data['subtask_finals'].extend(result['subtask_final'])
        data['periodic_records'].extend(result['periodic_records'])
        data['file_count'] += 1

    if not strategy_data:
        return None

    final_results = {}
    for strategy, data in strategy_data.items():
        hit_rates = data['hitRates_all']
        subtask_finals = data['subtask_finals']
        periodic_records = data['periodic_records']

        if not hit_rates and not subtask_finals and not periodic_records:
            continue

        # 选择数据源
        if subtask_finals:
            source = subtask_finals
            source_name = f"{len(subtask_finals)} 个 subtask 关闭记录"
        elif periodic_records:
            n_take = min(4, len(periodic_records))
            source = periodic_records[-n_take:]
            source_name = f"最后 {n_take} 条周期记录（回退）"
        else:
            source = []
            source_name = "无数据"

        # 总命中率
        if source:
            total_hit = sum(s['hit'] for s in source)
            total_miss = sum(s['miss'] for s in source)
            total_all = total_hit + total_miss
            if total_all > 0:
                avg_hit_rate = total_hit / total_all * 100
            else:
                avg_hit_rate = 0
            print(f"  {strategy} 总命中率（{source_name}）: "
                  f"hit={total_hit}, miss={total_miss}, rate={avg_hit_rate:.2f}%")
        elif hit_rates:
            stable_count = max(1, len(hit_rates) // 3)
            avg_hit_rate = sum(hit_rates[-stable_count:]) / stable_count
        else:
            avg_hit_rate = 0

        # 各层命中率
        tier_final_rates = {}
        for tier in ['T1', 'T2', 'T3', 'T4']:
            total_tier_hits = sum(s['tierHits'][tier] for s in source)
            total_tier_total = sum(s['tierTotals'][tier] for s in source)
            if total_tier_total > 0:
                tier_final_rates[tier] = round(
                    total_tier_hits / total_tier_total * 100, 1)
            else:
                tier_final_rates[tier] = 0.0
            if total_tier_total > 0:
                print(f"    {tier}: hit={total_tier_hits}, "
                      f"total={total_tier_total}, "
                      f"rate={tier_final_rates[tier]}%")

        series = hit_rates

        final_results[strategy] = {
            'hitRate': round(avg_hit_rate, 1),
            'tierRates': tier_final_rates,
            'hitrate_series': series,
        }

        print(f"\n  {strategy} 最终: hitRate={avg_hit_rate:.1f}%, "
              f"tiers={tier_final_rates}, "
              f"平均序列={len(hit_rates)}点, "
              f"日志文件={data['file_count']}")

    return final_results if final_results else None
