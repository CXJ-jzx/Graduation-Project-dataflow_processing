#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
论文级实验结果可视化脚本
========================
为三组实验生成直观、美观的对比图，适合直接插入论文。

用法:
  python plot_thesis.py                          # 生成全部
  python plot_thesis.py --scenario scale_up      # 只生成扩容测试图
  python plot_thesis.py --scenario scale_down0
  python plot_thesis.py --scenario fluctuate
  python plot_thesis.py --summary                # 只生成总结对比图
"""

import os
import csv
import argparse
import numpy as np
from collections import OrderedDict

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.ticker import MaxNLocator

# ============================================================
# 全局样式配置
# ============================================================
plt.rcParams.update({
    'font.sans-serif': ['SimHei', 'Microsoft YaHei', 'Arial', 'DejaVu Sans'],
    'axes.unicode_minus': False,
    'font.size': 13,
    'axes.titlesize': 15,
    'axes.titleweight': 'bold',
    'axes.labelsize': 13,
    'xtick.labelsize': 11,
    'ytick.labelsize': 11,
    'legend.fontsize': 11,
    'figure.dpi': 150,
    'savefig.dpi': 200,
    'savefig.bbox': 'tight',
    'savefig.pad_inches': 0.15,
    'axes.grid': True,
    'grid.alpha': 0.25,
    'grid.linestyle': '--',
    'lines.linewidth': 2.0,
    'axes.spines.top': False,
    'axes.spines.right': False,
})

# 配色方案
C_FIXED   = '#1565C0'   # 深蓝
C_DS2     = '#C62828'   # 深红
C_TARGET  = '#2E7D32'   # 深绿
C_PHASE   = '#757575'   # 灰
C_FILL_F  = '#BBDEFB'   # 浅蓝填充
C_FILL_D  = '#FFCDD2'   # 浅红填充

# 算子配色
OP_COLORS = {
    'Source':  '#7B1FA2',   # 紫
    'Filter':  '#2E7D32',   # 绿
    'Signal':  '#1565C0',   # 蓝
    'Feature': '#E65100',   # 橙
    'Grid':    '#C62828',   # 红
    'Sink':    '#4E342E',   # 棕
}

DATA_ROOT = "experiment_results_p10"

# ============================================================
# 数据加载工具
# ============================================================

def load_csv(path: str) -> list:
    if not os.path.exists(path):
        return []
    with open(path, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def extract_source_rate(data: list):
    """提取 Source 算子的输出速率时间序列"""
    times, values = [], []
    for row in data:
        if 'Source' in row.get('vertex_name', ''):
            try:
                t = float(row['elapsed_s']) / 60.0
                v = float(row['out_rate'])
                times.append(t)
                values.append(v)
            except (ValueError, TypeError, KeyError):
                pass
    return np.array(times), np.array(values)


def extract_slot_usage(data: list):
    """提取 Slot 使用量时间序列（去重）"""
    time_slots = OrderedDict()
    for row in data:
        try:
            t = round(float(row['elapsed_s']), 0)
            s = int(row['slot_usage'])
            time_slots[t] = s
        except (ValueError, TypeError, KeyError):
            pass
    if not time_slots:
        return np.array([]), np.array([])
    return (np.array([k / 60.0 for k in time_slots.keys()]),
            np.array(list(time_slots.values())))


def extract_operator_parallelism(data: list):
    """提取各算子并行度时间序列"""
    ops = OrderedDict()
    for row in data:
        vname = row.get('vertex_name', '')
        # 取算子短名
        for key in OP_COLORS:
            if key in vname:
                try:
                    t = float(row['elapsed_s']) / 60.0
                    p = int(row['parallelism'])
                    if key not in ops:
                        ops[key] = ([], [])
                    ops[key][0].append(t)
                    ops[key][1].append(p)
                except (ValueError, TypeError, KeyError):
                    pass
                break
    # 去重：同一时间点同一算子只保留最后一个值
    result = {}
    for key, (ts, ps) in ops.items():
        seen = OrderedDict()
        for t, p in zip(ts, ps):
            seen[round(t, 4)] = p
        result[key] = (np.array(list(seen.keys())),
                       np.array(list(seen.values())))
    return result


def extract_busy_ratio(data: list):
    """提取各算子繁忙度时间序列"""
    ops = OrderedDict()
    for row in data:
        vname = row.get('vertex_name', '')
        for key in ['Filter', 'Signal', 'Feature', 'Grid']:
            if key in vname:
                try:
                    t = float(row['elapsed_s']) / 60.0
                    b = float(row['busy_ratio'])
                    if key not in ops:
                        ops[key] = ([], [])
                    ops[key][0].append(t)
                    ops[key][1].append(b)
                except (ValueError, TypeError, KeyError):
                    pass
                break
    return {k: (np.array(ts), np.array(vs)) for k, (ts, vs) in ops.items()}


def smooth(values, window=5):
    """简单移动平均平滑"""
    if len(values) < window:
        return values
    kernel = np.ones(window) / window
    return np.convolve(values, kernel, mode='same')


# ============================================================
# 单场景绘图
# ============================================================

def plot_scale_up(output_dir: str):
    """扩容测试：生成3张独立图"""
    fixed = load_csv(os.path.join(output_dir, "fixed.csv"))
    ds2 = load_csv(os.path.join(output_dir, "ds2.csv"))

    if not fixed and not ds2:
        print("[WARN] scale_up: no data")
        return

    # ========== 图1: 吞吐量对比 ==========
    fig, ax = plt.subplots(figsize=(10, 5))

    if fixed:
        t, v = extract_source_rate(fixed)
        if len(t) > 0:
            ax.plot(t, smooth(v), color=C_FIXED, alpha=0.85,
                    label='Fixed (P=4)', linewidth=2)

    if ds2:
        t, v = extract_source_rate(ds2)
        if len(t) > 0:
            ax.plot(t, smooth(v), color=C_DS2, alpha=0.85,
                    label='DS2 (P=2起步)', linewidth=2)

    # 目标线
    max_t = max(
        (extract_source_rate(fixed)[0].max() if fixed and len(extract_source_rate(fixed)[0]) > 0 else 0),
        (extract_source_rate(ds2)[0].max() if ds2 and len(extract_source_rate(ds2)[0]) > 0 else 0),
    )
    ax.axhline(y=6000, color=C_TARGET, linestyle='--', alpha=0.6,
               linewidth=1.5, label='目标速率 (6000 r/s)')

    ax.set_xlabel('时间 (min)')
    ax.set_ylabel('Source 输出速率 (records/s)')
    ax.set_title('扩容测试 — 吞吐量对比')
    ax.legend(loc='upper right', framealpha=0.9)
    ax.set_ylim(bottom=-200)

    plt.tight_layout()
    path = os.path.join(output_dir, "fig_scaleup_throughput.png")
    plt.savefig(path)
    plt.close()
    print(f"[OK] {path}")

    # ========== 图2: Slot 使用量对比 ==========
    fig, ax = plt.subplots(figsize=(10, 4.5))

    if fixed:
        t, v = extract_slot_usage(fixed)
        if len(t) > 0:
            ax.fill_between(t, v, alpha=0.2, color=C_FIXED, step='post')
            ax.step(t, v, where='post', color=C_FIXED, linewidth=2.5,
                    label=f'Fixed (恒定 {v[0]} Slot)')

    if ds2:
        t, v = extract_slot_usage(ds2)
        if len(t) > 0:
            ax.fill_between(t, v, alpha=0.2, color=C_DS2, step='post')
            ax.step(t, v, where='post', color=C_DS2, linewidth=2.5,
                    label=f'DS2 (动态 {int(min(v))}~{int(max(v))} Slot)')

    ax.set_xlabel('时间 (min)')
    ax.set_ylabel('Slot 使用量')
    ax.set_title('扩容测试 — 资源使用量对比')
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.legend(loc='upper right', framealpha=0.9)
    ax.set_ylim(bottom=0)

    plt.tight_layout()
    path = os.path.join(output_dir, "fig_scaleup_slots.png")
    plt.savefig(path)
    plt.close()
    print(f"[OK] {path}")

    # ========== 图3: DS2 算子并行度变化 ==========
    if ds2:
        fig, ax = plt.subplots(figsize=(10, 4.5))
        ops = extract_operator_parallelism(ds2)

        for op_name, (t, p) in ops.items():
            color = OP_COLORS.get(op_name, 'gray')
            ax.step(t, p, where='post', color=color, linewidth=2,
                    alpha=0.8, label=op_name)

        ax.set_xlabel('时间 (min)')
        ax.set_ylabel('并行度')
        ax.set_title('扩容测试 — DS2 各算子并行度动态变化')
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        ax.legend(loc='upper right', framealpha=0.9, ncol=2)
        ax.set_ylim(bottom=0)

        plt.tight_layout()
        path = os.path.join(output_dir, "fig_scaleup_parallelism.png")
        plt.savefig(path)
        plt.close()
        print(f"[OK] {path}")


def plot_scale_down(output_dir: str):
    """缩容测试：生成3张独立图"""
    fixed = load_csv(os.path.join(output_dir, "fixed.csv"))
    ds2 = load_csv(os.path.join(output_dir, "ds2.csv"))

    if not fixed and not ds2:
        print("[WARN] scale_down: no data")
        return

    # ========== 图1: 吞吐量对比 ==========
    fig, ax = plt.subplots(figsize=(10, 5))

    if fixed:
        t, v = extract_source_rate(fixed)
        if len(t) > 0:
            ax.plot(t, smooth(v), color=C_FIXED, alpha=0.85,
                    label='Fixed (P=6)', linewidth=2)
    if ds2:
        t, v = extract_source_rate(ds2)
        if len(t) > 0:
            ax.plot(t, smooth(v), color=C_DS2, alpha=0.85,
                    label='DS2 (P=6起步)', linewidth=2)

    ax.axhline(y=2000, color=C_TARGET, linestyle='--', alpha=0.6,
               linewidth=1.5, label='目标速率 (2000 r/s)')

    ax.set_xlabel('时间 (min)')
    ax.set_ylabel('Source 输出速率 (records/s)')
    ax.set_title('缩容测试 — 吞吐量对比')
    ax.legend(loc='upper right', framealpha=0.9)
    ax.set_ylim(bottom=-200)

    plt.tight_layout()
    path = os.path.join(output_dir, "fig_scaledown_throughput.png")
    plt.savefig(path)
    plt.close()
    print(f"[OK] {path}")

    # ========== 图2: 资源消耗面积对比（核心图）==========
    fig, ax = plt.subplots(figsize=(10, 5))

    if fixed:
        t, v = extract_slot_usage(fixed)
        if len(t) > 0:
            ax.fill_between(t, v, alpha=0.35, color=C_FIXED, step='post',
                            label=f'Fixed 资源消耗 (恒定 {v[0]} Slot)')
            ax.step(t, v, where='post', color=C_FIXED, linewidth=2)

    if ds2:
        t, v = extract_slot_usage(ds2)
        if len(t) > 0:
            ax.fill_between(t, v, alpha=0.35, color=C_DS2, step='post',
                            label=f'DS2 资源消耗 (最终 {int(min(v))} Slot)')
            ax.step(t, v, where='post', color=C_DS2, linewidth=2)

    # 标注节省区域
    ax.annotate('资源节省 46.9%', xy=(0.65, 0.7),
                xycoords='axes fraction', fontsize=14,
                fontweight='bold', color=C_DS2,
                bbox=dict(boxstyle='round,pad=0.4', facecolor='white',
                          edgecolor=C_DS2, alpha=0.9))

    ax.set_xlabel('时间 (min)')
    ax.set_ylabel('Slot 使用量')
    ax.set_title('缩容测试 — 资源消耗对比 (面积 = slot·min)')
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.legend(loc='upper left', framealpha=0.9)
    ax.set_ylim(bottom=0)

    plt.tight_layout()
    path = os.path.join(output_dir, "fig_scaledown_resource.png")
    plt.savefig(path)
    plt.close()
    print(f"[OK] {path}")

    # ========== 图3: DS2 算子并行度变化 ==========
    if ds2:
        fig, ax = plt.subplots(figsize=(10, 4.5))
        ops = extract_operator_parallelism(ds2)

        for op_name, (t, p) in ops.items():
            color = OP_COLORS.get(op_name, 'gray')
            ax.step(t, p, where='post', color=color, linewidth=2,
                    alpha=0.8, label=op_name)

        ax.set_xlabel('时间 (min)')
        ax.set_ylabel('并行度')
        ax.set_title('缩容测试 — DS2 各算子并行度动态变化')
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        ax.legend(loc='upper right', framealpha=0.9, ncol=2)
        ax.set_ylim(bottom=0)

        plt.tight_layout()
        path = os.path.join(output_dir, "fig_scaledown_parallelism.png")
        plt.savefig(path)
        plt.close()
        print(f"[OK] {path}")


def plot_fluctuate(output_dir: str):
    """综合测试：生成4张独立图"""
    fixed = load_csv(os.path.join(output_dir, "fixed.csv"))
    ds2 = load_csv(os.path.join(output_dir, "ds2.csv"))

    if not fixed and not ds2:
        print("[WARN] fluctuate: no data")
        return

    phase_boundary = 10.0  # Phase 1 结束时间 (min)

    def add_phase_markers(ax):
        """添加阶段分界线和标注"""
        ymin, ymax = ax.get_ylim()
        ax.axvline(x=phase_boundary, color=C_PHASE, linestyle=':',
                   linewidth=2, alpha=0.5)
        ax.text(phase_boundary / 2, ymax * 0.95, '高负载阶段\n6000 r/s',
                ha='center', va='top', fontsize=10, color=C_PHASE,
                fontweight='bold', fontstyle='italic')
        ax.text(phase_boundary + (20 - phase_boundary) / 2, ymax * 0.95,
                '低负载阶段\n2000 r/s',
                ha='center', va='top', fontsize=10, color=C_PHASE,
                fontweight='bold', fontstyle='italic')

    # ========== 图1: 吞吐量对比（核心图）==========
    fig, ax = plt.subplots(figsize=(12, 5.5))

    if fixed:
        t, v = extract_source_rate(fixed)
        if len(t) > 0:
            ax.plot(t, smooth(v, 3), color=C_FIXED, alpha=0.8,
                    label='Fixed (P=4)', linewidth=2)
    if ds2:
        t, v = extract_source_rate(ds2)
        if len(t) > 0:
            ax.plot(t, smooth(v, 3), color=C_DS2, alpha=0.8,
                    label='DS2 (P=2起步)', linewidth=2)

    # 目标线
    ax.hlines(6000, 0, phase_boundary, colors=C_TARGET,
              linestyles='--', alpha=0.5, linewidth=1.5)
    ax.hlines(2000, phase_boundary, 20, colors=C_TARGET,
              linestyles='--', alpha=0.5, linewidth=1.5)
    ax.plot([], [], color=C_TARGET, linestyle='--', alpha=0.5,
            label='目标速率')

    ax.set_xlabel('时间 (min)')
    ax.set_ylabel('Source 输出速率 (records/s)')
    ax.set_title('综合测试 — 负载波动下的吞吐量对比')
    ax.legend(loc='upper right', framealpha=0.9, fontsize=12)
    ax.set_ylim(bottom=-500)
    add_phase_markers(ax)

    plt.tight_layout()
    path = os.path.join(output_dir, "fig_fluctuate_throughput.png")
    plt.savefig(path)
    plt.close()
    print(f"[OK] {path}")

    # ========== 图2: 资源消耗面积对比 ==========
    fig, ax = plt.subplots(figsize=(12, 5))

    if fixed:
        t, v = extract_slot_usage(fixed)
        if len(t) > 0:
            ax.fill_between(t, v, alpha=0.3, color=C_FIXED, step='post')
            ax.step(t, v, where='post', color=C_FIXED, linewidth=2.5,
                    label=f'Fixed (恒定 {v[0]} Slot)')
    if ds2:
        t, v = extract_slot_usage(ds2)
        if len(t) > 0:
            ax.fill_between(t, v, alpha=0.3, color=C_DS2, step='post')
            ax.step(t, v, where='post', color=C_DS2, linewidth=2.5,
                    label=f'DS2 (动态 {int(min(v))}~{int(max(v))} Slot)')

    ax.annotate('资源节省 41.2%', xy=(0.75, 0.8),
                xycoords='axes fraction', fontsize=14,
                fontweight='bold', color=C_DS2,
                bbox=dict(boxstyle='round,pad=0.4', facecolor='white',
                          edgecolor=C_DS2, alpha=0.9))

    ax.set_xlabel('时间 (min)')
    ax.set_ylabel('Slot 使用量')
    ax.set_title('综合测试 — 资源消耗对比')
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.legend(loc='upper left', framealpha=0.9)
    ax.set_ylim(bottom=0)
    add_phase_markers(ax)

    plt.tight_layout()
    path = os.path.join(output_dir, "fig_fluctuate_resource.png")
    plt.savefig(path)
    plt.close()
    print(f"[OK] {path}")

    # ========== 图3: DS2 算子并行度变化 ==========
    if ds2:
        fig, ax = plt.subplots(figsize=(12, 4.5))
        ops = extract_operator_parallelism(ds2)

        for op_name, (t, p) in ops.items():
            color = OP_COLORS.get(op_name, 'gray')
            ax.step(t, p, where='post', color=color, linewidth=2,
                    alpha=0.8, label=op_name)

        ax.axvline(x=phase_boundary, color=C_PHASE, linestyle=':',
                   linewidth=2, alpha=0.5)

        ax.set_xlabel('时间 (min)')
        ax.set_ylabel('并行度')
        ax.set_title('综合测试 — DS2 各算子并行度动态变化')
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        ax.legend(loc='upper right', framealpha=0.9, ncol=3)
        ax.set_ylim(bottom=0)

        # 标注扩容和缩容
        ymax = ax.get_ylim()[1]
        ax.annotate('扩容阶段', xy=(phase_boundary * 0.4, ymax * 0.9),
                    fontsize=11, color='#E65100', fontweight='bold',
                    ha='center')
        ax.annotate('缩容阶段', xy=(phase_boundary * 1.5, ymax * 0.9),
                    fontsize=11, color='#1565C0', fontweight='bold',
                    ha='center')

        plt.tight_layout()
        path = os.path.join(output_dir, "fig_fluctuate_parallelism.png")
        plt.savefig(path)
        plt.close()
        print(f"[OK] {path}")

    # ========== 图4: 分阶段效率柱状图 ==========
    fig, axes = plt.subplots(1, 3, figsize=(14, 5))

    # 柱状图数据
    bar_data = {
        '平均吞吐量\n(r/s)': {
            'Phase 1\n(6000 r/s)': (11671.9, 12007.3),
            'Phase 2\n(2000 r/s)': (4071.3, 4392.3),
        },
        '资源消耗\n(slot·min)': {
            'Phase 1': (40.0, 34.3),
            'Phase 2': (40.0, 31.2),
        },
        '资源效率\n(r/s/slot)': {
            'Phase 1': (2918.0, 3439.0),
            'Phase 2': (1017.8, 1382.1),
        },
    }

    for idx, (title, phases) in enumerate(bar_data.items()):
        ax = axes[idx]
        labels = list(phases.keys())
        fixed_vals = [phases[l][0] for l in labels]
        ds2_vals = [phases[l][1] for l in labels]

        x = np.arange(len(labels))
        w = 0.32
        bars_f = ax.bar(x - w/2, fixed_vals, w, color=C_FIXED,
                        alpha=0.85, label='Fixed', edgecolor='white')
        bars_d = ax.bar(x + w/2, ds2_vals, w, color=C_DS2,
                        alpha=0.85, label='DS2', edgecolor='white')

        # 数值标注
        for bar in bars_f:
            h = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2, h + h*0.02,
                    f'{h:.0f}', ha='center', va='bottom', fontsize=9,
                    color=C_FIXED, fontweight='bold')
        for bar in bars_d:
            h = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2, h + h*0.02,
                    f'{h:.0f}', ha='center', va='bottom', fontsize=9,
                    color=C_DS2, fontweight='bold')

        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.set_title(title, fontsize=13)
        if idx == 0:
            ax.legend(framealpha=0.9)

    fig.suptitle('综合测试 — 分阶段指标对比', fontsize=15, fontweight='bold')
    plt.tight_layout()
    path = os.path.join(output_dir, "fig_fluctuate_bars.png")
    plt.savefig(path)
    plt.close()
    print(f"[OK] {path}")


# ============================================================
# 总结对比图（跨场景）
# ============================================================

def plot_summary(output_root: str):
    """生成跨三组实验的总结对比图"""

    fig, axes = plt.subplots(1, 3, figsize=(15, 5.5))

    # ---- 数据 (来自各 summary.txt) ----
    scenarios = ['扩容测试', '缩容测试', '综合测试']

    # 图1: 资源消耗对比
    ax = axes[0]
    fixed_cost = [99.0, 90.0, 120.0]
    ds2_cost   = [66.2, 47.8, 70.6]
    x = np.arange(3)
    w = 0.32
    b1 = ax.bar(x - w/2, fixed_cost, w, color=C_FIXED, alpha=0.85,
                label='Fixed', edgecolor='white')
    b2 = ax.bar(x + w/2, ds2_cost, w, color=C_DS2, alpha=0.85,
                label='DS2', edgecolor='white')
    # 节省百分比标注
    for i in range(3):
        save_pct = (fixed_cost[i] - ds2_cost[i]) / fixed_cost[i] * 100
        mid_x = x[i] + w/2
        mid_y = ds2_cost[i] + (fixed_cost[i] - ds2_cost[i]) / 2
        ax.annotate(f'-{save_pct:.0f}%',
                    xy=(mid_x, ds2_cost[i]),
                    xytext=(mid_x + 0.2, mid_y),
                    fontsize=11, fontweight='bold', color=C_DS2,
                    arrowprops=dict(arrowstyle='->', color=C_DS2, lw=1.5))
    ax.set_xticks(x)
    ax.set_xticklabels(scenarios, fontsize=11)
    ax.set_ylabel('slot·min')
    ax.set_title('资源消耗对比')
    ax.legend(framealpha=0.9)

    # 图2: 资源效率对比
    ax = axes[1]
    fixed_eff = [2879.2, 646.8, 1930.3]
    ds2_eff   = [2587.9, 903.1, 2452.4]
    b1 = ax.bar(x - w/2, fixed_eff, w, color=C_FIXED, alpha=0.85,
                label='Fixed', edgecolor='white')
    b2 = ax.bar(x + w/2, ds2_eff, w, color=C_DS2, alpha=0.85,
                label='DS2', edgecolor='white')
    for i in range(3):
        gain = (ds2_eff[i] - fixed_eff[i]) / fixed_eff[i] * 100
        sign = '+' if gain > 0 else ''
        ax.text(x[i] + w/2, ds2_eff[i] + 30,
                f'{sign}{gain:.0f}%', ha='center', va='bottom',
                fontsize=11, fontweight='bold', color=C_DS2)
    ax.set_xticks(x)
    ax.set_xticklabels(scenarios, fontsize=11)
    ax.set_ylabel('r/s / slot')
    ax.set_title('资源利用效率对比')
    ax.legend(framealpha=0.9)

    # 图3: 平均Slot数对比
    ax = axes[2]
    fixed_slots = [4.0, 6.0, 4.0]
    ds2_slots   = [4.1, 4.6, 3.3]
    b1 = ax.bar(x - w/2, fixed_slots, w, color=C_FIXED, alpha=0.85,
                label='Fixed', edgecolor='white')
    b2 = ax.bar(x + w/2, ds2_slots, w, color=C_DS2, alpha=0.85,
                label='DS2', edgecolor='white')
    for bar_group, color in [(b1, C_FIXED), (b2, C_DS2)]:
        for bar in bar_group:
            h = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2, h + 0.08,
                    f'{h:.1f}', ha='center', va='bottom',
                    fontsize=10, fontweight='bold', color=color)
    ax.set_xticks(x)
    ax.set_xticklabels(scenarios, fontsize=11)
    ax.set_ylabel('Slot 数量')
    ax.set_title('平均 Slot 使用量对比')
    ax.legend(framealpha=0.9)
    ax.set_ylim(0, 8)

    fig.suptitle('三组实验核心指标总结', fontsize=16, fontweight='bold')
    plt.tight_layout()
    path = os.path.join(output_root, "fig_summary_comparison.png")
    plt.savefig(path)
    plt.close()
    print(f"[OK] {path}")


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="论文级实验可视化")
    parser.add_argument('--scenario',
                        choices=['scale_up', 'scale_down0', 'fluctuate'],
                        help='只生成指定场景的图')
    parser.add_argument('--summary', action='store_true',
                        help='只生成总结对比图')
    parser.add_argument('--data-root', default=DATA_ROOT,
                        help='数据根目录')
    args = parser.parse_args()

    root = args.data_root

    if args.summary:
        plot_summary(root)
        return

    if args.scenario:
        scenarios = [args.scenario]
    else:
        scenarios = ['scale_up', 'scale_down0', 'fluctuate']

    for sc in scenarios:
        out_dir = os.path.join(root, sc)
        if not os.path.exists(out_dir):
            print(f"[SKIP] {out_dir} not found")
            continue

        print(f"\n{'='*50}")
        print(f"  Generating: {sc}")
        print(f"{'='*50}")

        if sc == 'scale_up':
            plot_scale_up(out_dir)
        elif sc == 'scale_down0':
            plot_scale_down(out_dir)
        elif sc == 'fluctuate':
            plot_fluctuate(out_dir)

    # 总是生成总结图
    if not args.scenario:
        print(f"\n{'='*50}")
        print(f"  Generating: summary")
        print(f"{'='*50}")
        plot_summary(root)

    print(f"\n[DONE] All figures saved to {root}/")


if __name__ == "__main__":
    main()
