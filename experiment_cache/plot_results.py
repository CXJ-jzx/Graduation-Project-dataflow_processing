#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成缓存淘汰策略对比图表
"""

import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['figure.dpi'] = 150

COLORS = {
    "FIFO":      "#95a5a6",
    "LRU":       "#3498db",
    "LFU":       "#e67e22",
    "LRU-K":     "#9b59b6",
    "W-TINYLFU": "#e74c3c",
}

STRATEGY_ORDER = ["FIFO", "LRU", "LFU", "LRU-K", "W-TINYLFU"]


def get_ordered_strategies(data):
    ordered = []
    for s in STRATEGY_ORDER:
        if s in data:
            ordered.append(s)
    for s in data:
        if s not in ordered:
            ordered.append(s)
    return ordered


def get_color(strategy):
    return COLORS.get(strategy, "#333333")


def plot_overall_hitrate(data, save_dir):
    """图1: 总命中率柱状图"""
    strategies = get_ordered_strategies(data)
    rates = [data[s]['hitRate'] for s in strategies]
    colors = [get_color(s) for s in strategies]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(strategies, rates, color=colors, width=0.6,
                  edgecolor='white', linewidth=1.5)

    # 标注本文方案
    for bar, rate, s in zip(bars, rates, strategies):
        label = f'{rate:.1f}%'
        if s == "W-TINYLFU":
            label += ' ◀本文'
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                label, ha='center', va='bottom',
                fontsize=12, fontweight='bold')

    ax.set_ylabel('缓存命中率 (%)', fontsize=13)
    ax.set_title('L2 缓存总命中率对比', fontsize=15, fontweight='bold')
    ax.set_ylim(0, max(rates) * 1.4 if rates else 50)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.0f%%'))
    ax.grid(axis='y', alpha=0.3)
    ax.set_axisbelow(True)

    plt.tight_layout()
    path = os.path.join(save_dir, 'fig1_overall_hitrate.png')
    fig.savefig(path)
    plt.close(fig)
    print(f"  图1 已保存: {path}")


def plot_tier_hitrate(data, save_dir):
    """图2: 分层命中率分组柱状图"""
    strategies = get_ordered_strategies(data)
    tiers = ['T1', 'T2', 'T3', 'T4']
    tier_labels = ['T1 (高频)', 'T2 (中频)', 'T3 (低频)', 'T4 (极低频)']

    x = np.arange(len(tiers))
    width = 0.8 / max(len(strategies), 1)

    fig, ax = plt.subplots(figsize=(10, 6))

    for i, strategy in enumerate(strategies):
        tier_rates = data[strategy].get('tierRates', {})
        values = [tier_rates.get(t, 0) for t in tiers]
        offset = (i - len(strategies) / 2 + 0.5) * width
        bars = ax.bar(x + offset, values, width * 0.9,
                      label=strategy, color=get_color(strategy),
                      edgecolor='white', linewidth=0.5)

        for bar, val in zip(bars, values):
            if val > 0:
                fontsize = 7 if len(strategies) > 3 else 8
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + 0.3,
                        f'{val:.1f}', ha='center', va='bottom',
                        fontsize=fontsize)

    ax.set_xlabel('节点层级', fontsize=12)
    ax.set_ylabel('命中率 (%)', fontsize=12)
    ax.set_title('各层级 L2 缓存命中率对比', fontsize=15, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(tier_labels, fontsize=11)
    ax.legend(loc='upper right', fontsize=10)
    ax.grid(axis='y', alpha=0.3)
    ax.set_axisbelow(True)

    plt.tight_layout()
    path = os.path.join(save_dir, 'fig2_tier_hitrate.png')
    fig.savefig(path)
    plt.close(fig)
    print(f"  图2 已保存: {path}")


def plot_hitrate_timeline(data, save_dir):
    """图3: 命中率趋势折线图"""
    strategies = get_ordered_strategies(data)

    fig, ax = plt.subplots(figsize=(10, 5))

    for strategy in strategies:
        series = data[strategy].get('hitrate_series', [])
        if not series:
            continue

        lw = 3.0 if strategy == "W-TINYLFU" else 2.0
        x = list(range(1, len(series) + 1))
        ax.plot(x, series, label=strategy, color=get_color(strategy),
                linewidth=lw, marker='o', markersize=3, alpha=0.8)

    ax.set_xlabel('采样序号', fontsize=12)
    ax.set_ylabel('命中率 (%)', fontsize=12)
    ax.set_title('L2 缓存命中率变化趋势', fontsize=15, fontweight='bold')
    ax.legend(loc='best', fontsize=10)
    ax.grid(alpha=0.3)
    ax.set_axisbelow(True)

    plt.tight_layout()
    path = os.path.join(save_dir, 'fig3_hitrate_timeline.png')
    fig.savefig(path)
    plt.close(fig)
    print(f"  图3 已保存: {path}")


def plot_summary_table(data, save_dir):
    """图4: 综合对比表格"""
    strategies = get_ordered_strategies(data)

    columns = ['策略', '总命中率', 'T1(高频)', 'T2(中频)', 'T3(低频)', 'T4(极低频)']
    rows = []

    for s in strategies:
        tiers = data[s].get('tierRates', {})
        rows.append([
            s,
            f"{data[s]['hitRate']:.1f}%",
            f"{tiers.get('T1', 0):.1f}%",
            f"{tiers.get('T2', 0):.1f}%",
            f"{tiers.get('T3', 0):.1f}%",
            f"{tiers.get('T4', 0):.1f}%",
        ])

    fig, ax = plt.subplots(figsize=(10, 2 + len(strategies) * 0.6))
    ax.axis('off')

    cell_colors = []
    for i, s in enumerate(strategies):
        row_colors = ['#f8f9fa']
        hr = data[s]['hitRate']
        tiers = data[s].get('tierRates', {})

        if hr >= 30:
            row_colors.append('#d4edda')
        elif hr >= 15:
            row_colors.append('#fff3cd')
        else:
            row_colors.append('#f8d7da')

        for t in ['T1', 'T2', 'T3', 'T4']:
            val = tiers.get(t, 0)
            if val >= 40:
                row_colors.append('#d4edda')
            elif val >= 15:
                row_colors.append('#fff3cd')
            else:
                row_colors.append('#f8d7da')

        cell_colors.append(row_colors)

    # 本文方案行高亮
    for i, s in enumerate(strategies):
        if s == "W-TINYLFU":
            cell_colors[i] = ['#ffeaa7'] + cell_colors[i][1:]

    table = ax.table(
        cellText=rows,
        colLabels=columns,
        cellColours=cell_colors,
        colColours=['#343a40'] * len(columns),
        loc='center',
        cellLoc='center',
    )

    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1.2, 1.8)

    for j in range(len(columns)):
        table[0, j].get_text().set_color('white')
        table[0, j].get_text().set_fontweight('bold')

    ax.set_title('缓存策略综合对比', fontsize=15, fontweight='bold', pad=20)

    plt.tight_layout()
    path = os.path.join(save_dir, 'fig4_summary_table.png')
    fig.savefig(path, bbox_inches='tight')
    plt.close(fig)
    print(f"  图4 已保存: {path}")


def plot_all(data, save_dir):
    """生成所有图表"""
    os.makedirs(save_dir, exist_ok=True)

    print("\n生成可视化图表...")
    plot_overall_hitrate(data, save_dir)
    plot_tier_hitrate(data, save_dir)
    plot_hitrate_timeline(data, save_dir)
    plot_summary_table(data, save_dir)

    print(f"\n全部图表已保存到: {save_dir}/")
    print("  fig1_overall_hitrate.png   — 总命中率柱状图")
    print("  fig2_tier_hitrate.png      — 分层命中率柱状图")
    print("  fig3_hitrate_timeline.png  — 命中率趋势折线图")
    print("  fig4_summary_table.png     — 综合对比表格")
