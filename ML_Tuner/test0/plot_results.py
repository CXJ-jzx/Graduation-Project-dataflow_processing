#!/usr/bin/env python3
"""
生成调优效果图表

用法:
  python tests/plot_results.py \
    --observations data/observations.csv \
    --ab-results data/ab_results.csv \
    --output-dir data/plots
"""

import os
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # 无头模式

plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


def plot_convergence(obs_path, output_dir):
    """图1: J 值收敛曲线"""
    df = pd.read_csv(obs_path)
    valid = df[df['is_valid'] == True].copy()
    valid['running_best'] = valid['j_total'].cummin()

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(valid['round_id'], valid['j_total'], 'bo-', alpha=0.5,
            label='J (each round)', markersize=6)
    ax.plot(valid['round_id'], valid['running_best'], 'r-', linewidth=2,
            label='Best J so far')

    # 标记 warmup / optimize 分界
    warmup_max = valid[valid['phase'] == 'warmup']['round_id'].max()
    if pd.notna(warmup_max):
        ax.axvline(x=warmup_max + 0.5, color='gray', linestyle='--',
                   alpha=0.5, label='Warmup → Optimize')

    ax.set_xlabel('Round')
    ax.set_ylabel('J (objective)')
    ax.set_title('ML Tuner Convergence')
    ax.legend()
    ax.grid(True, alpha=0.3)

    path = os.path.join(output_dir, 'convergence.png')
    fig.savefig(path, dpi=150, bbox_inches='tight')
    print(f"已保存: {path}")
    plt.close()


def plot_parameter_exploration(obs_path, output_dir):
    """图2: 参数探索轨迹（3D 散点图）"""
    df = pd.read_csv(obs_path)
    valid = df[df['is_valid'] == True].copy()

    if len(valid) < 3:
        print("有效数据不足，跳过参数探索图")
        return

    fig = plt.figure(figsize=(10, 8))
    ax = fig.add_subplot(111, projection='3d')

    # 用 J 值作为颜色
    sc = ax.scatter(
        valid['checkpoint_interval'] / 1000,
        valid['buffer_timeout'],
        valid['watermark_delay'] / 1000,
        c=valid['j_total'],
        cmap='RdYlGn_r',
        s=80, alpha=0.7, edgecolors='black', linewidths=0.5,
    )

    # 标记最优点
    best_idx = valid['j_total'].idxmin()
    best = valid.loc[best_idx]
    ax.scatter(best['checkpoint_interval'] / 1000,
               best['buffer_timeout'],
               best['watermark_delay'] / 1000,
               c='red', s=200, marker='*', edgecolors='black',
               linewidths=1.5, zorder=5, label='Best')

    ax.set_xlabel('Checkpoint Interval (s)')
    ax.set_ylabel('Buffer Timeout (ms)')
    ax.set_zlabel('Watermark Delay (s)')
    ax.set_title('Parameter Exploration Space')
    fig.colorbar(sc, ax=ax, label='J value', shrink=0.6)
    ax.legend()

    path = os.path.join(output_dir, 'parameter_space.png')
    fig.savefig(path, dpi=150, bbox_inches='tight')
    print(f"已保存: {path}")
    plt.close()


def plot_metrics_trend(obs_path, output_dir):
    """图3: 各指标随轮次变化"""
    df = pd.read_csv(obs_path)
    valid = df[df['is_valid'] == True].copy()

    metrics = [
        ('avg_input_rate', 'Throughput (records/s)', 'blue'),
        ('e2e_latency_p99_ms', 'Latency P99 (ms)', 'red'),
        ('backpressure_ratio', 'Backpressure', 'orange'),
        ('checkpoint_avg_dur', 'Checkpoint Duration (ms)', 'green'),
    ]

    fig, axes = plt.subplots(2, 2, figsize=(14, 8))
    axes = axes.flatten()

    for idx, (col, label, color) in enumerate(metrics):
        ax = axes[idx]
        if col in valid.columns:
            ax.plot(valid['round_id'], valid[col], f'{color[0]}o-',
                    color=color, alpha=0.7, markersize=5)
        ax.set_xlabel('Round')
        ax.set_ylabel(label)
        ax.set_title(label)
        ax.grid(True, alpha=0.3)

    fig.suptitle('Metrics Trend per Round', fontsize=14)
    fig.tight_layout()

    path = os.path.join(output_dir, 'metrics_trend.png')
    fig.savefig(path, dpi=150, bbox_inches='tight')
    print(f"已保存: {path}")
    plt.close()


def plot_ab_comparison(ab_path, output_dir):
    """图4: A/B 对比箱线图"""
    if not os.path.exists(ab_path):
        print(f"A/B 结果文件不存在: {ab_path}")
        return

    df = pd.read_csv(ab_path)
    df_valid = df[df['is_valid'] == True]

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    metrics = [
        ('j_total', 'J Total (lower=better)'),
        ('e2e_latency_p99_ms', 'Latency P99 (ms)'),
        ('avg_input_rate', 'Throughput (records/s)'),
    ]

    for idx, (col, title) in enumerate(metrics):
        ax = axes[idx]
        data_default = df_valid[df_valid['label'] == 'default'][col]
        data_best = df_valid[df_valid['label'] == 'ml_best'][col]

        bp = ax.boxplot([data_default, data_best],
                        labels=['Default', 'ML Best'],
                        patch_artist=True)
        bp['boxes'][0].set_facecolor('#FFB3B3')
        bp['boxes'][1].set_facecolor('#B3FFB3')

        ax.set_title(title)
        ax.grid(True, alpha=0.3)

    fig.suptitle('A/B Test: Default vs ML Best', fontsize=14)
    fig.tight_layout()

    path = os.path.join(output_dir, 'ab_comparison.png')
    fig.savefig(path, dpi=150, bbox_inches='tight')
    print(f"已保存: {path}")
    plt.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--observations", default="data/observations.csv")
    parser.add_argument("--ab-results", default="data/ab_results.csv")
    parser.add_argument("--output-dir", default="data/plots")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    if os.path.exists(args.observations):
        plot_convergence(args.observations, args.output_dir)
        plot_parameter_exploration(args.observations, args.output_dir)
        plot_metrics_trend(args.observations, args.output_dir)

    plot_ab_comparison(args.ab_results, args.output_dir)
    print("\n全部图表生成完毕！")
