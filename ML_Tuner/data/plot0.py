import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import json

# 1. 设置严谨的学术图表样式 (白底黑线网格)
sns.set_theme(style="whitegrid", context="paper", font_scale=1.2)
plt.rcParams['font.family'] = 'sans-serif'

# 2. 读取与清洗数据
df = pd.read_csv('observations.csv')
df = df[df['is_valid'] == 1].copy()  # 只保留有效数据
df.sort_values('round_id', inplace=True)
df.reset_index(drop=True, inplace=True)

# 计算"迄今为止的历史最优J值" (核心收敛指标)
df['best_j_so_far'] = df['j_total'].cummin()

# 3. 初始化画布
fig, ax = plt.subplots(figsize=(10, 6))

# 4. 拆分不同阶段的数据以使用不同样式
warmup_mask = df['phase'] == 'warmup'
opt_mask = df['phase'] == 'optimize'

# 【优化点 1】: 将波动起伏的折线改为"散点"，表示独立的探索采样，消除不稳定感
ax.scatter(df[warmup_mask]['round_id'], df[warmup_mask]['j_total'],
           color='gray', alpha=0.5, marker='x', s=60, label='Warmup Sampling (LHS)')

ax.scatter(df[opt_mask]['round_id'], df[opt_mask]['j_total'],
           color='steelblue', alpha=0.6, marker='o', s=60, label='BO Exploration')

# 【优化点 2】: 使用严谨的"阶梯线 (Step Plot)" 展示收敛过程
# where='post' 表示在达到下一个采样点之前，最优值保持水平不变
ax.step(df['round_id'], df['best_j_so_far'],
        color='crimson', linewidth=2.5, where='post', label='Best Objective ($J$) So Far')

# 【优化点 3】: 突出标记全局最优解 (金星)
best_idx = df['j_total'].idxmin()
best_round = df.loc[best_idx, 'round_id']
best_j = df.loc[best_idx, 'j_total']
ax.plot(best_round, best_j, marker='*', color='gold', markersize=18,
        markeredgecolor='darkred', markeredgewidth=1.5, zorder=5,
        label=f'Global Optimum found at Round {best_round}\n($J$ = {best_j:.4f})')

# 【优化点 4】: 添加垂直虚线，明确切分预热期和算法发力期
optimize_start = df[df['phase'] == 'optimize']['round_id'].min()
if pd.notna(optimize_start):
    ax.axvline(x=optimize_start - 0.5, color='black', linestyle='--', alpha=0.6,
               linewidth=1.5, label='Bayesian Optimization Starts')

# 5. 图表美化与标签设置
ax.set_title('Convergence Curve of ML-Tuner for Flink Streaming',
             fontsize=15, fontweight='bold', pad=15)
ax.set_xlabel('Evaluation Round', fontsize=12, fontweight='bold')
ax.set_ylabel('Objective Function Cost $J$ (Lower is Better)', fontsize=12, fontweight='bold')

# 智能限制 Y 轴范围：防止极个别极差的探索点把主视图压缩得看不见
# 取 0 到 0.6 作为合理展示区间 (根据你的实际数据表现)
ax.set_ylim(0, 0.6)

# 设置 X 轴刻度为整数
ax.set_xticks(np.arange(0, df['round_id'].max() + 1, step=2))

# 优化图例：放置在右上角并增加阴影框
ax.legend(loc='upper right', frameon=True, fancybox=True, shadow=True, fontsize=10)

plt.tight_layout()
plt.savefig('academic_convergence_plot.png', dpi=300, bbox_inches='tight')
print("✅ 已生成学术版严谨收敛图：academic_convergence_plot.png")

# 可选：如果你想看弹出的窗口，取消注释下一行
plt.show()
