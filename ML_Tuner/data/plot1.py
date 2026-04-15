import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
import numpy as np

# ==========================================
# 1. 全局样式设置 (学术级精美风格)
# ==========================================
plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']
sns.set_theme(style="whitegrid", context="paper")
# 选取高级马卡龙色系，代表四种物理惩罚项
COLORS = ['#4A90E2', '#50E3C2', '#F5A623', '#D0021B']

# ==========================================
# 2. 数据读取与精细化处理
# ==========================================
df = pd.read_csv('observations.csv')
df = df[df['is_valid'] == 1].copy()
df.sort_values('round_id', inplace=True)
df.reset_index(drop=True, inplace=True)

# 解析复杂的 JSON 列 j_parts，提取四个物理维度的惩罚分
def parse_parts(json_str):
    try:
        clean_str = str(json_str).replace('""', '"').replace("'", '"')
        return json.loads(clean_str)
    except:
        return {}

parts_df = df['j_parts'].apply(parse_parts).apply(pd.Series)

# 【核心修复】：防止 json 内部的 j_total 与原本的 j_total 列名冲突
if 'j_total' in parts_df.columns:
    parts_df.drop('j_total', axis=1, inplace=True)

df = pd.concat([df, parts_df], axis=1)

# 确保提取的列存在，填充空值为0
metrics = ['f_latency', 'f_throughput', 'f_checkpoint', 'f_resource']
for m in metrics:
    if m not in df.columns:
        df[m] = 0.0

# ==========================================
# 3. 构建高维双子图 (Upper: 总分趋势, Lower: 惩罚项剖析)
# ==========================================
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 9), gridspec_kw={'height_ratios': [1, 1.8]})
fig.suptitle('Bayesian Optimization Trace & Penalty Breakdown', fontsize=18, fontweight='bold', y=0.96)

# ----------------- 上图：散点趋势与阶段划分 -----------------
warmup_df = df[df['phase'] == 'warmup']
opt_df = df[df['phase'] == 'optimize']

ax1.scatter(warmup_df['round_id'], warmup_df['j_total'],
            c='#95A5A6', s=100, marker='s', alpha=0.8, edgecolors='white', label='Phase: Warmup (LHS)')
ax1.scatter(opt_df['round_id'], opt_df['j_total'],
            c='#2C3E50', s=120, marker='o', alpha=0.8, edgecolors='white', label='Phase: Optimize (GP)')

# 标出全局最优解
best_idx = df['j_total'].idxmin()
best_row = df.loc[best_idx]
ax1.scatter(best_row['round_id'], best_row['j_total'],
            c='#FFD700', s=400, marker='*', edgecolors='red', linewidths=1.5, zorder=5,
            label=f"Global Best (Round {int(best_row['round_id'])}, J={best_row['j_total']:.4f})")

# 辅助线与标注
ax1.axvline(x=7.5, color='gray', linestyle='--', linewidth=1.5, alpha=0.7)
ax1.text(3, ax1.get_ylim()[1]*0.8, 'Random Exploration', fontsize=11, color='gray', style='italic')
ax1.text(15, ax1.get_ylim()[1]*0.8, 'Surrogate Model Exploitation & Uncertainty Bounding', fontsize=11, color='#2C3E50', style='italic')

ax1.set_ylabel('Total Cost $J$', fontsize=12, fontweight='bold')
ax1.set_xticks([])
ax1.legend(loc='upper right', framealpha=0.9)
ax1.set_ylim(0, df['j_total'].max() * 1.1)

# ----------------- 下图：严谨的分项惩罚堆叠柱状图 -----------------
bottom = np.zeros(len(df))
labels = ['Latency Penalty', 'Throughput Penalty', 'Checkpoint Penalty', 'Resource Under-utilization']

for i, col in enumerate(metrics):
    ax2.bar(df['round_id'], df[col], bottom=bottom, color=COLORS[i],
            label=labels[i], width=0.6, alpha=0.85, edgecolor='black', linewidth=0.5)
    bottom += df[col].values

# 突出显示最优解的那一根柱子
ax2.bar(best_row['round_id'], bottom[best_idx],
        facecolor='none', edgecolor='red', linewidth=2.5, width=0.7, zorder=10)
ax2.annotate('Perfect Balance', xy=(best_row['round_id'], bottom[best_idx]),
             xytext=(best_row['round_id']+1, bottom[best_idx]+0.15),
             arrowprops=dict(facecolor='black', shrink=0.05, width=1.5, headwidth=8),
             fontsize=12, fontweight='bold', color='red')

ax2.set_xlabel('Evaluation Round ID', fontsize=12, fontweight='bold')
ax2.set_ylabel('Penalty Components Contribution', fontsize=12, fontweight='bold')
ax2.set_xticks(df['round_id'])
ax2.legend(loc='upper right', framealpha=0.9)
ax2.grid(axis='y', linestyle='--', alpha=0.5)

# ==========================================
# 4. 收尾与保存
# ==========================================
plt.tight_layout()
plt.subplots_adjust(hspace=0.08)
plt.savefig('academic_stacked_analysis.png', dpi=300, bbox_inches='tight')
print("✅ 高级分析图表生成完毕：academic_stacked_analysis.png")
plt.show()
