import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import json
import numpy as np

# 1. 读取数据
df = pd.read_csv('observations.csv')
# 过滤掉极值或无效值用于绘图
df = df[df['is_valid'] == 1]
df = df[df['j_total'] < 1.0]

# 解析 JSON 格式的 theta 列
theta_df = df['theta'].apply(json.loads).apply(pd.Series)
df = pd.concat([df, theta_df], axis=1)

# 2. 绘制收敛曲线
plt.figure(figsize=(10, 5))
# 计算迄今为止的最佳（最小）J值
df['best_j_so_far'] = df['j_total'].cummin()

plt.plot(df['round_id'], df['j_total'], marker='o', linestyle='-', color='lightgray', label='Current Round J')
plt.plot(df['round_id'], df['best_j_so_far'], marker='s', linestyle='-', color='red', linewidth=2, label='Best J So Far')

plt.title('Bayesian Optimization Convergence for Flink Parameters')
plt.xlabel('Round ID')
plt.ylabel('Objective Function J (Lower is better)')
plt.grid(True, linestyle='--', alpha=0.6)
plt.legend()
plt.tight_layout()
plt.savefig('convergence_plot.png', dpi=300)
print("保存收敛图至 convergence_plot.png")

# 3. 绘制 3D 参数探索散点图
fig = plt.figure(figsize=(10, 8))
ax = fig.add_subplot(111, projection='3d')

# 使用 J 值作为颜色映射，J 越小越偏蓝/绿
sc = ax.scatter(df['checkpoint_interval'],
                df['buffer_timeout'],
                df['watermark_delay'],
                c=df['j_total'], cmap='viridis_r', s=100, alpha=0.8, edgecolors='k')

# 标记最优解
best_idx = df['j_total'].idxmin()
ax.scatter(df.loc[best_idx, 'checkpoint_interval'],
           df.loc[best_idx, 'buffer_timeout'],
           df.loc[best_idx, 'watermark_delay'],
           color='red', s=300, marker='*', label='Global Best')

ax.set_xlabel('Checkpoint Interval (ms)')
ax.set_ylabel('Buffer Timeout (ms)')
ax.set_zlabel('Watermark Delay (ms)')
ax.set_title('Parameter Space Exploration (Color = J Score)')
plt.colorbar(sc, label='J Score (Lower is better)', shrink=0.5)
plt.legend()
plt.tight_layout()
plt.savefig('parameter_space_3d.png', dpi=300)
print("保存 3D 参数空间图至 parameter_space_3d.png")

plt.show()
