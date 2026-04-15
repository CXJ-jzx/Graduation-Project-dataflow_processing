"""
特征工程模块

职责：
1. 多次采样聚合（中位数）
2. 16维环境特征的 Min-Max 归一化
3. 目标函数 J 的计算（加权多目标）
4. 吞吐量基线的动态更新
5. 约束检查
"""

import numpy as np
from typing import Dict, List, Tuple, Optional
import yaml
import logging

logger = logging.getLogger("ml_tuner.feature_engineer")


class FeatureEngineer:
    """特征工程与目标函数计算"""

    # 16维环境特征名称（固定顺序）
    FEATURE_NAMES = [
        "avg_input_rate",       # 平均输入速率 (records/s)
        "peak_input_rate",      # 峰值输入速率
        "input_cv",             # 输入速率变异系数
        "active_node_count",    # 活跃节点数（来自GridSummary）
        "avg_backlog",          # 平均积压量
        "avg_busy_ratio",       # 平均繁忙率
        "max_busy_ratio",       # 最大繁忙率
        "total_parallelism",    # 总并行度
        "heap_usage_ratio",     # 堆内存使用率
        "e2e_latency_p99_ms",          # Source→Sink P99延迟 (ms)
        "checkpoint_avg_dur",   # Checkpoint 平均耗时 (ms)
        "backpressure_ratio",   # 背压比例
        "l2_hit_rate",          # L2 缓存命中率
        "l2_occupancy",         # L2 缓存占用率
        "l3_hit_rate",          # L3 缓存命中率
        "l3_occupancy",         # L3 缓存占用率
    ]

    def __init__(self, config_path: str = None, config_dict: dict = None):
        """
        初始化特征工程模块

        Args:
            config_path: 配置文件路径
            config_dict: 完整配置字典
        """
        if config_dict is not None:
            cfg = config_dict
        elif config_path is not None:
            with open(config_path, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f)
        else:
            raise ValueError("必须提供 config_path 或 config_dict")

        # 无需修改，cfg 本身就是完整配置



        # 特征归一化范围
        self.feature_ranges = {}
        for name in self.FEATURE_NAMES:
            r = cfg['feature_ranges'].get(name, [0, 1])
            self.feature_ranges[name] = (float(r[0]), float(r[1]))

        # 目标函数权重
        obj_cfg = cfg['objective']
        self.weights = {
            'latency': float(obj_cfg['weights']['latency']),
            'throughput': float(obj_cfg['weights']['throughput']),
            'checkpoint': float(obj_cfg['weights']['checkpoint']),
            'resource': float(obj_cfg['weights']['resource']),
        }
        self.target_utilization = float(obj_cfg['target_utilization'])
        self.sla_latency_ms = float(obj_cfg['sla_latency_ms'])

        # 约束条件
        cons_cfg = cfg['constraints']
        self.max_latency_p99_ms = float(cons_cfg['max_latency_p99_ms'])
        self.max_backpressure = float(cons_cfg['max_backpressure'])

        # 吞吐量基线（动态更新）
        self.max_throughput_baseline = 0.0

        logger.info("特征工程初始化完成: %d 维特征, 权重=%s",
                     len(self.FEATURE_NAMES), self.weights)

        # ★ 新增：从 producer 配置读取固定基线
        producer_cfg = cfg.get('producer', {})
        producer_rate = producer_cfg.get('args', '')
        try:
            self._fixed_throughput_baseline = float(producer_rate)
        except (ValueError, TypeError):
            self._fixed_throughput_baseline = 0.0

        # 吞吐量基线（动态更新，但不低于固定基线）
        self.max_throughput_baseline = self._fixed_throughput_baseline

        logger.info("特征工程初始化完成: %d 维特征, 权重=%s, 吞吐量固定基线=%.0f",
                    len(self.FEATURE_NAMES), self.weights, self._fixed_throughput_baseline)

    # ==========================================
    # 采样聚合
    # ==========================================

    def aggregate_median(self, samples: List[Dict[str, float]]) -> Dict[str, float]:
        """
        对多次采样取中位数聚合

        Args:
            samples: 多次采样结果列表，每个元素是 {feature_name: value} 字典

        Returns:
            dict: 中位数聚合后的特征字典
        """
        if not samples:
            raise ValueError("采样列表为空")

        if len(samples) == 1:
            return dict(samples[0])

        result = {}
        for name in self.FEATURE_NAMES:
            values = []
            for s in samples:
                if name in s and s[name] is not None:
                    values.append(float(s[name]))

            if values:
                result[name] = float(np.median(values))
            else:
                result[name] = 0.0
                logger.warning("特征 %s 在所有采样中缺失，使用默认值 0.0", name)

        return result

    # ==========================================
    # 归一化
    # ==========================================

    def normalize_features(self, x_raw: Dict[str, float]) -> np.ndarray:
        """
        16维环境特征 Min-Max 归一化

        Args:
            x_raw: 原始特征字典

        Returns:
            np.ndarray: shape=(16,), 值域 [0, 1]
        """
        result = np.zeros(len(self.FEATURE_NAMES))
        for i, name in enumerate(self.FEATURE_NAMES):
            val = float(x_raw.get(name, 0.0))
            lo, hi = self.feature_ranges[name]
            if hi > lo:
                result[i] = np.clip((val - lo) / (hi - lo), 0.0, 1.0)
            else:
                result[i] = 0.0
        return result

    # ==========================================
    # 目标函数
    # ==========================================

    def compute_objective(self, x_raw: Dict[str, float]) -> Tuple[float, Dict[str, float]]:
        latency_p99 = float(
            x_raw.get('e2e_latency_p99_ms', x_raw.get('latency_p99', 0.0))
        )
        avg_input_rate = float(x_raw.get('avg_input_rate', 0.0))
        checkpoint_avg_dur = float(x_raw.get('checkpoint_avg_dur', 0.0))
        avg_busy_ratio = float(x_raw.get('avg_busy_ratio', 0.0))

        # --- 子目标1: 延迟 ---
        f_lat = min(latency_p99 / self.sla_latency_ms, 2.0)

        # --- 子目标2: 吞吐量损失 ---                           ★ 修改
        # 动态更新，但不低于固定基线
        if avg_input_rate > self.max_throughput_baseline:
            self.max_throughput_baseline = avg_input_rate
            logger.info("吞吐量基线更新: %.0f records/s", self.max_throughput_baseline)

        baseline = max(self.max_throughput_baseline, self._fixed_throughput_baseline, 1.0)

        f_thr = 1.0 - (avg_input_rate / baseline)
        f_thr = max(0.0, min(f_thr, 1.0))

        # --- 子目标3: Checkpoint 开销 ---
        f_ckp = min(checkpoint_avg_dur / self.sla_latency_ms, 2.0)

        # --- 子目标4: 资源利用偏离度 ---
        f_res = abs(avg_busy_ratio - self.target_utilization) / max(self.target_utilization, 0.01)
        f_res = min(f_res, 2.0)

        # --- 加权汇总 ---
        j_total = (self.weights['latency'] * f_lat +
                   self.weights['throughput'] * f_thr +
                   self.weights['checkpoint'] * f_ckp +
                   self.weights['resource'] * f_res)

        j_parts = {
            'f_latency': round(f_lat, 4),
            'f_throughput': round(f_thr, 4),
            'f_checkpoint': round(f_ckp, 4),
            'f_resource': round(f_res, 4),
            'j_total': round(j_total, 6),
        }

        return j_total, j_parts


    # ==========================================
    # 约束检查
    # ==========================================

    def check_constraints(self, x_raw: Dict[str, float]) -> Tuple[bool, List[str]]:
        """
        检查是否违反约束条件

        Args:
            x_raw: 原始特征字典

        Returns:
            (is_valid, violations): 是否满足约束, 违反项列表
        """
        violations = []

        latency_p99 = float(
            x_raw.get('e2e_latency_p99_ms', x_raw.get('latency_p99', 0.0))
        )
        if latency_p99 > self.max_latency_p99_ms:
            violations.append(
                f"e2e_latency_p99_ms={latency_p99:.0f}ms > 阈值{self.max_latency_p99_ms:.0f}ms"
            )

        backpressure = float(x_raw.get('backpressure_ratio', 0.0))
        if backpressure > self.max_backpressure:
            violations.append(
                f"backpressure={backpressure:.2f} > 阈值{self.max_backpressure:.2f}"
            )

        is_valid = len(violations) == 0

        if not is_valid:
            logger.warning("约束违反: %s", "; ".join(violations))

        return is_valid, violations

    # ==========================================
    # 吞吐量基线管理
    # ==========================================

    def set_throughput_baseline(self, baseline: float):
        """手动设置吞吐量基线（从历史记录恢复时使用）"""
        if baseline > 0:
            self.max_throughput_baseline = baseline
            logger.info("吞吐量基线设置为: %.0f records/s", baseline)

    def get_throughput_baseline(self) -> float:
        """获取当前吞吐量基线"""
        return self.max_throughput_baseline

    # ==========================================
    # 辅助方法
    # ==========================================

    def format_features(self, x_raw: Dict[str, float]) -> str:
        """格式化特征字典为可读字符串"""
        lines = []
        for name in self.FEATURE_NAMES:
            val = x_raw.get(name, None)
            if val is not None:
                lines.append(f"  {name}: {val:.4f}")
            else:
                lines.append(f"  {name}: N/A")
        return "\n".join(lines)

    def format_objective(self, j_parts: Dict[str, float]) -> str:
        """格式化目标函数分解为可读字符串"""
        return (f"J={j_parts['j_total']:.4f} "
                f"[lat={j_parts['f_latency']:.3f}×{self.weights['latency']}, "
                f"thr={j_parts['f_throughput']:.3f}×{self.weights['throughput']}, "
                f"ckp={j_parts['f_checkpoint']:.3f}×{self.weights['checkpoint']}, "
                f"res={j_parts['f_resource']:.3f}×{self.weights['resource']}]")
