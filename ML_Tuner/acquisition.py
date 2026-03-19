"""
采集函数模块

职责：
1. Expected Improvement (EI) 计算（解析解）
2. ξ（探索-利用平衡参数）的衰减策略
3. 批量计算支持

EI(θ) = (J_best - μ - ξ) · Φ(Z) + σ · φ(Z)   当 σ > 0
       = 0                                        当 σ = 0

其中 Z = (J_best - μ - ξ) / σ
"""

import numpy as np
from scipy.stats import norm
from typing import Tuple
import logging

logger = logging.getLogger("ml_tuner.acquisition")


class AcquisitionFunction:
    """Expected Improvement 采集函数"""

    def __init__(self, config_dict: dict = None, config_path: str = None):
        """
        初始化采集函数

        Args:
            config_dict: 完整配置字典
            config_path: 配置文件路径
        """
        import yaml

        if config_dict is not None:
            cfg = config_dict
        elif config_path is not None:
            with open(config_path, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f)
        else:
            raise ValueError("必须提供 config_path 或 config_dict")

        opt_cfg = cfg['optimizer']

        # ξ 衰减参数
        self.xi_initial = float(opt_cfg['xi_initial'])
        self.xi_final = float(opt_cfg['xi_final'])
        self.xi_decay_rounds = int(opt_cfg['xi_decay_rounds'])

        logger.info("AcquisitionFunction 初始化: xi_init=%.3f, xi_final=%.3f, decay_rounds=%d",
                     self.xi_initial, self.xi_final, self.xi_decay_rounds)

    def compute_xi(self, n_valid: int, warm_up_n: int) -> float:
        """
        计算当前轮次的 ξ 值（线性衰减）

        冷启动期：   ξ = xi_initial（鼓励探索）
        优化期：     从 xi_initial 线性衰减到 xi_final
        衰减完成后： ξ = xi_final（加速收敛）

        Args:
            n_valid: 当前有效观测数
            warm_up_n: 冷启动阈值

        Returns:
            float: 当前 ξ 值
        """
        if n_valid <= warm_up_n:
            return self.xi_initial

        # 优化期已进行的轮次
        opt_rounds = n_valid - warm_up_n

        if opt_rounds >= self.xi_decay_rounds:
            return self.xi_final

        # 线性衰减
        progress = opt_rounds / self.xi_decay_rounds
        xi = self.xi_initial + (self.xi_final - self.xi_initial) * progress

        return xi

    def compute_ei(self,
                   mu: np.ndarray,
                   sigma: np.ndarray,
                   j_best: float,
                   xi: float) -> np.ndarray:
        """
        批量计算 Expected Improvement

        EI(θ) = (J_best - μ - ξ) · Φ(Z) + σ · φ(Z)

        注意：J 越小越好，所以 improvement = J_best - μ

        Args:
            mu: shape=(m,), GP/RF 预测均值
            sigma: shape=(m,), GP/RF 预测标准差
            j_best: 历史最优 J 值
            xi: 探索参数

        Returns:
            np.ndarray: shape=(m,), 各候选点的 EI 值
        """
        ei = np.zeros_like(mu)

        # 只对 σ > 0 的点计算
        mask = sigma > 1e-10
        if not np.any(mask):
            return ei

        improvement = j_best - mu[mask] - xi
        Z = improvement / sigma[mask]

        ei[mask] = improvement * norm.cdf(Z) + sigma[mask] * norm.pdf(Z)

        # EI 不能为负
        ei = np.maximum(ei, 0.0)

        return ei

    def compute_ei_single(self, mu: float, sigma: float,
                          j_best: float, xi: float) -> float:
        """
        单点 EI 计算

        Args:
            mu: 预测均值
            sigma: 预测标准差
            j_best: 历史最优
            xi: 探索参数

        Returns:
            float: EI 值
        """
        if sigma < 1e-10:
            return 0.0

        improvement = j_best - mu - xi
        Z = improvement / sigma

        ei = improvement * norm.cdf(Z) + sigma * norm.pdf(Z)
        return max(ei, 0.0)

    def __repr__(self) -> str:
        return (f"AcquisitionFunction(xi_init={self.xi_initial}, "
                f"xi_final={self.xi_final}, decay={self.xi_decay_rounds})")
