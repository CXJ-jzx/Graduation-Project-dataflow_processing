"""
贝叶斯优化器模块

职责：
1. 候选参数生成（三来源混合采样）
2. GP/RF 预测 + 硬约束预过滤
3. EI 排序 + 安全选择（Top-K 中选 σ 最小的）
4. 效率阈值检查（ET）
5. 保守模式降级
"""

import numpy as np
from typing import Dict, List, Optional, Tuple
import logging
import yaml

from parameter_space import ParameterSpace
from surrogate_model import SurrogateModel
from acquisition import AcquisitionFunction
from observation_store import ObservationStore

logger = logging.getLogger("ml_tuner.bayesian_optimizer")


class BayesianOptimizer:
    """贝叶斯优化器"""

    def __init__(self, config_path: str = None, config_dict: dict = None):
        """
        初始化贝叶斯优化器

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

        self.cfg = cfg
        opt_cfg = cfg['optimizer']

        # 候选参数生成配置
        self.n_candidates = int(opt_cfg['n_candidates'])
        mix = opt_cfg['candidate_mix']
        self.random_ratio = float(mix['random_ratio'])
        self.best_neighbor_ratio = float(mix['best_neighbor_ratio'])
        self.top5_neighbor_ratio = float(mix['top5_neighbor_ratio'])
        self.neighbor_range = float(opt_cfg['neighbor_range'])

        # 安全选择
        self.top_k_select = int(opt_cfg['top_k_select'])

        # 效率阈值
        safety_cfg = cfg['safety']
        self.efficiency_threshold = float(safety_cfg['efficiency_threshold'])

        # 组件
        self.space = ParameterSpace(config_dict=cfg)
        self.model = SurrogateModel(config_dict=cfg)
        self.acquisition = AcquisitionFunction(config_dict=cfg)

        logger.info("BayesianOptimizer 初始化: candidates=%d, mix=(%.0f%%/%.0f%%/%.0f%%), "
                     "neighbor=%.0f%%, top_k=%d, ET=%.0f%%",
                     self.n_candidates,
                     self.random_ratio * 100, self.best_neighbor_ratio * 100,
                     self.top5_neighbor_ratio * 100,
                     self.neighbor_range * 100, self.top_k_select,
                     self.efficiency_threshold * 100)

    # ==========================================
    # 主入口
    # ==========================================

    def suggest(self,
                store: ObservationStore,
                j_current: float) -> Optional[Dict]:
        """
        建议下一组参数配置

        完整流程：
        1. 导出训练数据 → 拟合代理模型
        2. 生成候选参数（三来源混合）
        3. GP/RF 预测 → 计算 EI
        4. 安全选择（Top-K 中选 σ 最小的）
        5. 效率阈值检查

        Args:
            store: 观测存储
            j_current: 当前配置的 J 值

        Returns:
            dict 或 None:
                成功时: {"theta": dict, "ei": float, "mu": float, "sigma": float,
                         "model_type": str}
                ET 未通过: None
        """
        n_valid = store.count_valid()
        warm_up_n = self.model.warm_up_n
        j_best, theta_best, _ = store.get_best()

        if theta_best is None:
            logger.warning("无历史最优配置，无法进行优化")
            return None

        logger.info("开始 BO 建议: n_valid=%d, j_best=%.6f, j_current=%.6f",
                     n_valid, j_best, j_current)

        # ─────────────────────────────
        # 1. 拟合代理模型
        # ─────────────────────────────
        X_train, y_train = store.get_training_data(self.space)
        if len(y_train) < 2:
            logger.warning("训练数据不足 (n=%d), 无法拟合模型", len(y_train))
            return None

        model_type = self.model.fit(X_train, y_train)
        logger.info("模型拟合: type=%s, n_train=%d", model_type, len(y_train))

        # ─────────────────────────────
        # 2. 生成候选参数
        # ─────────────────────────────
        theta_best_norm = self.space.normalize(theta_best)
        top_k_thetas = store.get_top_k(5)
        top_k_norms = [self.space.normalize(t) for t in top_k_thetas]

        candidates_norm = self._generate_candidates(
            theta_best_norm, top_k_norms
        )
        logger.info("生成 %d 个候选参数", len(candidates_norm))

        # ─────────────────────────────
        # 3. 预测 + 去重过滤
        # ─────────────────────────────
        mu, sigma = self.model.predict(candidates_norm)

        # 去重：与已测试配置距离过近的候选被标记
        tested_norms = X_train  # shape=(n, 3)
        valid_mask = np.ones(len(candidates_norm), dtype=bool)

        for i in range(len(candidates_norm)):
            for tested in tested_norms:
                if self.space.distance(candidates_norm[i], tested) < 0.01:
                    valid_mask[i] = False
                    break

        # 至少保留一半候选
        if valid_mask.sum() < len(candidates_norm) // 2:
            valid_mask[:] = True  # 放弃去重

        # ─────────────────────────────
        # 4. 计算 EI + 安全选择
        # ─────────────────────────────
        xi = self.acquisition.compute_xi(n_valid, warm_up_n)
        ei = self.acquisition.compute_ei(mu, sigma, j_best, xi)

        # 仅对有效候选计算
        ei[~valid_mask] = 0.0

        if ei.max() <= 0:
            logger.info("所有候选 EI=0, 无改善空间")
            return None

        # 安全选择：EI Top-K 中选 σ 最小的
        top_k_indices = np.argsort(ei)[-self.top_k_select:][::-1]  # EI 降序
        # 在 Top-K 中选 σ 最小的
        best_idx = top_k_indices[np.argmin(sigma[top_k_indices])]

        best_theta_norm = candidates_norm[best_idx]
        best_theta = self.space.denormalize(best_theta_norm)
        best_ei = float(ei[best_idx])
        best_mu = float(mu[best_idx])
        best_sigma = float(sigma[best_idx])

        logger.info("最佳候选: θ=%s, EI=%.6f, μ=%.6f, σ=%.6f, ξ=%.4f",
                     best_theta, best_ei, best_mu, best_sigma, xi)

        # ─────────────────────────────
        # 5. 效率阈值检查 (ET)
        # ─────────────────────────────
        if j_current > 0:
            predicted_improvement = (j_current - best_mu) / j_current
        else:
            predicted_improvement = 1.0

        if predicted_improvement < self.efficiency_threshold:
            logger.info("ET 未通过: 预测改善=%.2f%% < 阈值%.0f%%, 跳过本轮",
                        predicted_improvement * 100, self.efficiency_threshold * 100)
            return None

        logger.info("ET 通过: 预测改善=%.2f%% >= 阈值%.0f%%",
                     predicted_improvement * 100, self.efficiency_threshold * 100)

        return {
            "theta": best_theta,
            "ei": best_ei,
            "mu": best_mu,
            "sigma": best_sigma,
            "model_type": model_type,
            "xi": xi,
            "predicted_improvement": predicted_improvement,
        }

    # ==========================================
    # 保守模式
    # ==========================================

    def suggest_conservative(self, theta_best: Dict[str, float]) -> Dict[str, float]:
        """
        保守模式：在最优配置附近小范围随机扰动

        用于 GP 置信度不足或连续回滚后的降级策略

        Args:
            theta_best: 历史最优参数配置

        Returns:
            dict: 微调后的参数配置
        """
        rng = np.random.RandomState()
        theta_norm = self.space.normalize(theta_best)
        conservative_range = 0.10  # ±10%

        candidates = self.space.neighbor_sample(
            theta_norm, conservative_range, n=5, rng=rng
        )

        # 选择离 theta_best 最近的（最保守）
        distances = [self.space.distance(c, theta_norm) for c in candidates]
        best_idx = np.argmin(distances)

        result = self.space.denormalize(candidates[best_idx])
        logger.info("保守建议: θ=%s (距离最优=%.4f)", result, distances[best_idx])

        return result

    # ==========================================
    # 候选生成
    # ==========================================

    def _generate_candidates(self,
                              theta_best_norm: np.ndarray,
                              top_k_norms: List[np.ndarray]) -> np.ndarray:
        """
        三来源混合生成候选参数

        来源 A: 随机均匀采样 (70%) — 全局探索
        来源 B: 最优邻域采样 (20%) — 局部精细搜索
        来源 C: Top-5 邻域采样 (10%) — 利用历史经验

        Args:
            theta_best_norm: 最优配置（归一化）
            top_k_norms: Top-K 配置列表（归一化）

        Returns:
            np.ndarray: shape=(n_candidates, 3)
        """
        rng = np.random.RandomState()

        n_random = int(self.n_candidates * self.random_ratio)
        n_best = int(self.n_candidates * self.best_neighbor_ratio)
        n_top5 = self.n_candidates - n_random - n_best

        candidates = []

        # 来源 A: 随机均匀采样
        random_samples = self.space.random_sample(n_random, rng=rng)
        candidates.append(random_samples)

        # 来源 B: 最优邻域采样
        best_neighbors = self.space.neighbor_sample(
            theta_best_norm, self.neighbor_range, n_best, rng=rng
        )
        candidates.append(best_neighbors)

        # 来源 C: Top-5 邻域采样
        if top_k_norms:
            per_top = max(1, n_top5 // len(top_k_norms))
            top5_samples = []
            for tk_norm in top_k_norms:
                nb = self.space.neighbor_sample(
                    tk_norm, self.neighbor_range, per_top, rng=rng
                )
                top5_samples.append(nb)
            top5_all = np.vstack(top5_samples)

            # 截断或补齐到 n_top5
            if len(top5_all) > n_top5:
                top5_all = top5_all[:n_top5]
            elif len(top5_all) < n_top5:
                extra = self.space.random_sample(n_top5 - len(top5_all), rng=rng)
                top5_all = np.vstack([top5_all, extra])

            candidates.append(top5_all)
        else:
            extra = self.space.random_sample(n_top5, rng=rng)
            candidates.append(extra)

        all_candidates = np.vstack(candidates)

        # 确保所有值在 [0, 1] 范围内
        all_candidates = np.clip(all_candidates, 0.0, 1.0)

        return all_candidates

    def __repr__(self) -> str:
        return (f"BayesianOptimizer(candidates={self.n_candidates}, "
                f"ET={self.efficiency_threshold:.0%})")
