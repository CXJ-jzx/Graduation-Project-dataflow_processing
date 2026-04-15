"""
贝叶斯优化器模块

职责：
1. 候选参数生成（三来源混合采样）
2. GP/RF 预测 + 硬约束预过滤
3. EI 排序 + 安全选择（Top-K 中选 σ 最小的）
4. 效率阈值检查（ET）—— 仅在足够观测后生效
5. 保守模式降级
6. ★ 连续跳过保护：连续 ET 拒绝后强制探索
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
        if config_dict is not None:
            cfg = config_dict
        elif config_path is not None:
            with open(config_path, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f)
        else:
            raise ValueError("必须提供 config_path 或 config_dict")

        self.cfg = cfg
        opt_cfg = cfg['optimizer']

        self.n_candidates = int(opt_cfg['n_candidates'])
        mix = opt_cfg['candidate_mix']
        self.random_ratio = float(mix['random_ratio'])
        self.best_neighbor_ratio = float(mix['best_neighbor_ratio'])
        self.top5_neighbor_ratio = float(mix['top5_neighbor_ratio'])
        self.neighbor_range = float(opt_cfg['neighbor_range'])
        self.top_k_select = int(opt_cfg['top_k_select'])

        safety_cfg = cfg['safety']
        self.efficiency_threshold = float(safety_cfg['efficiency_threshold'])

        # ★ 修改：ET 最少观测数默认值提高到 20
        self.et_min_observations = int(safety_cfg.get('et_min_observations', 20))

        # ★ 新增：连续跳过保护
        self.max_consecutive_skips = int(safety_cfg.get('max_consecutive_skips', 3))
        self._consecutive_skip_count = 0

        # ★ 新增：ET 使用 σ 感知判断（考虑不确定性）
        self.et_use_sigma_aware = bool(safety_cfg.get('et_use_sigma_aware', True))

        # 组件
        self.space = ParameterSpace(config_dict=cfg)
        self.model = SurrogateModel(config_dict=cfg)
        self.acquisition = AcquisitionFunction(config_dict=cfg)

        logger.info("BayesianOptimizer 初始化: candidates=%d, mix=(%.0f%%/%.0f%%/%.0f%%), "
                     "neighbor=%.0f%%, top_k=%d, ET=%.0f%% (min_obs=%d, max_skip=%d)",
                     self.n_candidates,
                     self.random_ratio * 100, self.best_neighbor_ratio * 100,
                     self.top5_neighbor_ratio * 100,
                     self.neighbor_range * 100, self.top_k_select,
                     self.efficiency_threshold * 100, self.et_min_observations,
                     self.max_consecutive_skips)

    # ==========================================
    # 主入口
    # ==========================================

    def suggest(self,
                store: ObservationStore,
                j_current: float) -> Optional[Dict]:
        """
        建议下一组参数配置

        Returns:
            dict 或 None:
                成功时: {"theta": ..., "ei": ..., "mu": ..., "sigma": ..., ...}
                ET 未通过且未触发强制探索: None
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

        tested_norms = X_train
        valid_mask = np.ones(len(candidates_norm), dtype=bool)

        for i in range(len(candidates_norm)):
            for tested in tested_norms:
                if self.space.distance(candidates_norm[i], tested) < 0.01:
                    valid_mask[i] = False
                    break

        if valid_mask.sum() < len(candidates_norm) // 2:
            valid_mask[:] = True

        # ─────────────────────────────
        # 4. 计算 EI + 安全选择
        # ─────────────────────────────
        xi = self.acquisition.compute_xi(n_valid, warm_up_n)
        ei = self.acquisition.compute_ei(mu, sigma, j_best, xi)

        ei[~valid_mask] = 0.0

        if ei.max() <= 0:
            # ★ EI 全为 0 的处理
            if n_valid < self.et_min_observations:
                # 观测不足：选 σ 最大的（最不确定的）去探索
                logger.info("EI 全为 0 但观测不足 (%d < %d)，选择 σ 最大的候选（探索）",
                            n_valid, self.et_min_observations)
                sigma_valid = sigma.copy()
                sigma_valid[~valid_mask] = 0.0
                best_idx = int(np.argmax(sigma_valid))
                best_theta = self.space.denormalize(candidates_norm[best_idx])
                self._consecutive_skip_count = 0
                return {
                    "theta": best_theta,
                    "ei": 0.0,
                    "mu": float(mu[best_idx]),
                    "sigma": float(sigma[best_idx]),
                    "model_type": model_type,
                    "xi": xi,
                    "predicted_improvement": 0.0,
                    "reason": "exploration_fallback",
                }

            # ★ 观测充足但连续跳过次数检查
            self._consecutive_skip_count += 1
            if self._consecutive_skip_count >= self.max_consecutive_skips:
                logger.warning("连续跳过 %d 轮，强制随机探索（打破局部最优）",
                               self._consecutive_skip_count)
                self._consecutive_skip_count = 0
                return self._force_random_exploration(
                    candidates_norm, mu, sigma, valid_mask, model_type, xi
                )

            logger.info("所有候选 EI=0, 且观测充足 (%d >= %d), 跳过 (%d/%d)",
                        n_valid, self.et_min_observations,
                        self._consecutive_skip_count, self.max_consecutive_skips)
            return None

        # 安全选择：EI Top-K 中选 σ 最小的
        top_k_indices = np.argsort(ei)[-self.top_k_select:][::-1]
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

        # 观测数不足时跳过 ET 检查
        if n_valid < self.et_min_observations:
            logger.info("观测不足 (%d < %d)，跳过 ET 检查，强制探索",
                        n_valid, self.et_min_observations)
            self._consecutive_skip_count = 0
            return {
                "theta": best_theta,
                "ei": best_ei,
                "mu": best_mu,
                "sigma": best_sigma,
                "model_type": model_type,
                "xi": xi,
                "predicted_improvement": 0.0,
                "reason": "forced_exploration",
            }

        # ★ 修复：ET 判断逻辑改为 σ 感知
        #   不再只看 μ vs j_best，而是看 (μ - 2σ) vs j_best
        #   即：如果候选在 2σ 置信区间内有可能优于 j_best，就通过 ET
        if j_best > 0:
            if self.et_use_sigma_aware:
                # σ 感知：乐观估计（μ 可能偏高，实际可能更低）
                optimistic_mu = best_mu - 1.5 * best_sigma
                predicted_improvement = (j_best - optimistic_mu) / j_best
                logger.info("ET σ感知: μ=%.4f, σ=%.4f, 乐观μ=%.4f, 改善=%.2f%%",
                            best_mu, best_sigma, optimistic_mu,
                            predicted_improvement * 100)
            else:
                predicted_improvement = (j_best - best_mu) / j_best
        else:
            predicted_improvement = 1.0

        if predicted_improvement < self.efficiency_threshold:
            self._consecutive_skip_count += 1

            # ★ 连续跳过保护
            if self._consecutive_skip_count >= self.max_consecutive_skips:
                logger.warning("ET 连续拒绝 %d 轮，强制探索（防止陷入死循环）",
                               self._consecutive_skip_count)
                self._consecutive_skip_count = 0
                return self._force_random_exploration(
                    candidates_norm, mu, sigma, valid_mask, model_type, xi
                )

            logger.info("ET 未通过: 预测改善=%.2f%% < 阈值%.0f%%, 跳过 (%d/%d)",
                        predicted_improvement * 100, self.efficiency_threshold * 100,
                        self._consecutive_skip_count, self.max_consecutive_skips)
            return None

        logger.info("ET 通过: 预测改善=%.2f%% >= 阈值%.0f%%",
                     predicted_improvement * 100, self.efficiency_threshold * 100)

        self._consecutive_skip_count = 0
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
    # ★ 新增：强制随机探索
    # ==========================================

    def _force_random_exploration(self, candidates_norm, mu, sigma,
                                  valid_mask, model_type, xi) -> Dict:
        """
        连续跳过后的强制探索策略：
        选择有效候选中 σ 最大的（最不确定的区域）
        """
        sigma_valid = sigma.copy()
        sigma_valid[~valid_mask] = 0.0

        # 选 σ 最大的
        best_idx = int(np.argmax(sigma_valid))
        best_theta = self.space.denormalize(candidates_norm[best_idx])

        logger.info("强制探索: θ=%s, μ=%.6f, σ=%.6f（选择最不确定区域）",
                     best_theta, float(mu[best_idx]), float(sigma[best_idx]))

        return {
            "theta": best_theta,
            "ei": 0.0,
            "mu": float(mu[best_idx]),
            "sigma": float(sigma[best_idx]),
            "model_type": model_type,
            "xi": xi,
            "predicted_improvement": 0.0,
            "reason": "forced_exploration_after_skips",
        }

    # ==========================================
    # 保守模式
    # ==========================================

    def suggest_conservative(self, theta_best: Dict[str, float]) -> Dict[str, float]:
        rng = np.random.RandomState()
        theta_norm = self.space.normalize(theta_best)
        conservative_range = 0.10

        candidates = self.space.neighbor_sample(
            theta_norm, conservative_range, n=5, rng=rng
        )

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
        rng = np.random.RandomState()

        n_random = int(self.n_candidates * self.random_ratio)
        n_best = int(self.n_candidates * self.best_neighbor_ratio)
        n_top5 = self.n_candidates - n_random - n_best

        candidates = []

        random_samples = self.space.random_sample(n_random, rng=rng)
        candidates.append(random_samples)

        best_neighbors = self.space.neighbor_sample(
            theta_best_norm, self.neighbor_range, n_best, rng=rng
        )
        candidates.append(best_neighbors)

        if top_k_norms:
            per_top = max(1, n_top5 // len(top_k_norms))
            top5_samples = []
            for tk_norm in top_k_norms:
                nb = self.space.neighbor_sample(
                    tk_norm, self.neighbor_range, per_top, rng=rng
                )
                top5_samples.append(nb)
            top5_all = np.vstack(top5_samples)

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
        all_candidates = np.clip(all_candidates, 0.0, 1.0)

        return all_candidates

    def __repr__(self) -> str:
        return (f"BayesianOptimizer(candidates={self.n_candidates}, "
                f"ET={self.efficiency_threshold:.0%}, "
                f"et_min_obs={self.et_min_observations}, "
                f"max_skip={self.max_consecutive_skips})")
