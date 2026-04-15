"""
代理模型模块

职责：
1. Phase 1 (冷启动): Random Forest 代理模型
2. Phase 2 (优化期): Gaussian Process 代理模型
3. 统一的 predict(θ) → (μ, σ) 接口
4. GP 拟合失败时自动降级为 RF
"""

import numpy as np
from typing import Tuple, Optional
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import ConstantKernel, RBF, WhiteKernel
from sklearn.ensemble import RandomForestRegressor
import logging
import warnings

warnings.filterwarnings('ignore', category=UserWarning, module='sklearn')

logger = logging.getLogger("ml_tuner.surrogate_model")


class SurrogateModel:
    """
    两阶段代理模型

    Phase 1 (n < warm_up): RandomForest
    Phase 2 (n >= warm_up): GaussianProcess（失败降级 RF）
    """

    def __init__(self, config_dict: dict = None, config_path: str = None):
        import yaml

        if config_dict is not None:
            cfg = config_dict
        elif config_path is not None:
            with open(config_path, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f)
        else:
            raise ValueError("必须提供 config_path 或 config_dict")

        surrogate_cfg = cfg['surrogate']

        self.warm_up_n = int(surrogate_cfg['warm_up_observations'])

        gp_cfg = surrogate_cfg['gp']
        self.gp_n_restarts = int(gp_cfg['n_restarts_optimizer'])
        self.gp_alpha = float(gp_cfg['alpha'])
        self.gp_normalize_y = bool(gp_cfg['normalize_y'])

        rf_cfg = surrogate_cfg['rf']
        self.rf_n_estimators = int(rf_cfg['n_estimators'])
        self.rf_max_depth = int(rf_cfg['max_depth'])
        self.rf_min_samples_leaf = int(rf_cfg['min_samples_leaf'])

        # ★ 新增：GP 噪声下限，防止过度自信
        self.gp_noise_floor = float(surrogate_cfg.get('gp_noise_floor', 0.01))
        # ★ 新增：预测时的最小 sigma，保证探索性
        self.min_predict_sigma = float(surrogate_cfg.get('min_predict_sigma', 0.005))

        self._model = None
        self._model_type: Optional[str] = None
        self._is_fitted = False

        self._X_train: Optional[np.ndarray] = None
        self._y_train: Optional[np.ndarray] = None

        logger.info("SurrogateModel 初始化完成: warm_up=%d, GP(restarts=%d, alpha=%g, "
                     "noise_floor=%g), RF(trees=%d, depth=%d), min_sigma=%g",
                     self.warm_up_n, self.gp_n_restarts, self.gp_alpha,
                     self.gp_noise_floor,
                     self.rf_n_estimators, self.rf_max_depth,
                     self.min_predict_sigma)

    # ==========================================
    # 模型拟合
    # ==========================================

    def fit(self, X: np.ndarray, y: np.ndarray) -> str:
        n = len(y)
        self._X_train = X.copy()
        self._y_train = y.copy()

        if n < self.warm_up_n:
            return self._fit_rf(X, y)
        else:
            try:
                return self._fit_gp(X, y)
            except Exception as e:
                logger.warning("GP 拟合失败 (%s), 降级使用 RF", e)
                return self._fit_rf(X, y)

    def _fit_rf(self, X: np.ndarray, y: np.ndarray) -> str:
        self._model = RandomForestRegressor(
            n_estimators=self.rf_n_estimators,
            max_depth=self.rf_max_depth,
            min_samples_leaf=self.rf_min_samples_leaf,
            random_state=42,
            n_jobs=-1,
        )
        self._model.fit(X, y)
        self._model_type = "rf"
        self._is_fitted = True
        logger.info("RF 拟合完成: n=%d, n_trees=%d", len(y), self.rf_n_estimators)
        return "rf"

    def _fit_gp(self, X: np.ndarray, y: np.ndarray) -> str:
        n_dims = X.shape[1]

        # ★ 修复：噪声下限从 1e-5 提高到 gp_noise_floor
        #   防止 GP 过度自信（同参数不同轮次 J 差 0.05，noise 不应该只有 0.035）
        noise_lb = max(self.gp_noise_floor, 1e-3)

        kernel = (
            ConstantKernel(1.0, constant_value_bounds=(1e-3, 1e3)) *
            RBF(
                length_scale=[1.0] * n_dims,
                length_scale_bounds=(1e-2, 1e2)
            ) +
            WhiteKernel(
                noise_level=max(0.1, noise_lb),          # ★ 初始值也提高
                noise_level_bounds=(noise_lb, 1e1)       # ★ 下限提高
            )
        )

        # ★ 修复：alpha 也设下限，双重保障
        effective_alpha = max(self.gp_alpha, self.gp_noise_floor * 0.1)

        self._model = GaussianProcessRegressor(
            kernel=kernel,
            n_restarts_optimizer=self.gp_n_restarts,
            alpha=effective_alpha,                        # ★ 使用有下限的 alpha
            normalize_y=self.gp_normalize_y,
            random_state=42,
        )
        self._model.fit(X, y)
        self._model_type = "gp"
        self._is_fitted = True

        # ★ 新增：检查拟合后的噪声是否过低
        if hasattr(self._model, 'kernel_'):
            kernel_str = str(self._model.kernel_)
            logger.info("GP 拟合完成: n=%d, kernel=%s", len(y), kernel_str)

            # 提取 noise_level 并警告
            for param_name, param_val in self._model.kernel_.get_params().items():
                if 'noise_level' in param_name and isinstance(param_val, float):
                    if param_val < noise_lb * 1.1:
                        logger.warning("GP noise_level=%.4f 接近下限 %.4f，"
                                       "模型可能仍然过度自信", param_val, noise_lb)
        else:
            logger.info("GP 拟合完成: n=%d", len(y))

        return "gp"

    # ==========================================
    # 预测
    # ==========================================

    def predict(self, X: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        if not self._is_fitted:
            raise RuntimeError("模型未拟合，请先调用 fit()")

        if X.ndim == 1:
            X = X.reshape(1, -1)

        if self._model_type == "gp":
            mu, sigma = self._model.predict(X, return_std=True)
        elif self._model_type == "rf":
            mu, sigma = self._predict_rf(X)
        else:
            raise RuntimeError(f"未知模型类型: {self._model_type}")

        # ★ 修复：sigma 最小值从 1e-10 提高到 min_predict_sigma
        #   保证即使 GP 过度自信，EI 计算仍有非零探索项
        sigma = np.maximum(sigma, self.min_predict_sigma)

        return mu, sigma

    def _predict_rf(self, X: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        tree_predictions = np.array([
            tree.predict(X) for tree in self._model.estimators_
        ])
        mu = np.mean(tree_predictions, axis=0)
        sigma = np.std(tree_predictions, axis=0)
        return mu, sigma

    # ==========================================
    # 状态查询
    # ==========================================

    def get_model_type(self) -> Optional[str]:
        return self._model_type

    def is_fitted(self) -> bool:
        return self._is_fitted

    def get_training_stats(self) -> dict:
        if not self._is_fitted:
            return {"fitted": False}

        stats = {
            "fitted": True,
            "model_type": self._model_type,
            "n_train": len(self._y_train) if self._y_train is not None else 0,
        }

        if self._model_type == "gp" and hasattr(self._model, 'kernel_'):
            stats["kernel"] = str(self._model.kernel_)
            stats["log_marginal_likelihood"] = float(
                self._model.log_marginal_likelihood_value_
            )

        return stats

    def __repr__(self) -> str:
        if self._is_fitted:
            n = len(self._y_train) if self._y_train is not None else 0
            return f"SurrogateModel(type={self._model_type}, n_train={n})"
        else:
            return "SurrogateModel(not fitted)"
