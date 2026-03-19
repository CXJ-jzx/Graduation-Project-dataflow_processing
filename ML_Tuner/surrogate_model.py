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

# 忽略 sklearn 收敛警告
warnings.filterwarnings('ignore', category=UserWarning, module='sklearn')

logger = logging.getLogger("ml_tuner.surrogate_model")


class SurrogateModel:
    """
    两阶段代理模型

    Phase 1 (n < warm_up): RandomForest
        - 对少量数据更鲁棒
        - 不确定性通过各棵树预测的标准差估算

    Phase 2 (n >= warm_up): GaussianProcess
        - 提供有理论保证的不确定性估计
        - 支持 EI 采集函数的精确计算
        - 拟合失败时自动降级回 RF
    """

    def __init__(self, config_dict: dict = None, config_path: str = None):
        """
        初始化代理模型

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

        surrogate_cfg = cfg['surrogate']

        # 冷启动阈值
        self.warm_up_n = int(surrogate_cfg['warm_up_observations'])

        # GP 配置
        gp_cfg = surrogate_cfg['gp']
        self.gp_n_restarts = int(gp_cfg['n_restarts_optimizer'])
        self.gp_alpha = float(gp_cfg['alpha'])
        self.gp_normalize_y = bool(gp_cfg['normalize_y'])

        # RF 配置
        rf_cfg = surrogate_cfg['rf']
        self.rf_n_estimators = int(rf_cfg['n_estimators'])
        self.rf_max_depth = int(rf_cfg['max_depth'])
        self.rf_min_samples_leaf = int(rf_cfg['min_samples_leaf'])

        # 当前使用的模型
        self._model = None
        self._model_type: Optional[str] = None  # "rf" 或 "gp"
        self._is_fitted = False

        # 训练数据缓存（用于降级时快速切换）
        self._X_train: Optional[np.ndarray] = None
        self._y_train: Optional[np.ndarray] = None

        logger.info("SurrogateModel 初始化完成: warm_up=%d, GP(restarts=%d, alpha=%g), "
                     "RF(trees=%d, depth=%d)",
                     self.warm_up_n, self.gp_n_restarts, self.gp_alpha,
                     self.rf_n_estimators, self.rf_max_depth)

    # ==========================================
    # 模型拟合
    # ==========================================

    def fit(self, X: np.ndarray, y: np.ndarray) -> str:
        """
        根据数据量自动选择并拟合代理模型

        Args:
            X: 训练特征, shape=(n, 3), 归一化后的参数
            y: 训练目标, shape=(n,), 目标函数值

        Returns:
            str: 实际使用的模型类型 ("rf" 或 "gp")
        """
        n = len(y)
        self._X_train = X.copy()
        self._y_train = y.copy()

        if n < self.warm_up_n:
            # Phase 1: 冷启动，使用 RF
            return self._fit_rf(X, y)
        else:
            # Phase 2: 尝试 GP，失败则降级 RF
            try:
                return self._fit_gp(X, y)
            except Exception as e:
                logger.warning("GP 拟合失败 (%s), 降级使用 RF", e)
                return self._fit_rf(X, y)

    def _fit_rf(self, X: np.ndarray, y: np.ndarray) -> str:
        """拟合 Random Forest"""
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
        """拟合 Gaussian Process"""
        n_dims = X.shape[1]

        # 构建核函数: C * RBF + WhiteKernel
        kernel = (
            ConstantKernel(1.0, constant_value_bounds=(1e-3, 1e3)) *
            RBF(
                length_scale=[1.0] * n_dims,
                length_scale_bounds=(1e-2, 1e2)
            ) +
            WhiteKernel(
                noise_level=0.1,
                noise_level_bounds=(1e-5, 1e1)
            )
        )

        self._model = GaussianProcessRegressor(
            kernel=kernel,
            n_restarts_optimizer=self.gp_n_restarts,
            alpha=self.gp_alpha,
            normalize_y=self.gp_normalize_y,
            random_state=42,
        )
        self._model.fit(X, y)
        self._model_type = "gp"
        self._is_fitted = True

        logger.info("GP 拟合完成: n=%d, kernel=%s", len(y), self._model.kernel_)
        return "gp"

    # ==========================================
    # 预测
    # ==========================================

    def predict(self, X: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """
        预测目标值的均值和标准差

        Args:
            X: 预测点, shape=(m, 3), 归一化后的参数

        Returns:
            (mu, sigma):
                mu: shape=(m,), 预测均值
                sigma: shape=(m,), 预测标准差
        """
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

        # 确保 sigma 非负
        sigma = np.maximum(sigma, 1e-10)

        return mu, sigma

    def _predict_rf(self, X: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """
        RF 预测：均值 + 各棵树预测的标准差

        Args:
            X: shape=(m, 3)

        Returns:
            (mu, sigma): shape=(m,), shape=(m,)
        """
        # 获取每棵树的预测
        tree_predictions = np.array([
            tree.predict(X) for tree in self._model.estimators_
        ])  # shape=(n_trees, m)

        mu = np.mean(tree_predictions, axis=0)     # shape=(m,)
        sigma = np.std(tree_predictions, axis=0)    # shape=(m,)

        return mu, sigma

    # ==========================================
    # 状态查询
    # ==========================================

    def get_model_type(self) -> Optional[str]:
        """获取当前使用的模型类型"""
        return self._model_type

    def is_fitted(self) -> bool:
        """模型是否已拟合"""
        return self._is_fitted

    def get_training_stats(self) -> dict:
        """获取训练统计"""
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
