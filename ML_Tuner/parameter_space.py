"""
参数空间管理模块

职责：
1. 3维调优参数的定义、归一化、反归一化
2. LHS（拉丁超立方）采样生成冷启动候选
3. 随机采样、邻域采样
4. 离散化（对齐到step）、边界裁剪
"""

import numpy as np
from typing import Dict, List, Tuple, Optional
import yaml
import logging

logger = logging.getLogger("ml_tuner.parameter_space")


class ParameterSpace:
    """3维调优参数空间管理器"""

    # 参数名称（固定顺序，与GP输入维度对应）
    PARAM_NAMES = ["checkpoint_interval", "buffer_timeout", "watermark_delay"]

    def __init__(self, config_path: str = None, config_dict: dict = None):
        """
        初始化参数空间

        Args:
            config_path: 配置文件路径
            config_dict: 直接传入配置字典（支持完整配置或仅 parameter_space 子配置）
        """
        if config_dict is not None:
            # 兼容：传入完整配置 或 仅 parameter_space 子配置
            if 'parameter_space' in config_dict:
                space_cfg = config_dict['parameter_space']
            else:
                space_cfg = config_dict
        elif config_path is not None:
            with open(config_path, 'r', encoding='utf-8') as f:
                full_config = yaml.safe_load(f)
            space_cfg = full_config['parameter_space']
        else:
            raise ValueError("必须提供 config_path 或 config_dict")

        # 解析每个参数的边界
        self.params = {}
        for name in self.PARAM_NAMES:
            cfg = space_cfg[name]
            self.params[name] = {
                'min': float(cfg['min']),
                'max': float(cfg['max']),
                'step': float(cfg['step']),
                'default': float(cfg['default']),
                'cli_arg': cfg.get('cli_arg', name.replace('_', '-')),
            }

        self.n_dims = len(self.PARAM_NAMES)

        logger.info("参数空间初始化完成: %d 维", self.n_dims)
        for name, p in self.params.items():
            n_levels = int((p['max'] - p['min']) / p['step']) + 1
            logger.info("  %s: [%g, %g], step=%g, levels=%d, default=%g",
                        name, p['min'], p['max'], p['step'], n_levels, p['default'])

    def get_defaults(self) -> Dict[str, float]:
        """获取默认参数值"""
        return {name: p['default'] for name, p in self.params.items()}

    def get_bounds(self) -> List[Tuple[float, float]]:
        """获取归一化空间的边界列表 [(0,1), (0,1), (0,1)]"""
        return [(0.0, 1.0)] * self.n_dims

    def get_raw_bounds(self) -> List[Tuple[float, float]]:
        """获取原始空间的边界列表"""
        return [(p['min'], p['max']) for p in self.params.values()]

    # ==========================================
    # 归一化 / 反归一化
    # ==========================================

    def normalize(self, theta_dict: Dict[str, float]) -> np.ndarray:
        """
        原始参数 → [0,1] 归一化

        Args:
            theta_dict: {"checkpoint_interval": 30000, "buffer_timeout": 100, ...}

        Returns:
            np.ndarray: shape=(3,), 值域 [0,1]
        """
        result = np.zeros(self.n_dims)
        for i, name in enumerate(self.PARAM_NAMES):
            p = self.params[name]
            val = float(theta_dict[name])
            result[i] = (val - p['min']) / (p['max'] - p['min'])
            result[i] = np.clip(result[i], 0.0, 1.0)
        return result

    def denormalize(self, theta_norm: np.ndarray) -> Dict[str, float]:
        """
        [0,1] 归一化 → 原始参数（含离散化 + 裁剪）

        Args:
            theta_norm: shape=(3,), 值域 [0,1]

        Returns:
            dict: {"checkpoint_interval": 30000, "buffer_timeout": 100, ...}
        """
        result = {}
        for i, name in enumerate(self.PARAM_NAMES):
            p = self.params[name]
            # 反归一化
            val = theta_norm[i] * (p['max'] - p['min']) + p['min']
            # 离散化：对齐到 step
            val = round(val / p['step']) * p['step']
            # 裁剪到边界
            val = max(p['min'], min(p['max'], val))
            # 整数化（这些参数都是毫秒级整数）
            result[name] = int(val)
        return result

    # ==========================================
    # 采样方法
    # ==========================================

    def latin_hypercube_sample(self, n: int, seed: int = 42) -> List[Dict[str, float]]:
        """
        LHS（拉丁超立方）采样

        在每个维度上均匀分层采样，确保覆盖性。
        用于冷启动阶段生成初始候选配置。

        Args:
            n: 采样数量
            seed: 随机种子

        Returns:
            List[dict]: n 个参数配置字典
        """
        rng = np.random.RandomState(seed)
        samples = np.zeros((n, self.n_dims))

        for dim in range(self.n_dims):
            # 将 [0,1] 分成 n 个等分层
            perm = rng.permutation(n)
            for i in range(n):
                low = perm[i] / n
                high = (perm[i] + 1) / n
                samples[i, dim] = rng.uniform(low, high)

        # 反归一化为实际参数值
        results = []
        for i in range(n):
            theta_dict = self.denormalize(samples[i])
            results.append(theta_dict)

        logger.info("LHS采样完成: 生成 %d 个候选配置", n)
        return results

    def random_sample(self, n: int, rng: np.random.RandomState = None) -> np.ndarray:
        """
        在 [0,1]^3 空间中均匀随机采样

        Args:
            n: 采样数量
            rng: 随机数生成器

        Returns:
            np.ndarray: shape=(n, 3), 归一化空间中的候选点
        """
        if rng is None:
            rng = np.random.RandomState()
        return rng.uniform(0.0, 1.0, size=(n, self.n_dims))

    def neighbor_sample(self, theta_norm: np.ndarray, range_pct: float,
                        n: int, rng: np.random.RandomState = None) -> np.ndarray:
        """
        在给定点的邻域内采样

        Args:
            theta_norm: shape=(3,), 中心点（归一化）
            range_pct: 邻域范围（每个维度 ± range_pct）
            n: 采样数量
            rng: 随机数生成器

        Returns:
            np.ndarray: shape=(n, 3), 邻域内的候选点
        """
        if rng is None:
            rng = np.random.RandomState()

        candidates = np.zeros((n, self.n_dims))
        for i in range(n):
            for dim in range(self.n_dims):
                low = max(0.0, theta_norm[dim] - range_pct)
                high = min(1.0, theta_norm[dim] + range_pct)
                candidates[i, dim] = rng.uniform(low, high)

        return candidates

    # ==========================================
    # 工具方法
    # ==========================================

    def to_cli_args(self, theta_dict: Dict[str, float]) -> str:
        """
        将参数字典转为 Flink CLI 参数字符串

        Args:
            theta_dict: {"checkpoint_interval": 30000, ...}

        Returns:
            str: "--checkpoint-interval 30000 --buffer-timeout 100 --watermark-delay 5000"
        """
        parts = []
        for name in self.PARAM_NAMES:
            cli_arg = self.params[name]['cli_arg']
            val = int(theta_dict[name])
            parts.append(f"--{cli_arg} {val}")
        return " ".join(parts)

    def is_same_config(self, a: Dict[str, float], b: Dict[str, float]) -> bool:
        """判断两组参数是否相同（离散化后比较）"""
        for name in self.PARAM_NAMES:
            if int(a.get(name, 0)) != int(b.get(name, 0)):
                return False
        return True

    def distance(self, a_norm: np.ndarray, b_norm: np.ndarray) -> float:
        """计算归一化空间中两点的欧氏距离"""
        return float(np.linalg.norm(a_norm - b_norm))

    def __repr__(self) -> str:
        lines = [f"ParameterSpace({self.n_dims}D):"]
        for name, p in self.params.items():
            lines.append(f"  {name}: [{p['min']}, {p['max']}] step={p['step']}")
        return "\n".join(lines)

    def sample_random_single(self) -> Optional[Dict[str, float]]:
        """随机采样一个参数配置"""
        import random
        theta = {}
        for name, info in self.params.items():
            low = info['min']
            high = info['max']
            step = info.get('step', 1)
            n_steps = int((high - low) / step)
            theta[name] = low + random.randint(0, n_steps) * step
        return theta

