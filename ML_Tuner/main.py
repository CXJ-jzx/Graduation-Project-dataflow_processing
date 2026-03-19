#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ML Tuner 主循环 (修正版)

修正内容:
1. _build_executor / _build_collector 使用 config_dict= 关键字参数
2. _execute_round 调用 apply_parameters() 而非 apply_config()
3. DS2 锁管理交给 apply_parameters 内部处理
4. _health_check 匹配 is_job_healthy()
5. _collect_features 正确聚合 List[Dict] 样本
6. 移除过度防御性代码，简化调用链
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import time
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import yaml

from bayesian_optimizer import BayesianOptimizer
from feature_engineer import FeatureEngineer
from job_executor import JobExecutor
from observation_collector import ObservationCollector
from observation_store import ObservationStore
from parameter_space import ParameterSpace

logger = logging.getLogger("ml_tuner.main")


def load_yaml_config(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}


def nested_get(mapping: Dict[str, Any], path, default=None):
    cur = mapping
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def setup_logging(level: str = "INFO"):
    numeric_level = getattr(logging, str(level).upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        force=True,
    )


class MLTunerLoop:
    def __init__(self, config_path: str = "ml_tuner_config.yaml",
                 config: Optional[Dict[str, Any]] = None):
        self.config_path = config_path
        self.config = config or load_yaml_config(config_path)

        log_level = self._cfg_first(
            [("logging", "level"), ("log", "level")],
            default="INFO",
        )
        setup_logging(log_level)

        # 运行状态
        self.stop_requested = False
        self._running = True
        self.failure_streak = 0
        self._lhs_cache = None
        self._current_theta = None

        # 调度参数
        self.warm_up_rounds = int(self._cfg_first(
            [("scheduler", "warm_up_rounds"), ("surrogate", "warm_up_observations")],
            default=8,
        ))
        self.max_rounds = int(self._cfg_first(
            [("scheduler", "max_rounds"), ("tuner", "max_rounds")],
            default=20,
        ))
        self.max_consecutive_failures = int(self._cfg_first(
            [("scheduler", "max_consecutive_failures"),
             ("rollback", "max_consecutive_failures")],
            default=3,
        ))
        self.rollback_on_constraint_violation = bool(self._cfg_first(
            [("scheduler", "rollback_on_constraint_violation"),
             ("rollback", "rollback_on_constraint_violation")],
            default=False,
        ))

        # 采集参数
        self.window_seconds = int(self._cfg_first(
            [("metrics", "collection", "window_seconds")],
            default=300,
        ))
        self.collection_interval_seconds = int(self._cfg_first(
            [("metrics", "collection", "interval_seconds"),
             ("collection", "window_interval_seconds")],
            default=30,
        ))
        self.sample_count = int(self._cfg_first(
            [("metrics", "collection", "sample_count"),
             ("collection", "windows_per_round")],
            default=10,
        ))

        # 预热时间
        self.warmup_seconds = int(self._cfg_first(
            [("scheduler", "warm_up_after_apply_seconds"),
             ("scheduler", "warmup_seconds"),
             ("executor", "warmup_seconds")],
            default=90,
        ))

        # CSV 路径
        self.csv_path = self._cfg_first(
            [("storage", "csv_path"), ("observation_store", "csv_path"),
             ("output", "csv_path"), ("store", "csv_path")],
            default="ml_tuner_observations.csv",
        )

        # ━━━ 构建各组件 ━━━
        self.space = self._build_space()
        self.fe = self._build_feature_engineer()
        self.store = self._build_store()
        self.optimizer = self._build_optimizer()
        self.executor = self._build_executor()
        self.collector = self._build_collector()

        # 加载历史数据
        if hasattr(self.store, "load_csv") and self.csv_path and os.path.exists(self.csv_path):
            try:
                self.store.load_csv(self.csv_path)
                logger.info("已加载历史观测: %d 条", self._store_count_total())
            except Exception as e:
                logger.warning("加载历史观测失败: %s", e)

        self._restore_feature_engineer_baseline()

        # 信号处理
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except Exception:
            pass

    # ================================================================
    # 配置辅助
    # ================================================================

    def _cfg(self, *path, default=None):
        return nested_get(self.config, path, default)

    def _cfg_first(self, paths, default=None):
        for path in paths:
            v = nested_get(self.config, path, default=None)
            if v is not None:
                return v
        return default

    # ================================================================
    # 组件构建（签名已确认，全部使用 config_dict=）
    # ================================================================

    def _build_space(self):
        return ParameterSpace(config_dict=self.config)

    def _build_feature_engineer(self):
        return FeatureEngineer(config_dict=self.config)

    def _build_store(self):
        return ObservationStore(
            csv_path=self.csv_path,
            autosave=False,
            config_dict=self.config,
        )

    def _build_optimizer(self):
        return BayesianOptimizer(config_dict=self.config)

    def _build_executor(self):
        return JobExecutor(config_dict=self.config)

    def _build_collector(self):
        return ObservationCollector(config_dict=self.config)


    # ================================================================
    # 信号处理与安全中断
    # ================================================================

    def _signal_handler(self, signum, frame):
        logger.warning("收到退出信号 %s，准备安全停止...", signum)
        self.stop_requested = True
        self._running = False

    def _interruptible_sleep(self, seconds: float):
        end = time.time() + max(0.0, float(seconds))
        while time.time() < end:
            if self.stop_requested or not self._running:
                break
            remaining = end - time.time()
            if remaining <= 0:
                break
            time.sleep(min(2.0, remaining))

    # ================================================================
    # Store 操作辅助
    # ================================================================

    def _store_count_total(self) -> int:
        if hasattr(self.store, "count_total"):
            try:
                return int(self.store.count_total())
            except Exception:
                pass
        return len(getattr(self.store, "records", []))

    def _store_count_valid(self) -> int:
        if hasattr(self.store, "count_valid"):
            try:
                return int(self.store.count_valid())
            except Exception:
                pass
        records = getattr(self.store, "records", [])
        return sum(1 for r in records
                   if getattr(r, "is_valid", getattr(r, "valid", False)))

    def _get_best(self):
        """返回 (j_best, theta_best, record_best)"""
        if hasattr(self.store, "get_best"):
            ret = self.store.get_best()
            if isinstance(ret, tuple):
                if len(ret) >= 3:
                    return ret[0], ret[1], ret[2]
                if len(ret) == 2:
                    return ret[0], ret[1], None
        return None, None, None

    def _get_j_current(self) -> float:
        """获取当前最优 J 值"""
        j_best, _, _ = self._get_best()
        if j_best is not None:
            return float(j_best)

        if hasattr(self.store, "get_latest"):
            latest = self.store.get_latest(valid_only=True)
            if latest is not None:
                return float(latest.j_total)

        return float("inf")

    def _save_store(self):
        if hasattr(self.store, "save_csv"):
            try:
                self.store.save_csv()
            except Exception as e:
                logger.warning("save_csv 失败: %s", e)

    def _store_add_record(self, theta, features, j_total, j_parts=None,
                           is_valid=True, reason="", phase="", round_id=-1):
        """兼容多种 store.add 签名"""
        payloads = [
            dict(theta=theta, features=features, j_total=j_total,
                 j_parts=j_parts or {}, is_valid=is_valid,
                 reason=reason, phase=phase, round_id=round_id),
            dict(theta=theta, features=features, j_total=j_total,
                 parts=j_parts or {}, valid=is_valid,
                 reason=reason, phase=phase, round_id=round_id),
            dict(theta=theta, features=features, j_total=j_total,
                 is_valid=is_valid, reason=reason),
            dict(theta=theta, features=features, j_total=j_total),
        ]
        for payload in payloads:
            try:
                return self.store.add(**payload)
            except TypeError:
                continue
        raise RuntimeError("store.add 所有签名都不匹配")

    def _store_mark_invalid(self, theta, features=None, j_total=float("inf"),
                             j_parts=None, reason="", phase="", round_id=-1):
        """记录无效观测"""
        if hasattr(self.store, "mark_invalid"):
            payloads = [
                dict(theta=theta, features=features or {}, j_total=j_total,
                     j_parts=j_parts or {}, reason=reason,
                     phase=phase, round_id=round_id),
                dict(theta=theta, features=features or {}, j_total=j_total,
                     reason=reason),
                dict(theta=theta, reason=reason),
            ]
            for payload in payloads:
                try:
                    return self.store.mark_invalid(**payload)
                except TypeError:
                    continue

        return self._store_add_record(
            theta=theta, features=features or {}, j_total=j_total,
            j_parts=j_parts or {}, is_valid=False, reason=reason,
            phase=phase, round_id=round_id,
        )

    # ================================================================
    # Baseline 恢复
    # ================================================================

    def _restore_feature_engineer_baseline(self):
        if self._store_count_valid() <= 0:
            return

        baseline = None
        if hasattr(self.store, "estimate_throughput_baseline"):
            try:
                baseline = self.store.estimate_throughput_baseline()
            except Exception:
                pass

        if baseline is None:
            return

        for attr in ("throughput_baseline", "baseline_throughput", "tput_baseline"):
            if hasattr(self.fe, attr):
                try:
                    setattr(self.fe, attr, baseline)
                    logger.info("已恢复吞吐量 baseline=%.4f", baseline)
                    return
                except Exception:
                    pass

    # ================================================================
    # 采集与评估 ★ 修正点 5
    # ================================================================

    def _collect_features(self) -> Dict[str, float]:
        """
        采集并聚合指标

        ★ 修正: collect_window() 返回 List[Dict]，需要聚合为单个 Dict
        """
        samples: List[Dict] = self.collector.collect_window()

        if not samples:
            logger.error("采集返回空样本")
            return {}

        # 如果只有 1 个样本，直接返回
        if len(samples) == 1:
            return samples[0]

        # 尝试用 FeatureEngineer 聚合
        for method_name in ("aggregate_median", "aggregate_window",
                            "aggregate_samples", "extract_features"):
            if hasattr(self.fe, method_name):
                try:
                    result = getattr(self.fe, method_name)(samples)
                    if isinstance(result, dict) and result:
                        return result
                except Exception as e:
                    logger.debug("聚合方法 %s 失败: %s", method_name, e)
                    continue

        # 兜底: 手动取中位数
        logger.info("使用兜底聚合: 取各字段中位数")
        all_keys = set()
        for s in samples:
            all_keys.update(s.keys())

        aggregated = {}
        for key in all_keys:
            values = [s[key] for s in samples if key in s
                      and isinstance(s[key], (int, float))
                      and not np.isnan(s[key])]
            if values:
                aggregated[key] = float(np.median(values))
            else:
                aggregated[key] = 0.0

        return aggregated

    def _compute_objective(self, features: Dict[str, float]) -> Tuple[float, Dict]:
        """计算目标函数"""
        result = self.fe.compute_objective(features)

        if isinstance(result, tuple):
            if len(result) >= 2:
                return float(result[0]), dict(result[1] or {})
            return float(result[0]), {}
        if isinstance(result, dict) and "j_total" in result:
            return float(result["j_total"]), dict(result.get("j_parts", {}))
        return float(result), {}

    def _check_constraints(self, features: Dict[str, float]) -> Tuple[bool, str]:
        """检查约束"""
        result = self.fe.check_constraints(features)

        if isinstance(result, tuple):
            if len(result) >= 2:
                return bool(result[0]), str(result[1])
            return bool(result[0]), ""
        if isinstance(result, dict):
            valid = result.get("is_valid", result.get("valid", True))
            reason = result.get("reason", "")
            return bool(valid), str(reason)
        return bool(result), ""

    # ================================================================
    # 参数建议 ★ 修正点: 正确调用 bo.suggest(store, j_current)
    # ================================================================

    def _materialize_theta(self, candidate) -> Optional[Dict]:
        """将各种格式的候选转换为 {param_name: value} 字典"""
        if candidate is None:
            return None

        if isinstance(candidate, dict):
            # 检查是否是嵌套结构
            for key in ("theta", "params", "candidate"):
                if key in candidate and isinstance(candidate[key], dict):
                    return dict(candidate[key])
            return dict(candidate)

        if isinstance(candidate, (list, tuple, np.ndarray)):
            if hasattr(self.space, "denormalize"):
                try:
                    theta = self.space.denormalize(candidate)
                    if isinstance(theta, dict):
                        return theta
                except Exception:
                    pass

        return None

    def _fallback_theta(self) -> Dict:
        """获取安全的回退参数"""
        _, theta_best, _ = self._get_best()
        if theta_best is not None:
            return dict(theta_best)

        if hasattr(self.space, "get_defaults"):
            return self.space.get_defaults()

        # 硬编码默认值
        return {
            "checkpoint_interval": 30000,
            "buffer_timeout": 100,
            "watermark_delay": 5000,
        }

    def _suggest_theta(self) -> Tuple[Optional[Dict], Dict]:
        """
        调用 BayesianOptimizer.suggest(store, j_current)

        Returns:
            (theta_dict, info_dict)
            theta_dict 可能为 None（ET 拒绝时）
        """
        j_current = self._get_j_current()

        if self.optimizer is None or not hasattr(self.optimizer, "suggest"):
            return self._fallback_theta(), {"source": "fallback"}

        # 尝试调用 suggest
        ret = None
        try:
            ret = self.optimizer.suggest(self.store, j_current)
        except TypeError:
            try:
                ret = self.optimizer.suggest(self.store)
            except TypeError:
                try:
                    ret = self.optimizer.suggest()
                except Exception as e:
                    logger.warning("optimizer.suggest 全部失败: %s", e)
                    return self._fallback_theta(), {"source": "fallback"}

        # ★ suggest 返回 None = ET 拒绝，不值得尝试
        if ret is None:
            logger.info("BO 认为无改善空间 (ET 拒绝)，跳过本轮")
            return None, {"source": "et_rejected"}

        # 解析返回值
        info = {}
        theta = None

        if isinstance(ret, dict):
            # suggest 返回的是 {"theta": {...}, "ei": ..., ...}
            theta = self._materialize_theta(ret)
            info = {k: v for k, v in ret.items() if k != "theta"}
            info["source"] = "bo"

        elif isinstance(ret, tuple):
            if len(ret) >= 1:
                theta = self._materialize_theta(ret[0])
            if len(ret) >= 2 and isinstance(ret[1], dict):
                info = ret[1]
        else:
            theta = self._materialize_theta(ret)

        if theta is None:
            logger.warning("suggest 返回无法解析: %s", type(ret))
            return self._fallback_theta(), {"source": "fallback"}

        return theta, info


    # ================================================================
    # 核心执行 ★ 修正点 3, 4
    # ================================================================

    def _execute_round(self, round_id: int, theta: Dict[str, Any],
                        phase: str = "optimize") -> bool:
        """
        执行一轮优化

        ★ 关键修正:
        1. 调用 executor.apply_parameters(theta)（不是 apply_config）
        2. DS2 锁由 apply_parameters 内部管理（不在这里处理）
        3. apply_parameters 后 reset_cache（因为 Job ID 变了）
        4. 健康检查用 is_job_healthy()
        """
        if self.stop_requested or not self._running:
            logger.warning("已停止，跳过 round=%d", round_id)
            return False

        self._current_theta = theta
        logger.info("=" * 50)
        logger.info("Round %d [%s] 开始, theta=%s", round_id, phase, theta)
        logger.info("=" * 50)

        try:
            # ━━━ 1. 应用参数（Savepoint → Cancel → Resubmit） ━━━
            logger.info("[Round %d] 应用参数到 Flink...", round_id)
            ok = self.executor.apply_parameters(theta)

            if not ok:
                logger.error("[Round %d] apply_parameters 失败", round_id)
                self.failure_streak += 1
                self._store_mark_invalid(
                    theta=theta, reason="apply_parameters_failed",
                    phase=phase, round_id=round_id,
                )
                return False

            # ━━━ 2. 重置采集器缓存（Job ID 已变） ━━━
            if hasattr(self.collector, "reset_cache"):
                self.collector.reset_cache()

            # ━━━ 3. 预热等待 ━━━
            logger.info("[Round %d] 预热等待 %ds...", round_id, self.warmup_seconds)
            self._interruptible_sleep(self.warmup_seconds)

            if self.stop_requested:
                return False

            # ━━━ 4. 健康检查 ━━━
            if not self._health_check():
                logger.error("[Round %d] 健康检查失败", round_id)
                self.failure_streak += 1
                self._store_mark_invalid(
                    theta=theta, reason="health_check_failed",
                    phase=phase, round_id=round_id,
                )
                return False

            # ━━━ 5. 采集指标 ━━━
            logger.info("[Round %d] 开始采集指标 (%d 次, 间隔 %ds)...",
                        round_id, self.sample_count, self.collection_interval_seconds)
            features = self._collect_features()

            if not features:
                logger.error("[Round %d] 采集返回空", round_id)
                self.failure_streak += 1
                self._store_mark_invalid(
                    theta=theta, reason="empty_features",
                    phase=phase, round_id=round_id,
                )
                return False

            # ━━━ 6. 计算目标函数 ━━━
            j_total, j_parts = self._compute_objective(features)

            # ━━━ 7. 检查约束 ━━━
            is_valid, reason = self._check_constraints(features)

            if not is_valid:
                logger.warning("[Round %d] 约束违规: %s, J=%.6f",
                               round_id, reason, j_total)
                self._store_mark_invalid(
                    theta=theta, features=features, j_total=j_total,
                    j_parts=j_parts, reason=reason,
                    phase=phase, round_id=round_id,
                )
                self.failure_streak += 1

                if self.rollback_on_constraint_violation:
                    self._rollback_to_best(reason=reason)

                return False

            # ━━━ 8. 记录结果 ━━━
            self._store_add_record(
                theta=theta, features=features, j_total=j_total,
                j_parts=j_parts, is_valid=True, reason="",
                phase=phase, round_id=round_id,
            )

            self.failure_streak = 0
            j_best, _, _ = self._get_best()
            logger.info("[Round %d] ✅ 完成, J=%.6f (best=%.6f)",
                        round_id, j_total, j_best or j_total)
            return True

        except Exception as e:
            logger.exception("[Round %d] 执行异常: %s", round_id, e)
            self.failure_streak += 1
            try:
                self._store_mark_invalid(
                    theta=theta, reason=f"exception: {e}",
                    phase=phase, round_id=round_id,
                )
            except Exception:
                pass
            return False

    def _health_check(self) -> bool:
        """
        ★ 修正: 匹配新 JobExecutor 的 is_job_healthy() 方法
        """
        for method_name in ("is_job_healthy", "health_check",
                            "check_job_health", "is_healthy"):
            if hasattr(self.executor, method_name):
                try:
                    ret = getattr(self.executor, method_name)()
                    if isinstance(ret, tuple):
                        return bool(ret[0])
                    return bool(ret)
                except Exception as e:
                    logger.warning("%s 失败: %s", method_name, e)
                    return False

        # 兜底: 检查是否有运行中的作业
        if hasattr(self.executor, "get_current_job_id"):
            return self.executor.get_current_job_id() is not None

        return True

    # ================================================================
    # 轮次调度
    # ================================================================

    def _run_warmup_round(self, round_id: int) -> bool:
        """LHS 探索轮"""
        total = self._store_count_total()

        # 第 0 轮：用默认参数
        if round_id == 0 and total == 0:
            theta = self.space.get_defaults()
            return self._execute_round(round_id, theta, phase="warmup")

        # 后续轮：LHS 采样
        lhs_n = max(1, self.warm_up_rounds - 1)

        if self._lhs_cache is None or len(self._lhs_cache) != lhs_n:
            self._lhs_cache = self.space.latin_hypercube_sample(lhs_n, seed=42)

        idx = min(max(0, round_id - 1), len(self._lhs_cache) - 1)
        theta = self._lhs_cache[idx]

        return self._execute_round(round_id, theta, phase="warmup")

    def _run_optimize_round(self, round_id: int) -> bool:
        """BO 优化轮"""
        try:
            theta, info = self._suggest_theta()
            source = info.get("source", "")

            # ★ ET 拒绝: 跳过本轮，不重启作业
            if theta is None or source == "et_rejected":
                logger.info("[Round %d] BO 无改善建议，跳过本轮（不重启作业）",
                            round_id)
                # 记录一条跳过记录，但不算失败
                _, theta_best, _ = self._get_best()
                if theta_best:
                    self._store_add_record(
                        theta=theta_best,
                        features={"skipped": True},
                        j_total=self._get_j_current(),
                        j_parts={"reason": "et_rejected"},
                        is_valid=True,
                        reason="et_skip",
                        phase="skip",
                        round_id=round_id,
                    )
                return True  # 不算失败

            phase = "optimize"
            if source == "fallback":
                phase = "conservative"
                logger.info("[Round %d] 使用 fallback 参数", round_id)

            return self._execute_round(round_id, theta, phase=phase)

        except Exception as e:
            logger.exception("参数建议失败: %s", e)
            theta = self._fallback_theta()
            return self._execute_round(round_id, theta, phase="conservative")


    # ================================================================
    # 回滚
    # ================================================================

    def _rollback_to_best(self, reason: str = "") -> bool:
        """回滚到历史最优参数"""
        _, theta_best, _ = self._get_best()
        if theta_best is None:
            logger.warning("没有可回滚的 theta_best")
            return False

        logger.warning("回滚到 theta_best=%s, reason=%s", theta_best, reason)

        ok = self.executor.apply_parameters(theta_best)
        if ok:
            if hasattr(self.collector, "reset_cache"):
                self.collector.reset_cache()
            self._interruptible_sleep(self.warmup_seconds)
        return ok

    # ================================================================
    # 汇总与清理
    # ================================================================

    def _print_summary(self):
        records = getattr(self.store, "records", [])
        valid_records = [r for r in records
                         if getattr(r, "is_valid", getattr(r, "valid", False))]

        print("\n" + "=" * 50)
        print("ML Tuner 运行总结")
        print("=" * 50)
        print(f"总记录数:   {len(records)}")
        print(f"有效记录:   {len(valid_records)}")
        print(f"无效记录:   {len(records) - len(valid_records)}")

        if not valid_records:
            print("无有效观测。")
            return

        best = min(valid_records,
                   key=lambda r: float(getattr(r, "j_total", float("inf"))))
        best_j = float(getattr(best, "j_total", float("inf")))
        best_theta = getattr(best, "theta", {})
        best_phase = getattr(best, "phase", "")
        best_round = getattr(best, "round_id", -1)

        print(f"\n最优 J:     {best_j:.6f}")
        print(f"最优参数:   {best_theta}")
        if best_phase:
            print(f"所在阶段:   {best_phase}")
        if best_round >= 0:
            print(f"所在轮次:   {best_round}")

        # 打印收敛趋势
        print(f"\n{'─' * 50}")
        print("J 值变化趋势:")
        running_best = float("inf")
        for r in valid_records:
            j = float(getattr(r, "j_total", float("inf")))
            rid = getattr(r, "round_id", "?")
            ph = getattr(r, "phase", "?")
            running_best = min(running_best, j)
            marker = " ★ NEW BEST" if j <= running_best else ""
            print(f"  Round {rid:>3} [{ph:<12}]  J={j:.6f}  best={running_best:.6f}{marker}")

    def _cleanup(self):
        self._save_store()
        self._print_summary()
        self._running = False

        if hasattr(self.collector, "close"):
            try:
                self.collector.close()
            except Exception:
                pass
        if hasattr(self.executor, "close"):
            try:
                self.executor.close()
            except Exception:
                pass

    # ================================================================
    # 主循环
    # ================================================================

    def run(self, total_rounds: Optional[int] = None):
        self._running = True
        self.stop_requested = False

        if total_rounds is None:
            total_rounds = self.max_rounds

        start_round = self._store_count_total()

        logger.info("=" * 50)
        logger.info("ML Tuner 启动")
        logger.info("  起始轮次:   %d", start_round)
        logger.info("  总轮次:     %d", total_rounds)
        logger.info("  预热轮数:   %d", self.warm_up_rounds)
        logger.info("  预热等待:   %ds", self.warmup_seconds)
        logger.info("  采样次数:   %d 次/轮, 间隔 %ds",
                     self.sample_count, self.collection_interval_seconds)
        logger.info("=" * 50)

        try:
            for round_id in range(start_round, total_rounds):
                if self.stop_requested or not self._running:
                    logger.warning("停止标志已设置，退出主循环")
                    break

                # 选择阶段
                if round_id < self.warm_up_rounds:
                    ok = self._run_warmup_round(round_id)
                else:
                    ok = self._run_optimize_round(round_id)

                # 保存
                self._save_store()

                # 连续失败处理
                if not ok and self.failure_streak >= self.max_consecutive_failures:
                    logger.warning("连续失败 %d 次，尝试回滚",
                                   self.failure_streak)
                    try:
                        self._rollback_to_best(reason="consecutive_failures")
                    except Exception as e:
                        logger.warning("回滚失败: %s", e)
                    self.failure_streak = 0

                # 检查 executor 是否认为应该中止
                if hasattr(self.executor, "should_abort") and self.executor.should_abort():
                    logger.error("Executor 报告应中止")
                    break

        finally:
            self._cleanup()


def main():
    parser = argparse.ArgumentParser(description="ML Tuner Main Loop")
    parser.add_argument("--config", default="ml_tuner_config.yaml",
                        help="配置文件路径")
    parser.add_argument("--rounds", type=int, default=None,
                        help="总轮数覆盖")
    args = parser.parse_args()

    loop = MLTunerLoop(config_path=args.config)
    loop.run(total_rounds=args.rounds)


if __name__ == "__main__":
    main()
