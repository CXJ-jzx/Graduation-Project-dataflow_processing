#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
    lv = getattr(logging, str(level).upper(), logging.INFO)
    logging.basicConfig(
        level=lv,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        force=True
    )


class MLTunerLoop:
    def __init__(self, config_path: str = "ml_tuner_config.yaml",
                 config: Optional[Dict[str, Any]] = None):
        self.config_path = config_path
        self.config = config or load_yaml_config(config_path)

        setup_logging(self._cfg_first([("logging", "level")], "INFO"))

        self.stop_requested = False
        self._running = True
        self.failure_streak = 0
        self._lhs_cache = None

        self.warm_up_rounds = int(self._cfg_first(
            [("scheduler", "warm_up_rounds"), ("surrogate", "warm_up_observations")], 8))
        self.max_rounds = int(self._cfg_first(
            [("scheduler", "max_rounds"), ("tuner", "max_rounds")], 20))
        self.max_consecutive_failures = int(self._cfg_first(
            [("scheduler", "max_consecutive_failures")], 3))
        self.rollback_on_constraint_violation = bool(self._cfg_first(
            [("scheduler", "rollback_on_constraint_violation")], False))
        self.warmup_seconds = int(self._cfg_first(
            [("scheduler", "warm_up_after_apply_seconds")], 90))
        self.sample_count = int(self._cfg_first(
            [("metrics", "collection", "sample_count")], 10))
        self.collection_interval_seconds = int(self._cfg_first(
            [("metrics", "collection", "interval_seconds")], 30))

        self.csv_path = self._cfg_first(
            [("storage", "csv_path")],
            "ml_tuner_observations.csv"
        )

        self.space = ParameterSpace(config_dict=self.config)
        self.fe = FeatureEngineer(config_dict=self.config)
        self.store = ObservationStore(csv_path=self.csv_path, autosave=False, config_dict=self.config)
        self.optimizer = BayesianOptimizer(config_dict=self.config)
        self.executor = JobExecutor(config_dict=self.config)
        self.collector = ObservationCollector(config_dict=self.config)

        if hasattr(self.store, "load_csv") and self.csv_path and os.path.exists(self.csv_path):
            try:
                self.store.load_csv(self.csv_path)
                logger.info("已加载历史观测: %d 条", self._store_count_total())
            except Exception as e:
                logger.warning("加载历史观测失败: %s", e)

        self._restore_feature_engineer_baseline()

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _cfg_first(self, paths, default=None):
        for path in paths:
            v = nested_get(self.config, path, default=None)
            if v is not None:
                return v
        return default

    def _signal_handler(self, signum, frame):
        logger.warning("收到退出信号 %s，准备安全停止...", signum)
        self.stop_requested = True
        self._running = False

    def _interruptible_sleep(self, seconds: float):
        end = time.time() + max(0.0, float(seconds))
        while time.time() < end:
            if self.stop_requested or not self._running:
                break
            time.sleep(min(2.0, end - time.time()))

    def _store_count_total(self) -> int:
        if hasattr(self.store, "count_total"):
            return int(self.store.count_total())
        return len(getattr(self.store, "records", []))

    def _store_count_valid(self) -> int:
        if hasattr(self.store, "count_valid"):
            return int(self.store.count_valid())
        records = getattr(self.store, "records", [])
        return sum(1 for r in records if getattr(r, "is_valid", getattr(r, "valid", False)))

    def _get_best(self):
        if hasattr(self.store, "get_best"):
            ret = self.store.get_best()
            if isinstance(ret, tuple):
                if len(ret) >= 3:
                    return ret[0], ret[1], ret[2]
                if len(ret) == 2:
                    return ret[0], ret[1], None
        return None, None, None

    def _get_j_current(self) -> float:
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

    def _store_add_record(self, theta, features, j_total,
                          j_parts=None, is_valid=True,
                          reason="", phase="", round_id=-1):
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
        for p in payloads:
            try:
                return self.store.add(**p)
            except TypeError:
                continue
        raise RuntimeError("store.add 签名不匹配")

    def _store_mark_invalid(self, theta, features=None,
                            j_total=float("inf"), j_parts=None,
                            reason="", phase="", round_id=-1):
        if hasattr(self.store, "mark_invalid"):
            payloads = [
                dict(theta=theta, features=features or {},
                     j_total=j_total, j_parts=j_parts or {},
                     reason=reason, phase=phase, round_id=round_id),
                dict(theta=theta, features=features or {}, j_total=j_total, reason=reason),
                dict(theta=theta, reason=reason),
            ]
            for p in payloads:
                try:
                    return self.store.mark_invalid(**p)
                except TypeError:
                    continue

        return self._store_add_record(
            theta=theta, features=features or {},
            j_total=j_total, j_parts=j_parts or {},
            is_valid=False, reason=reason,
            phase=phase, round_id=round_id
        )

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
                setattr(self.fe, attr, baseline)
                logger.info("已恢复吞吐量 baseline=%.4f", baseline)
                break

    def _collect_features(self) -> Dict[str, float]:
        samples: List[Dict] = self.collector.collect_window()
        if not samples:
            return {}
        if len(samples) == 1:
            return samples[0]

        for method_name in ("aggregate_median", "aggregate_window",
                            "aggregate_samples", "extract_features"):
            if hasattr(self.fe, method_name):
                try:
                    result = getattr(self.fe, method_name)(samples)
                    if isinstance(result, dict) and result:
                        return result
                except Exception:
                    pass

        all_keys = set()
        for s in samples:
            all_keys.update(s.keys())

        aggregated = {}
        for key in all_keys:
            values = [s[key] for s in samples
                      if key in s and isinstance(s[key], (int, float))
                      and not np.isnan(s[key])]
            aggregated[key] = float(np.median(values)) if values else 0.0
        return aggregated

    def _validate_latency(self, features: Dict[str, float], round_id: int) -> Dict[str, float]:
        latency = features.get('e2e_latency_p99_ms', features.get('latency_p99', 0.0))
        throughput = features.get('avg_input_rate', 0.0)

        if latency > 0 or throughput <= 0:
            return features

        logger.warning("[Round %d] 延迟=0 但吞吐量=%.1f，延迟重采 60s 后进行", round_id, throughput)
        self._interruptible_sleep(60)
        if self.stop_requested:
            return features

        retry = []
        for _ in range(3):
            s = self.collector.collect_once()
            if s:
                retry.append(s)
            time.sleep(10)

        if retry:
            vals = [s.get('e2e_latency_p99_ms', s.get('latency_p99', 0.0)) for s in retry]
            m = max(vals)
            if m > 0:
                features['e2e_latency_p99_ms'] = float(np.median(vals))
                features['latency_p99'] = features['e2e_latency_p99_ms']
                return features

        avg_busy = features.get('avg_busy_ratio', 0.0)
        if avg_busy > 0:
            est = max(avg_busy * 1000.0 * 5, 10.0)
            features['e2e_latency_p99_ms'] = est
            features['latency_p99'] = est
        else:
            features['e2e_latency_p99_ms'] = 500.0
            features['latency_p99'] = 500.0
        return features

    def _compute_objective(self, features: Dict[str, float]) -> Tuple[float, Dict]:
        result = self.fe.compute_objective(features)
        if isinstance(result, tuple):
            if len(result) >= 2:
                return float(result[0]), dict(result[1] or {})
            return float(result[0]), {}
        if isinstance(result, dict) and "j_total" in result:
            return float(result["j_total"]), dict(result.get("j_parts", {}))
        return float(result), {}

    def _check_constraints(self, features: Dict[str, float]) -> Tuple[bool, str]:
        result = self.fe.check_constraints(features)
        if isinstance(result, tuple):
            if len(result) >= 2:
                valid = bool(result[0])
                reason_obj = result[1]
                if isinstance(reason_obj, (list, tuple)):
                    reason = "; ".join(str(x) for x in reason_obj)
                else:
                    reason = str(reason_obj)
                return valid, reason
            return bool(result[0]), ""
        if isinstance(result, dict):
            return bool(result.get("is_valid", result.get("valid", True))), str(result.get("reason", ""))
        return bool(result), ""

    def _materialize_theta(self, candidate) -> Optional[Dict]:
        if candidate is None:
            return None
        if isinstance(candidate, dict):
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
        _, theta_best, _ = self._get_best()
        if theta_best is not None:
            return dict(theta_best)
        if hasattr(self.space, "get_defaults"):
            return self.space.get_defaults()
        return {"checkpoint_interval": 30000, "buffer_timeout": 100, "watermark_delay": 5000}

    def _suggest_theta(self) -> Tuple[Optional[Dict], Dict]:
        j_current = self._get_j_current()
        try:
            ret = self.optimizer.suggest(self.store, j_current)
        except TypeError:
            try:
                ret = self.optimizer.suggest(self.store)
            except Exception:
                return self._fallback_theta(), {"source": "fallback"}

        if ret is None:
            return None, {"source": "et_rejected"}

        info = {}
        theta = None
        if isinstance(ret, dict):
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
            return self._fallback_theta(), {"source": "fallback"}
        return theta, info

    def _health_check(self) -> bool:
        if not self.executor.is_job_healthy():
            return False
        if hasattr(self.executor, "is_producer_healthy"):
            if not self.executor.is_producer_healthy():
                logger.warning("Producer 健康检查失败")
                return False
        return True

    def _wait_for_data_flow(self, round_id: int, theta: Dict, phase: str) -> bool:
        logger.info("[Round %d] 检查数据流就绪...", round_id)

        ok = self.collector.wait_until_data_flowing(
            min_rate=10.0,
            stable_required=2,
            check_interval=15,
            timeout=180,
            stop_flag=lambda: self.stop_requested,
        )
        if ok:
            return True

        if self.stop_requested:
            return False

        # 额外诊断：Producer 健康
        if hasattr(self.executor, "is_producer_healthy"):
            p_ok = self.executor.is_producer_healthy()
            logger.error("[Round %d] 数据流未就绪, producer_healthy=%s", round_id, p_ok)
        else:
            logger.error("[Round %d] 数据流未就绪", round_id)

        self.failure_streak += 1
        self._store_mark_invalid(
            theta=theta,
            reason="data_flow_not_ready",
            phase=phase, round_id=round_id
        )
        return False

    def _execute_round(self, round_id: int, theta: Dict[str, Any], phase: str = "optimize") -> bool:
        if self.stop_requested or not self._running:
            return False

        logger.info("=" * 50)
        logger.info("Round %d [%s] 开始, theta=%s", round_id, phase, theta)
        logger.info("=" * 50)

        try:
            logger.info("[Round %d] 应用参数到 Flink...", round_id)
            ok = self.executor.apply_parameters(theta)
            if not ok:
                self.failure_streak += 1
                self._store_mark_invalid(theta=theta, reason="apply_parameters_failed",
                                         phase=phase, round_id=round_id)
                return False

            self.collector.reset_cache()

            logger.info("[Round %d] 预热等待 %ds...", round_id, self.warmup_seconds)
            self._interruptible_sleep(self.warmup_seconds)
            if self.stop_requested:
                return False

            logger.info("[Round %d] 等待背压排空...", round_id)
            self.collector.wait_until_stable(
                bp_threshold=0.50, stable_required=3,
                check_interval=15, timeout=300,
                stop_flag=lambda: self.stop_requested
            )

            if self.stop_requested:
                return False

            if not self._wait_for_data_flow(round_id, theta, phase):
                return False

            if not self._health_check():
                self.failure_streak += 1
                self._store_mark_invalid(theta=theta, reason="health_check_failed",
                                         phase=phase, round_id=round_id)
                return False

            logger.info("[Round %d] 开始采集指标 (%d 次, 间隔 %ds)...",
                        round_id, self.sample_count, self.collection_interval_seconds)
            features = self._collect_features()
            if not features:
                self.failure_streak += 1
                self._store_mark_invalid(theta=theta, reason="empty_features",
                                         phase=phase, round_id=round_id)
                return False

            if features.get('avg_input_rate', 0.0) <= 0:
                self.failure_streak += 1
                self._store_mark_invalid(theta=theta, features=features,
                                         j_total=float("inf"),
                                         reason="zero_throughput_after_collection",
                                         phase=phase, round_id=round_id)
                return False

            features = self._validate_latency(features, round_id)
            j_total, j_parts = self._compute_objective(features)
            is_valid, reason = self._check_constraints(features)

            if not is_valid:
                self._store_mark_invalid(theta=theta, features=features,
                                         j_total=j_total, j_parts=j_parts,
                                         reason=reason, phase=phase, round_id=round_id)
                self.failure_streak += 1
                if self.rollback_on_constraint_violation:
                    self._rollback_to_best(reason=reason)
                return False

            self._store_add_record(theta=theta, features=features,
                                   j_total=j_total, j_parts=j_parts,
                                   is_valid=True, reason="",
                                   phase=phase, round_id=round_id)

            self.failure_streak = 0
            j_best, _, _ = self._get_best()
            logger.info("[Round %d] ✅ 完成, J=%.6f (best=%.6f)",
                        round_id, j_total, j_best or j_total)
            return True

        except Exception as e:
            logger.exception("[Round %d] 执行异常: %s", round_id, e)
            self.failure_streak += 1
            self._store_mark_invalid(theta=theta, reason=f"exception: {e}",
                                     phase=phase, round_id=round_id)
            return False

    def _run_warmup_round(self, round_id: int) -> bool:
        total = self._store_count_total()

        if round_id == 0 and total == 0:
            theta = self.space.get_defaults()
            return self._execute_round(round_id, theta, phase="warmup")

        lhs_n = max(1, self.warm_up_rounds - 1)
        if self._lhs_cache is None or len(self._lhs_cache) != lhs_n:
            self._lhs_cache = self.space.latin_hypercube_sample(lhs_n, seed=42)

        idx = min(max(0, round_id - 1), len(self._lhs_cache) - 1)
        theta = self._lhs_cache[idx]
        return self._execute_round(round_id, theta, phase="warmup")

    def _run_optimize_round(self, round_id: int) -> bool:
        theta, info = self._suggest_theta()
        source = info.get("source", "")

        if theta is None or source == "et_rejected":
            logger.info("[Round %d] BO 无改善建议，跳过本轮（不重启作业）", round_id)
            return True

        phase = "optimize" if source != "fallback" else "conservative"
        return self._execute_round(round_id, theta, phase=phase)

    def _rollback_to_best(self, reason: str = "") -> bool:
        _, theta_best, _ = self._get_best()
        if theta_best is None:
            return False
        logger.warning("回滚到 theta_best=%s, reason=%s", theta_best, reason)
        ok = self.executor.apply_parameters(theta_best)
        if ok:
            self.collector.reset_cache()
            self._interruptible_sleep(self.warmup_seconds)
        return ok

    def _print_summary(self):
        records = getattr(self.store, "records", [])
        valid_records = [r for r in records if getattr(r, "is_valid", getattr(r, "valid", False))]

        print("\n" + "=" * 50)
        print("ML Tuner 运行总结")
        print("=" * 50)
        print(f"总记录数:   {len(records)}")
        print(f"有效记录:   {len(valid_records)}")
        print(f"无效记录:   {len(records) - len(valid_records)}")

        if not valid_records:
            print("无有效观测。")
            return

        best = min(valid_records, key=lambda r: float(getattr(r, "j_total", float("inf"))))
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

    def _cleanup(self):
        self._save_store()
        self._print_summary()
        self._running = False

        try:
            self.collector.close()
        except Exception:
            pass
        try:
            self.executor.close()
        except Exception:
            pass

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

                if round_id < self.warm_up_rounds:
                    ok = self._run_warmup_round(round_id)
                else:
                    ok = self._run_optimize_round(round_id)

                self._save_store()

                if (not ok and self.failure_streak >= self.max_consecutive_failures):
                    logger.warning("连续失败 %d 次，尝试回滚", self.failure_streak)
                    try:
                        self._rollback_to_best(reason="consecutive_failures")
                    except Exception as e:
                        logger.warning("回滚失败: %s", e)
                    self.failure_streak = 0

                if hasattr(self.executor, "should_abort") and self.executor.should_abort():
                    logger.error("Executor 报告应中止")
                    break
        finally:
            self._cleanup()


def main():
    parser = argparse.ArgumentParser(description="ML Tuner Main Loop")
    parser.add_argument("--config", default="ml_tuner_config.yaml", help="配置文件路径")
    parser.add_argument("--rounds", type=int, default=None, help="总轮数覆盖")
    args = parser.parse_args()

    loop = MLTunerLoop(config_path=args.config)
    loop.run(total_rounds=args.rounds)


if __name__ == "__main__":
    main()
