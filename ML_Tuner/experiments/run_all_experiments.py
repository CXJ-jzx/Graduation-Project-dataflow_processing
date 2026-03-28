#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ML 参数调优验证实验套件

实验 1: 收敛性验证 — BO 是否随轮次收敛
实验 2: 对比基线   — ML调优 vs 默认参数 vs 随机搜索 vs 网格搜索
实验 3: 自适应验证 — 不同数据负载下参数是否自适应调整
实验 4: 稳定性复验 — 最优参数重复运行 N 次，验证结果一致
实验 5: 消融实验   — GP vs RF、EI vs Random、各目标权重贡献
"""

import os
import sys
import json
import time
import copy
import logging
import argparse
import csv
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import numpy as np

# 添加 ML_Tuner 路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from main import MLTunerLoop, load_yaml_config
from job_executor import JobExecutor
from observation_collector import ObservationCollector
from feature_engineer import FeatureEngineer
from parameter_space import ParameterSpace
from observation_store import ObservationStore

logger = logging.getLogger("ml_tuner.experiments")


# ================================================================
# 工具函数
# ================================================================

def setup_experiment_logging(exp_name: str, log_dir: str = "experiments/logs"):
    os.makedirs(log_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{exp_name}_{ts}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ],
        force=True
    )
    logger.info("实验日志: %s", log_file)
    return log_file


def save_results(results: List[Dict], output_path: str):
    """保存实验结果为 CSV"""
    if not results:
        return

    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)

    keys = list(results[0].keys())
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(results)

    logger.info("结果已保存: %s (%d 条)", output_path, len(results))


def evaluate_single_config(
    executor: JobExecutor,
    collector: ObservationCollector,
    fe: FeatureEngineer,
    theta: Dict[str, int],
    warmup_seconds: int = 90,
    sample_count: int = 10,
    sample_interval: int = 30,
    run_id: int = 0,
) -> Dict[str, Any]:
    """
    用给定参数跑一次完整的「提交→预热→采集→评估」流程。

    Returns:
        包含 theta、features、J、各分项的完整记录
    """
    record = {
        "run_id": run_id,
        "timestamp": datetime.now().isoformat(),
        "theta": json.dumps(theta),
    }

    # 1) 应用参数
    ok = executor.apply_parameters(theta)
    if not ok:
        record["status"] = "apply_failed"
        record["j_total"] = float("inf")
        return record

    # 2) 重置采集器
    collector.reset_cache()

    # 3) 预热
    logger.info("  预热 %ds...", warmup_seconds)
    time.sleep(warmup_seconds)

    # 4) 等待稳定
    collector.discover()
    collector.wait_until_stable(
        bp_threshold=0.50,
        stable_required=3,
        check_interval=15,
        timeout=180
    )

    # 5) 等待数据流
    data_ok = collector.wait_until_data_flowing(
        min_rate=10.0,
        stable_required=2,
        check_interval=15,
        timeout=180
    )
    if not data_ok:
        record["status"] = "no_data_flow"
        record["j_total"] = float("inf")
        return record

    # 6) 采集
    samples = collector.collect_window()
    if not samples:
        record["status"] = "empty_samples"
        record["j_total"] = float("inf")
        return record

    # 7) 聚合特征
    all_keys = set()
    for s in samples:
        all_keys.update(s.keys())

    features = {}
    for key in all_keys:
        values = [s[key] for s in samples
                  if key in s and isinstance(s[key], (int, float))
                  and not np.isnan(s[key])]
        features[key] = float(np.median(values)) if values else 0.0

    # 8) 计算目标函数
    result = fe.compute_objective(features)
    if isinstance(result, tuple):
        j_total, j_parts = float(result[0]), dict(result[1] or {})
    elif isinstance(result, dict):
        j_total = float(result.get("j_total", float("inf")))
        j_parts = dict(result.get("j_parts", {}))
    else:
        j_total, j_parts = float(result), {}

    # 9) 填充记录
    record["status"] = "success"
    record["j_total"] = j_total
    for k, v in j_parts.items():
        record[f"j_{k}"] = v

    # 关键特征
    for feat_key in [
        "avg_input_rate", "e2e_latency_p99_ms",
        "backpressure_ratio", "avg_busy_ratio", "max_busy_ratio",
        "checkpoint_avg_dur", "heap_usage_ratio",
        "l2_hit_rate", "l3_hit_rate", "total_parallelism",
        "avg_backlog"
    ]:
        record[feat_key] = features.get(feat_key, 0.0)

    return record


# ================================================================
# 实验 1: 收敛性验证
# ================================================================

class Experiment1_Convergence:
    """
    验证 BO 随轮次推进是否收敛（J 值递减趋势）

    方法:
    - 清空历史，从零开始跑 N 轮
    - 记录每轮 J、running_best
    - 绘制收敛曲线
    - 计算收敛速度指标
    """

    def __init__(self, config_path: str, total_rounds: int = 15):
        self.config_path = config_path
        self.total_rounds = total_rounds
        self.results = []

    def run(self) -> List[Dict]:
        setup_experiment_logging("exp1_convergence")
        logger.info("=" * 60)
        logger.info("实验 1: 收敛性验证 (共 %d 轮)", self.total_rounds)
        logger.info("=" * 60)

        config = load_yaml_config(self.config_path)

        # 使用临时 CSV，不污染主数据
        exp_csv = "experiments/data/exp1_observations.csv"
        os.makedirs(os.path.dirname(exp_csv), exist_ok=True)

        # 清除旧数据
        if os.path.exists(exp_csv):
            os.remove(exp_csv)

        config["storage"]["csv_path"] = exp_csv
        config["scheduler"]["max_rounds"] = self.total_rounds

        loop = MLTunerLoop(config_path=self.config_path, config=config)
        loop.csv_path = exp_csv
        loop.store = ObservationStore(
            csv_path=exp_csv, autosave=False, config_dict=config
        )

        # 跑调优
        loop.run(total_rounds=self.total_rounds)

        # 提取结果
        records = getattr(loop.store, "records", [])
        running_best = float("inf")

        for i, r in enumerate(records):
            j = float(getattr(r, "j_total", float("inf")))
            is_valid = getattr(r, "is_valid", getattr(r, "valid", False))
            theta = getattr(r, "theta", {})
            phase = getattr(r, "phase", "")
            running_best = min(running_best, j) if is_valid else running_best

            self.results.append({
                "round": i,
                "phase": phase,
                "theta": json.dumps(theta) if isinstance(theta, dict) else str(theta),
                "j_total": j,
                "is_valid": is_valid,
                "running_best": running_best,
                "improvement": (running_best - j) if is_valid else 0.0,
            })

        # 保存
        save_results(self.results, "experiments/results/exp1_convergence.csv")

        # 分析
        self._analyze()
        return self.results

    def _analyze(self):
        valid = [r for r in self.results if r["is_valid"]]
        if len(valid) < 3:
            logger.warning("有效样本不足 3 条，无法分析收敛性")
            return

        j_values = [r["j_total"] for r in valid]
        first_half = j_values[:len(j_values) // 2]
        second_half = j_values[len(j_values) // 2:]

        mean_first = np.mean(first_half)
        mean_second = np.mean(second_half)
        improvement_pct = (mean_first - mean_second) / mean_first * 100

        # 找到首次达到最终 best 90% 的轮次
        final_best = min(j_values)
        threshold_90 = j_values[0] - 0.9 * (j_values[0] - final_best)
        converge_round = len(valid)
        for i, j in enumerate(j_values):
            if j <= threshold_90:
                converge_round = i
                break

        logger.info("=" * 50)
        logger.info("实验 1 分析结果:")
        logger.info("  有效轮次: %d", len(valid))
        logger.info("  初始 J:   %.6f", j_values[0])
        logger.info("  最终 best: %.6f", final_best)
        logger.info("  前半均值: %.6f", mean_first)
        logger.info("  后半均值: %.6f", mean_second)
        logger.info("  改善幅度: %.2f%%", improvement_pct)
        logger.info("  收敛轮次 (90%%): %d", converge_round)
        logger.info("  结论: %s",
                     "✅ 收敛" if improvement_pct > 2.0 else "⚠️ 改善不明显")
        logger.info("=" * 50)


# ================================================================
# 实验 2: 对比基线
# ================================================================

class Experiment2_BaselineComparison:
    """
    ML调优 vs 默认参数 vs 随机搜索 vs 网格搜索

    方法:
    - 策略 A: 默认参数（固定跑 3 次取均值）
    - 策略 B: 随机搜索（随机 N 组参数）
    - 策略 C: 网格搜索（均匀网格 N 组）
    - 策略 D: ML 调优结果（从已有 observations.csv 读取）
    """

    def __init__(self, config_path: str, n_random: int = 5, n_grid: int = 5):
        self.config_path = config_path
        self.config = load_yaml_config(config_path)
        self.n_random = n_random
        self.n_grid = n_grid
        self.results = []

    def run(self) -> List[Dict]:
        setup_experiment_logging("exp2_baseline")
        logger.info("=" * 60)
        logger.info("实验 2: 对比基线")
        logger.info("=" * 60)

        executor = JobExecutor(config_dict=self.config)
        collector = ObservationCollector(config_dict=self.config)
        fe = FeatureEngineer(config_dict=self.config)
        space = ParameterSpace(config_dict=self.config)

        try:
            # ━━━ A: 默认参数 ━━━
            logger.info("策略 A: 默认参数 (3 次复验)")
            default_theta = space.get_defaults()
            for i in range(3):
                logger.info("  默认参数 run %d/3", i + 1)
                r = evaluate_single_config(
                    executor, collector, fe,
                    default_theta,
                    warmup_seconds=90,
                    run_id=i
                )
                r["strategy"] = "default"
                r["strategy_run"] = i
                self.results.append(r)

            # ━━━ B: 随机搜索 ━━━
            logger.info("策略 B: 随机搜索 (%d 组)", self.n_random)
            random_thetas = [space.random_sample() for _ in range(self.n_random)]
            for i, theta in enumerate(random_thetas):
                logger.info("  随机搜索 run %d/%d: %s",
                            i + 1, self.n_random, theta)
                r = evaluate_single_config(
                    executor, collector, fe,
                    theta,
                    warmup_seconds=90,
                    run_id=i
                )
                r["strategy"] = "random"
                r["strategy_run"] = i
                self.results.append(r)

            # ━━━ C: 网格搜索 ━━━
            logger.info("策略 C: 网格搜索 (%d 组)", self.n_grid)
            grid_thetas = self._generate_grid(space, self.n_grid)
            for i, theta in enumerate(grid_thetas):
                logger.info("  网格搜索 run %d/%d: %s",
                            i + 1, len(grid_thetas), theta)
                r = evaluate_single_config(
                    executor, collector, fe,
                    theta,
                    warmup_seconds=90,
                    run_id=i
                )
                r["strategy"] = "grid"
                r["strategy_run"] = i
                self.results.append(r)

            # ━━━ D: ML 调优最优 (复验 3 次) ━━━
            logger.info("策略 D: ML 调优最优参数 (3 次复验)")
            ml_best_theta = {
                "checkpoint_interval": 25000,
                "buffer_timeout": 10,
                "watermark_delay": 26000,
            }
            for i in range(3):
                logger.info("  ML 最优 run %d/3", i + 1)
                r = evaluate_single_config(
                    executor, collector, fe,
                    ml_best_theta,
                    warmup_seconds=90,
                    run_id=i
                )
                r["strategy"] = "ml_tuner"
                r["strategy_run"] = i
                self.results.append(r)

        finally:
            executor.close()
            collector.close()

        save_results(self.results, "experiments/results/exp2_baseline.csv")
        self._analyze()
        return self.results

    def _generate_grid(self, space: ParameterSpace, n: int) -> List[Dict]:
        """在参数空间均匀取 n 个点"""
        grid = []
        dims = space.get_param_names()
        ranges = {}
        for d in dims:
            info = space.get_param_info(d)
            lo, hi, step = info["min"], info["max"], info["step"]
            levels = list(range(int(lo), int(hi) + 1, int(step)))
            # 均匀取 ceil(n^(1/3)) 个
            k = max(2, int(np.ceil(n ** (1.0 / len(dims)))))
            indices = np.linspace(0, len(levels) - 1, k, dtype=int)
            ranges[d] = [levels[i] for i in indices]

        # 笛卡尔积
        from itertools import product
        all_combos = list(product(*[ranges[d] for d in dims]))
        np.random.seed(42)
        if len(all_combos) > n:
            indices = np.random.choice(len(all_combos), n, replace=False)
            all_combos = [all_combos[i] for i in indices]

        for combo in all_combos:
            theta = {dims[i]: int(combo[i]) for i in range(len(dims))}
            grid.append(theta)

        return grid

    def _analyze(self):
        strategies = {}
        for r in self.results:
            s = r.get("strategy", "unknown")
            if s not in strategies:
                strategies[s] = []
            if r.get("status") == "success":
                strategies[s].append(r["j_total"])

        logger.info("=" * 60)
        logger.info("实验 2 分析结果:")
        logger.info("%-15s %-10s %-10s %-10s %-10s", "策略", "次数", "均值", "最优", "标准差")
        logger.info("-" * 60)

        for s in ["default", "random", "grid", "ml_tuner"]:
            vals = strategies.get(s, [])
            if vals:
                logger.info("%-15s %-10d %-10.6f %-10.6f %-10.6f",
                            s, len(vals), np.mean(vals), min(vals), np.std(vals))
            else:
                logger.info("%-15s %-10s", s, "无有效结果")

        # ML vs Default 改善
        ml_vals = strategies.get("ml_tuner", [])
        default_vals = strategies.get("default", [])
        if ml_vals and default_vals:
            improvement = (np.mean(default_vals) - np.mean(ml_vals)) / np.mean(default_vals) * 100
            logger.info("\nML调优 vs 默认: 改善 %.2f%%", improvement)
            logger.info("结论: %s",
                         "✅ ML调优显著优于默认" if improvement > 5.0
                         else "⚠️ 改善不显著" if improvement > 0
                         else "❌ ML调优不如默认")

        logger.info("=" * 60)


# ================================================================
# 实验 3: 自适应性验证
# ================================================================

class Experiment3_Adaptivity:
    """
    验证：不同数据负载下，ML 调优是否能自适应调整参数

    方法:
    - 场景 1: 低负载 (Producer 发送速率 200 records/s)
    - 场景 2: 中负载 (Producer 发送速率 800 records/s)  ← 当前
    - 场景 3: 高负载 (Producer 发送速率 2000 records/s)

    对每个场景：
    - 用 ML 调优跑 10 轮
    - 记录最优参数
    - 验证不同场景的最优参数是否不同（自适应）
    """

    def __init__(self, config_path: str, rounds_per_scenario: int = 10):
        self.config_path = config_path
        self.rounds_per_scenario = rounds_per_scenario
        self.results = []

    def run(self) -> List[Dict]:
        setup_experiment_logging("exp3_adaptivity")
        logger.info("=" * 60)
        logger.info("实验 3: 自适应性验证")
        logger.info("=" * 60)

        scenarios = [
            {"name": "low_load",  "rate": 200,  "description": "低负载 (~200 records/s)"},
            {"name": "mid_load",  "rate": 800,  "description": "中负载 (~800 records/s)"},
            {"name": "high_load", "rate": 2000, "description": "高负载 (~2000 records/s)"},
        ]

        for scenario in scenarios:
            logger.info("场景: %s", scenario["description"])
            logger.info("  ⚠️ 请手动调整 Producer 发送速率至 ~%d records/s",
                        scenario["rate"])
            logger.info("  ⚠️ 调整后按 Enter 继续...")

            # 在自动化模式下跳过等待
            try:
                input(f"  [等待确认: 已将 Producer 速率调至 ~{scenario['rate']} r/s] > ")
            except EOFError:
                logger.info("  自动模式，跳过手动确认")

            config = load_yaml_config(self.config_path)

            exp_csv = f"experiments/data/exp3_{scenario['name']}.csv"
            os.makedirs(os.path.dirname(exp_csv), exist_ok=True)
            if os.path.exists(exp_csv):
                os.remove(exp_csv)

            config["storage"]["csv_path"] = exp_csv
            config["scheduler"]["max_rounds"] = self.rounds_per_scenario
            config["scheduler"]["warm_up_rounds"] = min(5, self.rounds_per_scenario)

            loop = MLTunerLoop(config_path=self.config_path, config=config)
            loop.csv_path = exp_csv
            loop.store = ObservationStore(csv_path=exp_csv, autosave=False, config_dict=config)
            loop.run(total_rounds=self.rounds_per_scenario)

            # 提取最优
            records = getattr(loop.store, "records", [])
            valid = [r for r in records
                     if getattr(r, "is_valid", getattr(r, "valid", False))]

            if valid:
                best = min(valid, key=lambda r: float(getattr(r, "j_total", float("inf"))))
                best_theta = getattr(best, "theta", {})
                best_j = float(getattr(best, "j_total", float("inf")))
            else:
                best_theta = {}
                best_j = float("inf")

            self.results.append({
                "scenario": scenario["name"],
                "description": scenario["description"],
                "target_rate": scenario["rate"],
                "total_rounds": len(records),
                "valid_rounds": len(valid),
                "best_j": best_j,
                "best_theta": json.dumps(best_theta),
                "best_checkpoint_interval": best_theta.get("checkpoint_interval", "N/A"),
                "best_buffer_timeout": best_theta.get("buffer_timeout", "N/A"),
                "best_watermark_delay": best_theta.get("watermark_delay", "N/A"),
            })

        save_results(self.results, "experiments/results/exp3_adaptivity.csv")
        self._analyze()
        return self.results

    def _analyze(self):
        logger.info("=" * 60)
        logger.info("实验 3 分析结果:")
        logger.info("%-12s %-10s %-12s %-12s %-12s %-10s",
                     "场景", "目标速率", "ckpt_intv", "buf_timeout", "wm_delay", "J")
        logger.info("-" * 70)

        thetas = []
        for r in self.results:
            theta = json.loads(r["best_theta"]) if isinstance(r["best_theta"], str) else r["best_theta"]
            thetas.append(theta)
            logger.info("%-12s %-10d %-12s %-12s %-12s %-10.6f",
                         r["scenario"], r["target_rate"],
                         theta.get("checkpoint_interval", "N/A"),
                         theta.get("buffer_timeout", "N/A"),
                         theta.get("watermark_delay", "N/A"),
                         r["best_j"])

        # 检查不同场景参数是否不同
        if len(thetas) >= 2:
            params_differ = False
            for key in ["checkpoint_interval", "buffer_timeout", "watermark_delay"]:
                vals = [t.get(key, 0) for t in thetas if key in t]
                if len(set(vals)) > 1:
                    params_differ = True
                    break

            logger.info("\n自适应判定: %s",
                         "✅ 不同负载下参数不同（自适应生效）" if params_differ
                         else "⚠️ 参数相同（可能负载差异不够大）")

        logger.info("=" * 60)


# ================================================================
# 实验 4: 稳定性复验
# ================================================================

class Experiment4_Stability:
    """
    最优参数重复运行 N 次，验证 J 值的一致性

    通过标准:
    - 变异系数 CV < 10%
    - 最大偏差 < 20%
    """

    def __init__(self, config_path: str,
                 theta: Dict[str, int] = None,
                 n_repeats: int = 5):
        self.config_path = config_path
        self.config = load_yaml_config(config_path)
        self.theta = theta or {
            "checkpoint_interval": 25000,
            "buffer_timeout": 10,
            "watermark_delay": 26000,
        }
        self.n_repeats = n_repeats
        self.results = []

    def run(self) -> List[Dict]:
        setup_experiment_logging("exp4_stability")
        logger.info("=" * 60)
        logger.info("实验 4: 稳定性复验 (%d 次)", self.n_repeats)
        logger.info("  参数: %s", self.theta)
        logger.info("=" * 60)

        executor = JobExecutor(config_dict=self.config)
        collector = ObservationCollector(config_dict=self.config)
        fe = FeatureEngineer(config_dict=self.config)

        try:
            for i in range(self.n_repeats):
                logger.info("复验 %d/%d", i + 1, self.n_repeats)
                r = evaluate_single_config(
                    executor, collector, fe,
                    self.theta,
                    warmup_seconds=90,
                    sample_count=10,
                    sample_interval=30,
                    run_id=i
                )
                r["repeat"] = i
                self.results.append(r)

                if r.get("status") == "success":
                    logger.info("  J=%.6f, latency=%.1fms, throughput=%.0f",
                                r["j_total"],
                                r.get("e2e_latency_p99_ms", 0),
                                r.get("avg_input_rate", 0))
                else:
                    logger.warning("  失败: %s", r.get("status"))

        finally:
            executor.close()
            collector.close()

        save_results(self.results, "experiments/results/exp4_stability.csv")
        self._analyze()
        return self.results

    def _analyze(self):
        valid = [r for r in self.results if r.get("status") == "success"]

        logger.info("=" * 60)
        logger.info("实验 4 分析结果:")

        if len(valid) < 2:
            logger.warning("有效样本不足 2 条，无法分析")
            return

        j_vals = [r["j_total"] for r in valid]
        lat_vals = [r.get("e2e_latency_p99_ms", 0) for r in valid]
        tput_vals = [r.get("avg_input_rate", 0) for r in valid]

        mean_j = np.mean(j_vals)
        std_j = np.std(j_vals)
        cv_j = std_j / mean_j * 100 if mean_j > 0 else 0
        max_dev = (max(j_vals) - min(j_vals)) / mean_j * 100 if mean_j > 0 else 0

        logger.info("  复验次数:     %d / %d 成功", len(valid), self.n_repeats)
        logger.info("  J 均值:       %.6f", mean_j)
        logger.info("  J 标准差:     %.6f", std_j)
        logger.info("  J 变异系数:   %.2f%%", cv_j)
        logger.info("  J 最大偏差:   %.2f%%", max_dev)
        logger.info("  延迟均值:     %.1f ms", np.mean(lat_vals))
        logger.info("  吞吐量均值:   %.0f records/s", np.mean(tput_vals))
        logger.info("")
        logger.info("  CV < 10%%:     %s", "✅ 通过" if cv_j < 10 else "❌ 未通过")
        logger.info("  最大偏差 < 20%%: %s", "✅ 通过" if max_dev < 20 else "❌ 未通过")
        logger.info("  结论:         %s",
                     "✅ 参数稳定可靠" if cv_j < 10 and max_dev < 20
                     else "⚠️ 存在波动，需进一步排查")
        logger.info("=" * 60)


# ================================================================
# 实验 5: 消融实验
# ================================================================

class Experiment5_Ablation:
    """
    验证各组件的贡献:
    - A: 完整 BO (GP + EI)        → 基准
    - B: 去掉 GP，纯随机搜索      → 验证 GP 贡献
    - C: 去掉 EI，用 UCB          → 验证采集函数贡献
    - D: 改变目标权重              → 验证权重敏感性
    """

    def __init__(self, config_path: str, rounds_per_variant: int = 10):
        self.config_path = config_path
        self.rounds = rounds_per_variant
        self.results = []

    def run(self) -> List[Dict]:
        setup_experiment_logging("exp5_ablation")
        logger.info("=" * 60)
        logger.info("实验 5: 消融实验")
        logger.info("=" * 60)

        variants = self._define_variants()

        for variant in variants:
            logger.info("变体: %s — %s", variant["name"], variant["description"])

            config = load_yaml_config(self.config_path)

            # 应用变体修改
            for path, value in variant.get("overrides", {}).items():
                keys = path.split(".")
                cfg = config
                for k in keys[:-1]:
                    cfg = cfg.setdefault(k, {})
                cfg[keys[-1]] = value

            exp_csv = f"experiments/data/exp5_{variant['name']}.csv"
            os.makedirs(os.path.dirname(exp_csv), exist_ok=True)
            if os.path.exists(exp_csv):
                os.remove(exp_csv)

            config["storage"]["csv_path"] = exp_csv
            config["scheduler"]["max_rounds"] = self.rounds
            config["scheduler"]["warm_up_rounds"] = min(5, self.rounds)

            loop = MLTunerLoop(config_path=self.config_path, config=config)
            loop.csv_path = exp_csv
            loop.store = ObservationStore(csv_path=exp_csv, autosave=False, config_dict=config)

            loop.run(total_rounds=self.rounds)

            records = getattr(loop.store, "records", [])
            valid = [r for r in records
                     if getattr(r, "is_valid", getattr(r, "valid", False))]

            j_vals = [float(getattr(r, "j_total", float("inf"))) for r in valid]

            self.results.append({
                "variant": variant["name"],
                "description": variant["description"],
                "total_rounds": len(records),
                "valid_rounds": len(valid),
                "best_j": min(j_vals) if j_vals else float("inf"),
                "mean_j": float(np.mean(j_vals)) if j_vals else float("inf"),
                "std_j": float(np.std(j_vals)) if len(j_vals) >= 2 else 0.0,
            })

        save_results(self.results, "experiments/results/exp5_ablation.csv")
        self._analyze()
        return self.results

    def _define_variants(self) -> List[Dict]:
        return [
            {
                "name": "full_bo",
                "description": "完整 BO (GP + EI) — 基准",
                "overrides": {}
            },
            {
                "name": "no_gp_random_only",
                "description": "去掉 GP，纯随机搜索",
                "overrides": {
                    "surrogate.warm_up_observations": 9999,
                }
            },
            {
                "name": "latency_heavy",
                "description": "延迟权重提高到 0.7",
                "overrides": {
                    "objective.weights.latency": 0.70,
                    "objective.weights.throughput": 0.15,
                    "objective.weights.checkpoint": 0.10,
                    "objective.weights.resource": 0.05,
                }
            },
            {
                "name": "throughput_heavy",
                "description": "吞吐量权重提高到 0.7",
                "overrides": {
                    "objective.weights.latency": 0.10,
                    "objective.weights.throughput": 0.70,
                    "objective.weights.checkpoint": 0.10,
                    "objective.weights.resource": 0.10,
                }
            },
        ]

    def _analyze(self):
        logger.info("=" * 60)
        logger.info("实验 5 分析结果:")
        logger.info("%-25s %-10s %-10s %-10s %-10s",
                     "变体", "有效轮数", "best_J", "mean_J", "std_J")
        logger.info("-" * 65)

        baseline_best = None
        for r in self.results:
            if r["variant"] == "full_bo":
                baseline_best = r["best_j"]
            logger.info("%-25s %-10d %-10.6f %-10.6f %-10.6f",
                         r["variant"], r["valid_rounds"],
                         r["best_j"], r["mean_j"], r["std_j"])

        if baseline_best and baseline_best < float("inf"):
            logger.info("")
            for r in self.results:
                if r["variant"] != "full_bo" and r["best_j"] < float("inf"):
                    diff = (r["best_j"] - baseline_best) / baseline_best * 100
                    logger.info("  %s vs 基准: %+.2f%%", r["variant"], diff)

        logger.info("=" * 60)


# ================================================================
# 主入口
# ================================================================

def main():
    parser = argparse.ArgumentParser(description="ML 参数调优验证实验")
    parser.add_argument("--config", default="ml_tuner_config.yaml", help="配置文件")
    parser.add_argument("--exp", type=int, default=0,
                        help="运行哪个实验 (1-5, 0=全部)")
    parser.add_argument("--rounds", type=int, default=12, help="每个实验的轮数")
    parser.add_argument("--repeats", type=int, default=5, help="稳定性复验次数")
    args = parser.parse_args()

    os.makedirs("experiments/results", exist_ok=True)
    os.makedirs("experiments/data", exist_ok=True)
    os.makedirs("experiments/logs", exist_ok=True)

    if args.exp == 0 or args.exp == 1:
        print("\n" + "▓" * 60)
        print("  实验 1: 收敛性验证")
        print("▓" * 60)
        exp1 = Experiment1_Convergence(args.config, total_rounds=args.rounds)
        exp1.run()

    if args.exp == 0 or args.exp == 2:
        print("\n" + "▓" * 60)
        print("  实验 2: 对比基线")
        print("▓" * 60)
        exp2 = Experiment2_BaselineComparison(args.config, n_random=5, n_grid=5)
        exp2.run()

    if args.exp == 0 or args.exp == 3:
        print("\n" + "▓" * 60)
        print("  实验 3: 自适应性验证")
        print("▓" * 60)
        exp3 = Experiment3_Adaptivity(args.config, rounds_per_scenario=args.rounds)
        exp3.run()

    if args.exp == 0 or args.exp == 4:
        print("\n" + "▓" * 60)
        print("  实验 4: 稳定性复验")
        print("▓" * 60)
        exp4 = Experiment4_Stability(
            args.config,
            theta={
                "checkpoint_interval": 25000,
                "buffer_timeout": 10,
                "watermark_delay": 26000,
            },
            n_repeats=args.repeats
        )
        exp4.run()

    if args.exp == 0 or args.exp == 5:
        print("\n" + "▓" * 60)
        print("  实验 5: 消融实验")
        print("▓" * 60)
        exp5 = Experiment5_Ablation(args.config, rounds_per_variant=args.rounds)
        exp5.run()

    print("\n" + "=" * 60)
    print("所有实验完成!")
    print("结果目录: experiments/results/")
    print("日志目录: experiments/logs/")
    print("=" * 60)


if __name__ == "__main__":
    main()
