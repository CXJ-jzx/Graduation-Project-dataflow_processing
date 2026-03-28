#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ML 参数调优验证实验（精简版）

实验 1: 收敛性  — 从已有 observations.csv 分析 J 值趋势
实验 2: 对比    — ML最优 vs 默认参数 vs 随机参数
实验 3: 稳定性  — 最优参数重复跑 3 次
"""

import os
import sys
import json
import time
import csv
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Any

import numpy as np
import yaml

from job_executor import JobExecutor
from observation_collector import ObservationCollector
from observation_store import ObservationStore
from feature_engineer import FeatureEngineer
from parameter_space import ParameterSpace

logger = logging.getLogger("ml_tuner.experiment")


def setup_logging():
    os.makedirs("experiments", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"experiments/experiment_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ],
        force=True
    )
    return log_file


def load_config(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def run_one_config(executor, collector, fe, theta, warmup=90) -> Dict[str, Any]:
    """提交一组参数 → 预热 → 采集 → 计算 J，返回结果字典"""

    result = {"theta": theta, "status": "failed", "j_total": float("inf")}

    # 1) 提交
    ok = executor.apply_parameters(theta)
    if not ok:
        result["status"] = "apply_failed"
        return result

    collector.reset_cache()

    # 2) 预热
    logger.info("  预热 %ds...", warmup)
    time.sleep(warmup)

    # 3) 等稳定 + 数据流
    collector.discover()
    collector.wait_until_stable(bp_threshold=0.5, stable_required=3,
                                check_interval=15, timeout=180)
    data_ok = collector.wait_until_data_flowing(min_rate=10, stable_required=2,
                                                 check_interval=15, timeout=180)
    if not data_ok:
        result["status"] = "no_data"
        return result

    # 4) 采集
    samples = collector.collect_window()
    if not samples:
        result["status"] = "no_samples"
        return result

    # 5) 聚合
    all_keys = set()
    for s in samples:
        all_keys.update(s.keys())
    features = {}
    for key in all_keys:
        vals = [s[key] for s in samples
                if key in s and isinstance(s[key], (int, float)) and not np.isnan(s[key])]
        features[key] = float(np.median(vals)) if vals else 0.0

    # 6) 计算 J
    obj = fe.compute_objective(features)
    if isinstance(obj, tuple):
        j_total = float(obj[0])
        j_parts = dict(obj[1] or {})
    else:
        j_total = float(obj)
        j_parts = {}

    result["status"] = "success"
    result["j_total"] = j_total
    result["j_parts"] = j_parts
    result["throughput"] = features.get("avg_input_rate", 0)
    result["latency_ms"] = features.get("e2e_latency_p99_ms", 0)
    result["backpressure"] = features.get("backpressure_ratio", 0)
    result["busy_ratio"] = features.get("avg_busy_ratio", 0)
    result["ckpt_dur"] = features.get("checkpoint_avg_dur", 0)

    return result


# ================================================================
# 实验 1: 收敛性（离线分析，不需要跑作业）
# ================================================================

def experiment_1_convergence(config_path: str):
    """从已有 observations.csv 分析收敛趋势"""
    logger.info("=" * 50)
    logger.info("实验 1: 收敛性分析")
    logger.info("=" * 50)

    cfg = load_config(config_path)
    csv_path = cfg.get("storage", {}).get("csv_path", "data/observations.csv")

    if not os.path.exists(csv_path):
        logger.error("找不到 %s，请先运行 ML 调优", csv_path)
        return

    store = ObservationStore(csv_path=csv_path, autosave=False, config_dict=cfg)
    try:
        store.load_csv(csv_path)
    except Exception as e:
        logger.error("加载失败: %s", e)
        return

    records = getattr(store, "records", [])
    valid = [r for r in records
             if getattr(r, "is_valid", getattr(r, "valid", False))]

    if len(valid) < 4:
        logger.warning("有效记录仅 %d 条，不足以分析", len(valid))
        return

    j_vals = [float(getattr(r, "j_total", float("inf"))) for r in valid]
    phases = [getattr(r, "phase", "") for r in valid]

    # running best
    running_best = []
    cur_best = float("inf")
    for j in j_vals:
        cur_best = min(cur_best, j)
        running_best.append(cur_best)

    # 分阶段统计
    warmup_j = [j for j, p in zip(j_vals, phases) if p == "warmup"]
    optimize_j = [j for j, p in zip(j_vals, phases) if p == "optimize"]

    # 关键指标 1: running_best 的总改善
    initial_j = j_vals[0]
    final_best_j = running_best[-1]
    total_improvement = (initial_j - final_best_j) / initial_j * 100

    # 关键指标 2: warmup 最优 vs optimize 最优
    warmup_best = min(warmup_j) if warmup_j else float("inf")
    optimize_best = min(optimize_j) if optimize_j else float("inf")
    bo_improvement = (warmup_best - optimize_best) / warmup_best * 100 if warmup_best < float("inf") else 0

    # 关键指标 3: 最后 3 轮 running_best 是否稳定（收敛判定）
    last_n = min(3, len(running_best))
    last_bests = running_best[-last_n:]
    converged = (max(last_bests) - min(last_bests)) < 0.01 * np.mean(last_bests)

    # 关键指标 4: 首次刷新 best 的轮次
    refresh_rounds = []
    for i in range(1, len(running_best)):
        if running_best[i] < running_best[i - 1]:
            refresh_rounds.append(i)

    # 输出详表
    logger.info("")
    logger.info("  轮次  |   J 值   | 当前最优 | 阶段")
    logger.info("  ------|----------|---------|--------")
    for i, (j, rb) in enumerate(zip(j_vals, running_best)):
        phase = phases[i]
        marker = " ★" if i in refresh_rounds or (i == 0 and rb == j) else ""
        logger.info("  %3d   | %.6f | %.6f | %-10s %s", i, j, rb, phase, marker)

    # 阶段统计
    logger.info("")
    logger.info("  ── 阶段统计 ──")
    if warmup_j:
        logger.info("  Warmup  (%d轮): 均值=%.6f, 最优=%.6f, 标准差=%.6f",
                     len(warmup_j), np.mean(warmup_j), min(warmup_j), np.std(warmup_j))
    if optimize_j:
        logger.info("  Optimize(%d轮): 均值=%.6f, 最优=%.6f, 标准差=%.6f",
                     len(optimize_j), np.mean(optimize_j), min(optimize_j), np.std(optimize_j))

    # 核心结论
    logger.info("")
    logger.info("  ── 收敛性指标 ──")
    logger.info("  初始 J:                %.6f", initial_j)
    logger.info("  最终最优 J:            %.6f", final_best_j)
    logger.info("  总改善幅度:            %.2f%%", total_improvement)
    logger.info("  BO 阶段改善:           %.2f%% (warmup最优 %.6f → optimize最优 %.6f)",
                 bo_improvement, warmup_best, optimize_best)
    logger.info("  best 刷新次数:         %d 次 (轮次: %s)", len(refresh_rounds),
                 refresh_rounds if refresh_rounds else "无")
    logger.info("  最后 %d 轮 best 稳定:  %s", last_n,
                 "✅ 是" if converged else "❌ 否")

    # 综合判定
    logger.info("")
    pass_total = total_improvement > 5
    pass_bo = bo_improvement > 0
    pass_converge = converged

    verdict_parts = []
    if pass_total:
        verdict_parts.append("总体改善 %.1f%%" % total_improvement)
    if pass_bo:
        verdict_parts.append("BO 进一步优化 %.1f%%" % bo_improvement)
    if pass_converge:
        verdict_parts.append("已收敛")

    if pass_total and pass_bo:
        verdict = "✅ 收敛性验证通过"
    elif pass_total:
        verdict = "✅ 总体收敛（BO 阶段改善有限）"
    elif pass_bo:
        verdict = "⚠️ BO 有改善但总幅度不大"
    else:
        verdict = "⚠️ 收敛不明显"

    logger.info("  综合结论: %s", verdict)
    if verdict_parts:
        logger.info("  依据:     %s", ", ".join(verdict_parts))
    logger.info("=" * 50)

    # 保存
    out = []
    for i, (j, rb) in enumerate(zip(j_vals, running_best)):
        theta = getattr(valid[i], "theta", {})
        out.append({
            "round": i,
            "phase": phases[i],
            "j_total": j,
            "running_best": rb,
            "is_new_best": "yes" if i in refresh_rounds or (i == 0 and rb == j) else "no",
            "theta": json.dumps(theta) if isinstance(theta, dict) else str(theta)
        })
    _save_csv(out, "experiments/exp1_convergence.csv")


# ================================================================
# 实验 2: 对比基线
# ================================================================

def experiment_2_comparison(config_path: str):
    """ML最优 vs 默认参数 vs 3组随机参数"""
    logger.info("=" * 50)
    logger.info("实验 2: 对比基线")
    logger.info("=" * 50)

    cfg = load_config(config_path)
    executor = JobExecutor(config_dict=cfg)
    collector = ObservationCollector(config_dict=cfg)
    fe = FeatureEngineer(config_dict=cfg)
    space = ParameterSpace(config_dict=cfg)

    results = []

    try:

        # A: 默认参数
        default_theta = space.get_defaults()
        logger.info("\n[A] 默认参数: %s", default_theta)
        r = run_one_config(executor, collector, fe, default_theta)
        r["strategy"] = "default"
        results.append(r)
        logger.info("  → J=%.6f" if r["status"] == "success" else "  → 失败: %s",
                     r.get("j_total", 0) if r["status"] == "success" else r["status"])


        # B: 随机 3 组
        for i in range(3):
            rand_theta = space.random_sample(1)[0]
            logger.info("\n[B] 随机参数 %d: %s", i + 1, rand_theta)
            r = run_one_config(executor, collector, fe, rand_theta)
            r["strategy"] = f"random_{i + 1}"
            results.append(r)
            logger.info("  → J=%.6f" if r["status"] == "success" else "  → 失败: %s",
                         r.get("j_total", 0) if r["status"] == "success" else r["status"])

        # C: ML 最优
        ml_theta = {"checkpoint_interval": 25000, "buffer_timeout": 10, "watermark_delay": 26000}
        logger.info("\n[C] ML最优参数: %s", ml_theta)
        r = run_one_config(executor, collector, fe, ml_theta)
        r["strategy"] = "ml_best"
        results.append(r)
        logger.info("  → J=%.6f" if r["status"] == "success" else "  → 失败: %s",
                     r.get("j_total", 0) if r["status"] == "success" else r["status"])

    finally:
        executor.close()
        collector.close()

    # 分析
    logger.info("")
    logger.info("=" * 50)
    logger.info("  %-12s %-10s %-10s %-10s %-10s", "策略", "J值", "吞吐量", "延迟ms", "状态")
    logger.info("  " + "-" * 52)

    for r in results:
        logger.info("  %-12s %-10.6f %-10.0f %-10.1f %-10s",
                     r["strategy"],
                     r.get("j_total", float("inf")),
                     r.get("throughput", 0),
                     r.get("latency_ms", 0),
                     r["status"])

    success = [r for r in results if r["status"] == "success"]
    ml_r = [r for r in success if r["strategy"] == "ml_best"]
    others = [r for r in success if r["strategy"] != "ml_best"]

    if ml_r and others:
        ml_j = ml_r[0]["j_total"]
        best_other = min(r["j_total"] for r in others)
        logger.info("")
        logger.info("  ML最优 J:    %.6f", ml_j)
        logger.info("  其他最优 J:  %.6f", best_other)
        logger.info("  结论: %s",
                     "✅ ML调优优于其他策略" if ml_j <= best_other
                     else "⚠️ ML调优未优于基线")

    _save_csv([{
        "strategy": r["strategy"],
        "j_total": r.get("j_total", "inf"),
        "throughput": r.get("throughput", 0),
        "latency_ms": r.get("latency_ms", 0),
        "backpressure": r.get("backpressure", 0),
        "busy_ratio": r.get("busy_ratio", 0),
        "status": r["status"],
        "theta": json.dumps(r["theta"]),
    } for r in results], "experiments/exp2_comparison.csv")


# ================================================================
# 实验 3: 稳定性复验
# ================================================================

def experiment_3_stability(config_path: str, n_repeats: int = 3):
    """最优参数重复跑 N 次"""
    logger.info("=" * 50)
    logger.info("实验 3: 稳定性复验 (%d 次)", n_repeats)
    logger.info("=" * 50)

    cfg = load_config(config_path)
    executor = JobExecutor(config_dict=cfg)
    collector = ObservationCollector(config_dict=cfg)
    fe = FeatureEngineer(config_dict=cfg)

    theta = {"checkpoint_interval": 25000, "buffer_timeout": 10, "watermark_delay": 26000}
    logger.info("  固定参数: %s", theta)

    results = []

    try:
        for i in range(n_repeats):
            logger.info("\n  复验 %d/%d", i + 1, n_repeats)
            r = run_one_config(executor, collector, fe, theta)
            r["repeat"] = i + 1
            results.append(r)

            if r["status"] == "success":
                logger.info("  → J=%.6f, throughput=%.0f, latency=%.1fms",
                            r["j_total"], r["throughput"], r["latency_ms"])
            else:
                logger.warning("  → 失败: %s", r["status"])

    finally:
        executor.close()
        collector.close()

    # 分析
    valid = [r for r in results if r["status"] == "success"]

    logger.info("")
    logger.info("=" * 50)

    if len(valid) < 2:
        logger.warning("成功次数不足 2，无法分析稳定性")
        return

    j_vals = [r["j_total"] for r in valid]
    tput_vals = [r["throughput"] for r in valid]
    lat_vals = [r["latency_ms"] for r in valid]

    mean_j = np.mean(j_vals)
    std_j = np.std(j_vals)
    cv = std_j / mean_j * 100 if mean_j > 0 else 0
    max_dev = (max(j_vals) - min(j_vals)) / mean_j * 100 if mean_j > 0 else 0

    logger.info("  成功次数:    %d / %d", len(valid), n_repeats)
    logger.info("  J 均值:      %.6f", mean_j)
    logger.info("  J 标准差:    %.6f", std_j)
    logger.info("  J 变异系数:  %.2f%%", cv)
    logger.info("  J 最大偏差:  %.2f%%", max_dev)
    logger.info("  吞吐量均值:  %.0f records/s", np.mean(tput_vals))
    logger.info("  延迟均值:    %.1f ms", np.mean(lat_vals))
    logger.info("")
    logger.info("  CV < 15%%:   %s", "✅ 通过" if cv < 15 else "❌ 未通过")
    logger.info("  结论:        %s",
                 "✅ 最优参数稳定可复现" if cv < 15
                 else "⚠️ 存在波动")

    _save_csv([{
        "repeat": r.get("repeat", i),
        "j_total": r.get("j_total", "inf"),
        "throughput": r.get("throughput", 0),
        "latency_ms": r.get("latency_ms", 0),
        "backpressure": r.get("backpressure", 0),
        "status": r["status"],
    } for i, r in enumerate(results)], "experiments/exp3_stability.csv")


# ================================================================
# 工具
# ================================================================

def _save_csv(rows: List[Dict], path: str):
    if not rows:
        return
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    keys = list(rows[0].keys())
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)
    logger.info("  结果已保存: %s", path)


# ================================================================
# 入口
# ================================================================

def main():
    parser = argparse.ArgumentParser(description="ML调优验证实验")
    parser.add_argument("--config", default="ml_tuner_config.yaml")
    parser.add_argument("--exp", type=int, default=0,
                        help="1=收敛性, 2=对比基线, 3=稳定性, 0=全部")
    parser.add_argument("--repeats", type=int, default=3, help="实验3复验次数")
    args = parser.parse_args()

    setup_logging()

    if args.exp in (0, 1):
        experiment_1_convergence(args.config)

    if args.exp in (0, 2):
        experiment_2_comparison(args.config)

    if args.exp in (0, 3):
        experiment_3_stability(args.config, n_repeats=args.repeats)

    print("\n结果目录: experiments/")


if __name__ == "__main__":
    main()


'''
cd E:\Desktop\bs_code\flink-rocketmq-demo\ML_Tuner

# 实验 1: 收敛性（不需要跑作业，直接分析已有数据，秒出结果）
python run_experiment.py --config ml_tuner_config.yaml --exp 1

# 实验 2: 对比基线（默认 vs 随机 vs ML最优，约 1h）
python run_experiment.py --config ml_tuner_config.yaml --exp 2

# 实验 3: 稳定性复验（最优参数跑 3 次，约 30min）
python run_experiment.py --config ml_tuner_config.yaml --exp 3 --repeats 3

# 全部顺序执行
python run_experiment.py --config ml_tuner_config.yaml --exp 0

'''