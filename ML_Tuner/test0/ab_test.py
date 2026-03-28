#!/usr/bin/env python3
"""
A/B 对比测试：默认参数 vs ML Tuner 最优参数

交替运行消除时间偏差，每组跑 N 次，输出统计检验结果

用法:
  python ab_test.py \
    --best-ckpt 110000 --best-buf 40 --best-wm 4000 \
    --repeats 3 --output data/ab_results.csv
"""

import os
import sys
import time
import csv
import argparse
import logging
import yaml
import numpy as np
from scipy import stats

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from job_executor import JobExecutor
from observation_collector import ObservationCollector
from feature_engineer import FeatureEngineer

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ab_test")

# ── 常量 ──
WARMUP_DEFAULT = 120
STABLE_BP_THRESHOLD = 0.50
STABLE_REQUIRED = 3
STABLE_CHECK_INTERVAL = 15
STABLE_TIMEOUT = 300


def load_config(path="ml_tuner_config.yaml"):
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_j_part(j_parts, name):
    """
    从 j_parts 中查找分项值，兼容多种 key 风格。

    feature_engineer 可能返回：
      {"latency": 0.1}  或  {"j_latency": 0.1}  或  {"Latency": 0.1}
    """
    for key in [name, f"j_{name}", f"J_{name}",
                name.capitalize(), name.upper(),
                name.lower()]:
        if key in j_parts:
            return j_parts[key]
    return 0.0


def run_single(executor, collector, fe, theta, warmup_sec, label):
    """
    运行一次测试

    A/B 测试使用全新启动（不从 savepoint 恢复），确保公平。
    """
    # ── 全新启动 ──
    ok = executor.apply_parameters(theta, use_savepoint=False)
    if not ok:
        logger.error("[%s] apply_parameters 失败", label)
        return None

    collector.reset_cache()

    # ── 预热 ──
    logger.info("[%s] 预热 %ds...", label, warmup_sec)
    time.sleep(warmup_sec)

    # ── 等待系统稳定 ──
    logger.info("[%s] 等待系统稳定...", label)
    stable = collector.wait_until_stable(
        bp_threshold=STABLE_BP_THRESHOLD,
        stable_required=STABLE_REQUIRED,
        check_interval=STABLE_CHECK_INTERVAL,
        timeout=STABLE_TIMEOUT,
    )
    if not stable:
        logger.warning("[%s] 系统未在 %ds 内稳定，仍继续采集",
                       label, STABLE_TIMEOUT)

    # ── 采集 ──
    samples = collector.collect_window()
    if not samples:
        logger.error("[%s] 采集返回空", label)
        return None

    # 检查延迟：如果窗口内所有样本延迟为 0，说明 Gauge 还没就绪
    # 等待额外 60s 再重采一次
    latencies = [s.get('e2e_latency_p99_ms', 0) for s in samples]
    if max(latencies) == 0:
        logger.warning("[%s] 所有样本延迟=0，等待 60s 后重采...", label)
        time.sleep(60)
        retry_samples = collector.collect_window()
        if retry_samples:
            retry_latencies = [s.get('e2e_latency_p99_ms', 0)
                               for s in retry_samples]
            if max(retry_latencies) > 0:
                logger.info("[%s] 重采成功，延迟恢复", label)
                samples = retry_samples
            else:
                logger.warning("[%s] 重采后延迟仍为 0", label)

    features = fe.aggregate_median(samples)
    j_total, j_parts = fe.compute_objective(features)
    is_valid, reason = fe.check_constraints(features)

    # 打印 j_parts 原始内容以便排查
    logger.info("[%s] j_parts=%s", label, j_parts)

    result = {
        "label": label,
        "j_total": j_total,
        "j_latency": get_j_part(j_parts, "latency"),
        "j_throughput": get_j_part(j_parts, "throughput"),
        "j_checkpoint": get_j_part(j_parts, "checkpoint"),
        "j_resource": get_j_part(j_parts, "resource"),
        "avg_input_rate": features.get("avg_input_rate", 0),
        "e2e_latency_p99_ms": features.get("e2e_latency_p99_ms", 0),
        "backpressure_ratio": features.get("backpressure_ratio", 0),
        "checkpoint_avg_dur": features.get("checkpoint_avg_dur", 0),
        "is_valid": is_valid,
        "constraint_reason": reason if not is_valid else "",
        **theta,
    }

    if not is_valid:
        logger.warning("[%s] 约束违规: %s", label, reason)

    return result


def run_ab_test(config_path, theta_default, theta_best, repeats, output_path,
                warmup_sec=WARMUP_DEFAULT):
    cfg = load_config(config_path)
    executor = JobExecutor(config_dict=cfg)
    collector = ObservationCollector(config_dict=cfg)
    fe = FeatureEngineer(config_dict=cfg)

    # ━━━ 预采集：建立并固定吞吐量 baseline ━━━
    logger.info("=" * 60)
    logger.info("预采集：建立吞吐量 baseline")
    logger.info("=" * 60)

    ok = executor.apply_parameters(theta_default, use_savepoint=False)
    if ok:
        collector.reset_cache()
        logger.info("预采集预热 %ds...", warmup_sec)
        time.sleep(warmup_sec)
        collector.wait_until_stable(
            bp_threshold=STABLE_BP_THRESHOLD,
            stable_required=STABLE_REQUIRED,
            check_interval=STABLE_CHECK_INTERVAL,
            timeout=STABLE_TIMEOUT,
        )
        baseline_samples = collector.collect_window()
        if baseline_samples:
            baseline_features = fe.aggregate_median(baseline_samples)
            baseline_rate = baseline_features.get("avg_input_rate", 0)
            if baseline_rate > 0 and hasattr(fe, "throughput_baseline"):
                fe.throughput_baseline = baseline_rate
                logger.info("✅ 吞吐量 baseline 固定为: %.1f rec/s",
                            baseline_rate)
            else:
                logger.warning("预采集吞吐量=%.1f，baseline 未更新",
                               baseline_rate)

    # 保存 baseline 值
    saved_baseline = getattr(fe, "throughput_baseline", None)
    logger.info("固定 baseline: %s", saved_baseline)

    results = []

    for i in range(repeats):
        logger.info("=" * 60)
        logger.info("A/B 测试 第 %d/%d 轮", i + 1, repeats)
        logger.info("=" * 60)

        # 每轮恢复 baseline（防止被自动更新覆盖）
        if saved_baseline is not None and hasattr(fe, "throughput_baseline"):
            fe.throughput_baseline = saved_baseline

        # 交替顺序消除时间偏差
        if i % 2 == 0:
            order = [("default", theta_default), ("ml_best", theta_best)]
        else:
            order = [("ml_best", theta_best), ("default", theta_default)]

        for label, theta in order:
            # 每次测试前恢复 baseline
            if saved_baseline is not None and hasattr(fe, "throughput_baseline"):
                fe.throughput_baseline = saved_baseline

            logger.info("--- 测试: %s ---", label)
            result = run_single(executor, collector, fe, theta,
                                warmup_sec, label)
            if result:
                result["repeat_id"] = i
                results.append(result)
                valid_str = "✅" if result["is_valid"] else "❌"
                logger.info("[%s] J=%.6f %s", label,
                            result["j_total"], valid_str)
            else:
                logger.error("[%s] 测试失败，跳过", label)

    # ━━━ 保存结果 ━━━
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    if results:
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            writer.writeheader()
            writer.writerows(results)
        logger.info("结果已保存: %s", output_path)

    # ━━━ 统计分析 ━━━
    default_j = [r["j_total"] for r in results
                 if r["label"] == "default" and r["is_valid"]]
    best_j = [r["j_total"] for r in results
              if r["label"] == "ml_best" and r["is_valid"]]

    print("\n" + "=" * 60)
    print("A/B 测试结果")
    print("=" * 60)

    if default_j and best_j:
        print(f"\n默认参数 (n={len(default_j)}):")
        print(f"  J 均值: {np.mean(default_j):.6f} ± {np.std(default_j):.6f}")
        print(f"  J 范围: [{np.min(default_j):.6f}, {np.max(default_j):.6f}]")

        print(f"\nML 最优 (n={len(best_j)}):")
        print(f"  J 均值: {np.mean(best_j):.6f} ± {np.std(best_j):.6f}")
        print(f"  J 范围: [{np.min(best_j):.6f}, {np.max(best_j):.6f}]")

        improvement = ((np.mean(default_j) - np.mean(best_j))
                       / np.mean(default_j) * 100)
        print(f"\n改善幅度: {improvement:.2f}%")

        # t 检验
        if len(default_j) >= 2 and len(best_j) >= 2:
            t_stat, p_value = stats.ttest_ind(default_j, best_j)
            print(f"\nt 检验:")
            print(f"  t 统计量: {t_stat:.4f}")
            print(f"  p 值:     {p_value:.6f}")
            if p_value < 0.05:
                print(f"  结论:     差异显著 (p < 0.05) ✅")
            else:
                print(f"  结论:     差异不显著 (p >= 0.05)")

        # 各分项对比
        print(f"\n{'─' * 60}")
        print("分项对比 (均值):")
        for metric in ["j_latency", "j_throughput", "j_checkpoint",
                        "j_resource", "avg_input_rate",
                        "e2e_latency_p99_ms", "backpressure_ratio",
                        "checkpoint_avg_dur"]:
            d_vals = [r[metric] for r in results
                      if r["label"] == "default" and r["is_valid"]]
            b_vals = [r[metric] for r in results
                      if r["label"] == "ml_best" and r["is_valid"]]
            if d_vals and b_vals:
                d_mean = np.mean(d_vals)
                b_mean = np.mean(b_vals)
                diff = ""
                if d_mean > 0:
                    pct = (d_mean - b_mean) / d_mean * 100
                    diff = f" ({pct:+.1f}%)"
                print(f"  {metric:<25} 默认={d_mean:>10.2f}"
                      f"  最优={b_mean:>10.2f}{diff}")
    else:
        print("有效数据不足，无法比较")
        if not default_j:
            print("  默认组无有效数据")
        if not best_j:
            print("  ML最优组无有效数据")

    executor.close()
    collector.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A/B Test")
    parser.add_argument("--config", default="ml_tuner_config.yaml")
    parser.add_argument("--best-ckpt", type=int, required=True,
                        help="ML最优 checkpoint_interval")
    parser.add_argument("--best-buf", type=int, required=True,
                        help="ML最优 buffer_timeout")
    parser.add_argument("--best-wm", type=int, required=True,
                        help="ML最优 watermark_delay")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--output", default="data/ab_results.csv")
    parser.add_argument("--warmup", type=int, default=WARMUP_DEFAULT)
    args = parser.parse_args()

    theta_default = {
        "checkpoint_interval": 30000,
        "buffer_timeout": 100,
        "watermark_delay": 5000,
    }
    theta_best = {
        "checkpoint_interval": args.best_ckpt,
        "buffer_timeout": args.best_buf,
        "watermark_delay": args.best_wm,
    }

    run_ab_test(args.config, theta_default, theta_best,
                args.repeats, args.output, args.warmup)
