#!/usr/bin/env python3
"""
基线测试：用固定参数重复运行，记录稳态指标

用法:
  python tests/baseline_test.py --repeats 5 --output data/baseline_results.csv
"""

import os
import sys
import time
import csv
import argparse
import logging
import yaml
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from job_executor import JobExecutor
from observation_collector import ObservationCollector
from feature_engineer import FeatureEngineer

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("baseline_test")


def load_config(path="ml_tuner_config.yaml"):
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def run_baseline(config_path: str, repeats: int, output_path: str,
                 warmup_sec: int = 120, sample_count: int = 10,
                 sample_interval: int = 30):
    """
    基线测试主流程

    Args:
        config_path: 配置文件路径
        repeats: 重复次数
        output_path: 输出 CSV 路径
        warmup_sec: 每次预热时间（秒）
        sample_count: 每次采样次数
        sample_interval: 采样间隔（秒）
    """
    cfg = load_config(config_path)
    executor = JobExecutor(config_dict=cfg)
    collector = ObservationCollector(config_dict=cfg)
    fe = FeatureEngineer(config_dict=cfg)

    # 测试参数组：默认 + 几个人工经验值
    test_configs = {
        "default": {
            "checkpoint_interval": 30000,
            "buffer_timeout": 100,
            "watermark_delay": 5000,
        },
        "conservative": {
            "checkpoint_interval": 60000,
            "buffer_timeout": 50,
            "watermark_delay": 3000,
        },
        "aggressive": {
            "checkpoint_interval": 10000,
            "buffer_timeout": 10,
            "watermark_delay": 2000,
        },
    }

    results = []

    for config_name, theta in test_configs.items():
        logger.info("=" * 60)
        logger.info("测试配置: %s = %s", config_name, theta)
        logger.info("=" * 60)

        for repeat_id in range(repeats):
            logger.info("--- %s 第 %d/%d 次 ---", config_name, repeat_id + 1, repeats)

            # 1. 应用参数
            ok = executor.apply_parameters(theta)
            if not ok:
                logger.error("参数应用失败，跳过")
                continue

            # 2. 重置采集器
            collector.reset_cache()

            # 3. 预热
            logger.info("预热等待 %ds...", warmup_sec)
            time.sleep(warmup_sec)

            # 4. 采集
            samples = collector.collect_window()
            if not samples:
                logger.error("采集失败，跳过")
                continue

            # 5. 聚合
            features = fe.aggregate_median(samples)

            # 6. 计算目标函数
            j_total, j_parts = fe.compute_objective(features)
            is_valid, reason = fe.check_constraints(features)

            # 7. 记录
            row = {
                "config_name": config_name,
                "repeat_id": repeat_id,
                "checkpoint_interval": theta["checkpoint_interval"],
                "buffer_timeout": theta["buffer_timeout"],
                "watermark_delay": theta["watermark_delay"],
                "j_total": j_total,
                "j_latency": j_parts.get("latency", 0),
                "j_throughput": j_parts.get("throughput", 0),
                "j_checkpoint": j_parts.get("checkpoint", 0),
                "j_resource": j_parts.get("resource", 0),
                "avg_input_rate": features.get("avg_input_rate", 0),
                "e2e_latency_p99_ms": features.get("e2e_latency_p99_ms", 0),
                "backpressure_ratio": features.get("backpressure_ratio", 0),
                "checkpoint_avg_dur": features.get("checkpoint_avg_dur", 0),
                "avg_busy_ratio": features.get("avg_busy_ratio", 0),
                "heap_usage_ratio": features.get("heap_usage_ratio", 0),
                "is_valid": is_valid,
                "constraint_violation": reason,
            }
            results.append(row)

            logger.info("结果: J=%.6f, latency=%.1fms, throughput=%.1f, "
                        "backpressure=%.4f, valid=%s",
                        j_total,
                        features.get("e2e_latency_p99_ms", 0),
                        features.get("avg_input_rate", 0),
                        features.get("backpressure_ratio", 0),
                        is_valid)

    # 保存结果
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    if results:
        fieldnames = list(results[0].keys())
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        logger.info("结果已保存: %s (%d 条)", output_path, len(results))

    # 打印汇总
    print("\n" + "=" * 60)
    print("基线测试汇总")
    print("=" * 60)
    for config_name in test_configs:
        config_results = [r for r in results if r["config_name"] == config_name]
        valid_results = [r for r in config_results if r["is_valid"]]
        if valid_results:
            j_values = [r["j_total"] for r in valid_results]
            print(f"\n{config_name}:")
            print(f"  有效轮次: {len(valid_results)}/{len(config_results)}")
            print(f"  J 均值:   {np.mean(j_values):.6f}")
            print(f"  J 标准差: {np.std(j_values):.6f}")
            print(f"  J 最优:   {np.min(j_values):.6f}")
            print(f"  J 最差:   {np.max(j_values):.6f}")
        else:
            print(f"\n{config_name}: 无有效结果")

    executor.close()
    collector.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Baseline Test")
    parser.add_argument("--config", default="ml_tuner_config.yaml")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--output", default="data/baseline_results.csv")
    parser.add_argument("--warmup", type=int, default=120)
    args = parser.parse_args()

    run_baseline(args.config, args.repeats, args.output, args.warmup)
