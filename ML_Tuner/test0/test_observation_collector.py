#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试 ObservationCollector 指标采集正确性

用途：
1) 验证 discover / 指标映射是否正常
2) 连续采样，检查关键指标是否合理
3) 检测延迟指标是否误命中 mailboxLatency
4) 输出可用于排障的详细报告

运行前提：
- Flink 作业处于 RUNNING
- Producer 正在发送数据（否则吞吐量可能为 0）

用法：
  python test_observation_collector.py
  python test_observation_collector.py --samples 6 --interval 10
"""

import argparse
import time
import yaml
import requests
from statistics import median

from observation_collector import ObservationCollector


def load_config(path="ml_tuner_config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="ml_tuner_config.yaml")
    parser.add_argument("--samples", type=int, default=6, help="采样次数")
    parser.add_argument("--interval", type=int, default=10, help="采样间隔秒")
    args = parser.parse_args()

    cfg = load_config(args.config)
    collector = ObservationCollector(config_dict=cfg)

    print("=" * 70)
    print("1) Discover 检查")
    print("=" * 70)
    ok = collector.discover()
    print(f"discover: {ok}")
    if not ok:
        print("❌ discover 失败，退出")
        return

    print(f"job_id: {collector._job_id}")
    print("vertex_map:")
    for k, v in collector._vertex_map.items():
        print(f"  {k:10s} -> {v[:12]} ...")

    print("\nevent_sink 指标映射:")
    event_map = collector._metric_name_map.get("event_sink", {})
    for k, v in event_map.items():
        print(f"  {k:25s} -> {v}")

    # 直接查看 event_sink 当前可用指标（延迟相关）
    print("\n当前 event_sink 可用延迟指标:")
    event_vid = collector._vertex_map.get("event_sink")
    if event_vid:
        url = f"{collector._rest_url}/jobs/{collector._job_id}/vertices/{event_vid}/subtasks/metrics"
        resp = requests.get(url, timeout=10)
        all_metrics = [m.get("id", "") for m in (resp.json() if resp.ok else [])]
        lat_metrics = [m for m in all_metrics if ("latency" in m.lower() or "e2e" in m.lower())]
        if lat_metrics:
            for m in lat_metrics:
                print(f"  {m}")
        else:
            print("  (无)")
    else:
        print("  (未找到 event_sink)")

    print("\n" + "=" * 70)
    print("2) 连续采样检查")
    print("=" * 70)

    snapshots = []
    for i in range(args.samples):
        s = collector._collect_one_snapshot()
        snapshots.append(s)

        print(f"\n样本 #{i+1}:")
        print(f"  avg_input_rate      = {safe_float(s.get('avg_input_rate')):.3f}")
        print(f"  backpressure_ratio  = {safe_float(s.get('backpressure_ratio')):.3f}")
        print(f"  e2e_latency_p99_ms  = {safe_float(s.get('e2e_latency_p99_ms')):.3f}")
        print(f"  checkpoint_avg_dur  = {safe_float(s.get('checkpoint_avg_dur')):.3f}")
        print(f"  filter_in_rate      = {safe_float(s.get('filter_in_rate')):.3f}")
        print(f"  feature_in_rate     = {safe_float(s.get('feature_in_rate')):.3f}")

        if i < args.samples - 1:
            time.sleep(args.interval)

    print("\n" + "=" * 70)
    print("3) 合规性判定")
    print("=" * 70)

    rates = [safe_float(s.get("avg_input_rate")) for s in snapshots]
    bps = [safe_float(s.get("backpressure_ratio")) for s in snapshots]
    lats = [safe_float(s.get("e2e_latency_p99_ms")) for s in snapshots]
    cks = [safe_float(s.get("checkpoint_avg_dur")) for s in snapshots]

    # 判定项
    cond_rate_nonzero = any(r > 0 for r in rates)
    cond_bp_range = all(0.0 <= b <= 1.0 for b in bps)
    cond_lat_nonzero = any(l > 0 for l in lats)
    cond_ck_nonneg = all(c >= 0 for c in cks)

    print(f"吞吐量非零（至少一次）: {'✅' if cond_rate_nonzero else '❌'}")
    print(f"背压范围合法 [0,1]:     {'✅' if cond_bp_range else '❌'}")
    print(f"延迟非零（至少一次）:   {'✅' if cond_lat_nonzero else '❌'}")
    print(f"Checkpoint 非负:       {'✅' if cond_ck_nonneg else '❌'}")

    # 检查是否误命中 mailbox 指标
    lat_name = getattr(collector, "_latency_metric_name", None)
    print(f"\n当前缓存延迟指标名: {lat_name}")
    if lat_name and "mailbox" in lat_name.lower():
        print("⚠️ 警告：当前延迟指标命中了 mailboxLatency（代理指标，不是严格 e2e）")
    else:
        print("✅ 延迟指标未命中 mailbox")

    print("\n统计摘要:")
    print(f"  avg_input_rate   median={median(rates):.3f}  min={min(rates):.3f}  max={max(rates):.3f}")
    print(f"  backpressure     median={median(bps):.3f}    min={min(bps):.3f}    max={max(bps):.3f}")
    print(f"  e2e_latency      median={median(lats):.3f}   min={min(lats):.3f}   max={max(lats):.3f}")
    print(f"  checkpoint_dur   median={median(cks):.3f}    min={min(cks):.3f}    max={max(cks):.3f}")

    print("\n结论:")
    if cond_rate_nonzero and cond_bp_range and cond_ck_nonneg:
        if cond_lat_nonzero:
            print("✅ 采集器核心指标可用（吞吐/背压/延迟/ckpt）")
        else:
            print("⚠️ 吞吐/背压/ckpt 正常，但延迟指标仍不可用（需继续排查 Gauge 注册）")
    else:
        print("❌ 采集器存在基础问题，请先修复再进行 A/B 或 BO")

    collector.close()


if __name__ == "__main__":
    main()
