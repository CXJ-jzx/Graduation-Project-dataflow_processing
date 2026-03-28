#!/usr/bin/env python3
"""验证修复后的指标采集"""
import yaml
from observation_collector import ObservationCollector

with open("ml_tuner_config.yaml", 'r', encoding='utf-8') as f:
    cfg = yaml.safe_load(f)

collector = ObservationCollector(config_dict=cfg)
if collector.discover():
    print("\n=== 采集一次快照 ===")
    snapshot = collector._collect_one_snapshot()
    for k, v in sorted(snapshot.items()):
        if isinstance(v, float):
            print(f"  {k:<35} = {v:.4f}")
        else:
            print(f"  {k:<35} = {v}")

    print(f"\n=== 关键指标 ===")
    print(f"  avg_input_rate:      {snapshot.get('avg_input_rate', 0):.1f} rec/s")
    print(f"  backpressure_ratio:  {snapshot.get('backpressure_ratio', -1):.4f}")
    print(f"  e2e_latency_p99_ms:  {snapshot.get('e2e_latency_p99_ms', -1):.2f} ms")
    print(f"  checkpoint_avg_dur:  {snapshot.get('checkpoint_avg_dur', 0):.0f} ms")

collector.close()
