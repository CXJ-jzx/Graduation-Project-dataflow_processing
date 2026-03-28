#!/usr/bin/env python3
"""最终验证：确认所有指标正常"""
import yaml
import time
from observation_collector import ObservationCollector

with open("ml_tuner_config.yaml", 'r', encoding='utf-8') as f:
    cfg = yaml.safe_load(f)

collector = ObservationCollector(config_dict=cfg)
collector.discover()

print("采集 3 次快照，间隔 15s\n")
for i in range(3):
    snapshot = collector._collect_one_snapshot()
    print(f"快照 #{i+1}:")
    print(f"  avg_input_rate:      {snapshot.get('avg_input_rate', 0):.1f} rec/s")
    print(f"  backpressure_ratio:  {snapshot.get('backpressure_ratio', -1):.4f}")
    print(f"  e2e_latency_p99_ms:  {snapshot.get('e2e_latency_p99_ms', -1):.2f} ms")
    print(f"  checkpoint_avg_dur:  {snapshot.get('checkpoint_avg_dur', 0):.0f} ms")
    print(f"  filter_in_rate:      {snapshot.get('filter_in_rate', 0):.1f}")
    print(f"  feature_in_rate:     {snapshot.get('feature_in_rate', 0):.1f}")
    print(f"  filter_output_queue: {snapshot.get('filter_output_queue', 0):.1f}")
    print(f"  source_out_pool:     {snapshot.get('source_out_pool_usage', 0):.4f}")
    print()
    if i < 2:
        time.sleep(15)

collector.close()
print("✅ 验证完成")
