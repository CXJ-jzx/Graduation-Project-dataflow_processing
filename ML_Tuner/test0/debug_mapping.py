#!/usr/bin/env python3
"""诊断指标名映射和吞吐量查询"""
import yaml
import json
import requests
from observation_collector import ObservationCollector

with open("ml_tuner_config.yaml", 'r', encoding='utf-8') as f:
    cfg = yaml.safe_load(f)

collector = ObservationCollector(config_dict=cfg)
collector.discover()

REST = collector._rest_url
JID = collector._job_id

print("=" * 70)
print("1. 指标名映射结果")
print("=" * 70)
for op_key, mapping in collector._metric_name_map.items():
    print(f"\n  [{op_key}]")
    if mapping:
        for generic, actual in mapping.items():
            flag = " ★" if generic != actual else ""
            print(f"    {generic:<40} → {actual}{flag}")
    else:
        print(f"    (空！没有找到任何映射)")

print()
print("=" * 70)
print("2. Source 算子的全部可用指标")
print("=" * 70)
source_vid = collector._vertex_map.get('source')
if source_vid:
    url = f"{REST}/jobs/{JID}/vertices/{source_vid}/subtasks/metrics"
    resp = requests.get(url)
    all_metrics = [m.get("id", "") for m in resp.json()] if resp.ok else []

    # 按关键词分组
    rate_metrics = [m for m in all_metrics if 'ecord' in m or 'Rate' in m or 'rate' in m]
    print(f"  共 {len(all_metrics)} 个指标")
    print(f"\n  吞吐量相关:")
    for m in rate_metrics:
        print(f"    {m}")

    # 尝试获取所有 rate 指标的值
    if rate_metrics:
        print(f"\n  获取值:")
        resp2 = requests.get(url, params={"get": ",".join(rate_metrics)})
        for item in resp2.json():
            mid = item.get("id", "?")
            print(f"    {mid}: sum={item.get('sum')}, avg={item.get('avg')}")

print()
print("=" * 70)
print("3. Filter 算子的全部可用指标")
print("=" * 70)
filter_vid = collector._vertex_map.get('filter')
if filter_vid:
    url = f"{REST}/jobs/{JID}/vertices/{filter_vid}/subtasks/metrics"
    resp = requests.get(url)
    all_metrics = [m.get("id", "") for m in resp.json()] if resp.ok else []

    rate_metrics = [m for m in all_metrics if 'ecord' in m or 'Rate' in m or 'rate' in m]
    print(f"  共 {len(all_metrics)} 个指标")
    print(f"\n  吞吐量相关:")
    for m in rate_metrics:
        print(f"    {m}")

    if rate_metrics:
        print(f"\n  获取值:")
        resp2 = requests.get(url, params={"get": ",".join(rate_metrics)})
        for item in resp2.json():
            mid = item.get("id", "?")
            print(f"    {mid}: sum={item.get('sum')}, avg={item.get('avg')}")

print()
print("=" * 70)
print("4. Feature 算子吞吐量（之前 debug_metrics3 中有值）")
print("=" * 70)
feature_vid = collector._vertex_map.get('feature')
if feature_vid:
    url = f"{REST}/jobs/{JID}/vertices/{feature_vid}/subtasks/metrics"
    resp = requests.get(url)
    all_metrics = [m.get("id", "") for m in resp.json()] if resp.ok else []

    rate_metrics = [m for m in all_metrics if 'ecord' in m or 'Rate' in m or 'rate' in m]
    print(f"\n  吞吐量相关:")
    for m in rate_metrics:
        print(f"    {m}")

    if rate_metrics:
        print(f"\n  获取值:")
        resp2 = requests.get(url, params={"get": ",".join(rate_metrics)})
        for item in resp2.json():
            mid = item.get("id", "?")
            print(f"    {mid}: sum={item.get('sum')}, avg={item.get('avg')}")

print()
print("=" * 70)
print("5. buffers.outputQueueSize 原始值（排查负数）")
print("=" * 70)
if filter_vid:
    url = f"{REST}/jobs/{JID}/vertices/{filter_vid}/subtasks/metrics"
    resp = requests.get(url, params={"get": "buffers.outputQueueSize,buffers.outputQueueLength,buffers.inputQueueSize"})
    print(f"  Filter 缓冲区:")
    for item in resp.json():
        mid = item.get("id", "?")
        print(f"    {mid}: sum={item.get('sum')}, avg={item.get('avg')}, "
              f"min={item.get('min')}, max={item.get('max')}")

print()
print("=" * 70)
print("6. 手动测试：直接查询带前缀的指标名")
print("=" * 70)
if source_vid:
    # 从 debug_metrics3 中知道 source 有这些指标
    test_names = [
        "Source__RocketMQ-Source.numRecordsInPerSecond",
        "Source__RocketMQ-Source.numRecordsOutPerSecond",
        "Watermark-Assigner.numRecordsIn",
        "numRecordsOutPerSecond",
        "numRecordsInPerSecond",
    ]
    url = f"{REST}/jobs/{JID}/vertices/{source_vid}/subtasks/metrics"
    resp = requests.get(url, params={"get": ",".join(test_names)})
    print(f"  Source 手动查询:")
    result = resp.json()
    if result:
        for item in result:
            mid = item.get("id", "?")
            print(f"    {mid}: sum={item.get('sum')}, avg={item.get('avg')}")
    else:
        print(f"    返回空! 响应: {resp.text[:200]}")



