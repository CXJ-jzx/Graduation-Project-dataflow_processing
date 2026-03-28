#!/usr/bin/env python3
"""诊断 baseline 属性名和延迟注册时间"""
import yaml
import time
import requests
from feature_engineer import FeatureEngineer
from observation_collector import ObservationCollector

with open("ml_tuner_config.yaml", 'r', encoding='utf-8') as f:
    cfg = yaml.safe_load(f)

# 1. 查 FeatureEngineer 的所有属性
fe = FeatureEngineer(config_dict=cfg)
print("=" * 50)
print("FeatureEngineer 属性（含 baseline/throughput）:")
for attr in sorted(dir(fe)):
    if attr.startswith('_'):
        continue
    if any(kw in attr.lower() for kw in ['baseline', 'throughput', 'tput', 'target']):
        val = getattr(fe, attr, '?')
        if not callable(val):
            print(f"  {attr} = {val}")

print("\n所有非下划线属性:")
for attr in sorted(dir(fe)):
    if attr.startswith('_'):
        continue
    val = getattr(fe, attr, '?')
    if not callable(val):
        print(f"  {attr} = {val}")

# 2. 查当前 job 的 e2e_latency 值
REST = cfg['flink']['rest_url'].rstrip('/')
resp = requests.get(f"{REST}/jobs/overview").json()
running = [j for j in resp.get("jobs", []) if j["state"] == "RUNNING"]
if running:
    jid = running[0]["jid"]
    print(f"\n{'=' * 50}")
    print(f"当前 Job: {jid}")

    vertices = requests.get(f"{REST}/jobs/{jid}").json().get("vertices", [])
    for v in vertices:
        if "EventFeature" in v.get("name", ""):
            vid = v["id"]
            print(f"EventFeature-Sink vertex: {vid}")

            # 列出所有指标
            url = f"{REST}/jobs/{jid}/vertices/{vid}/subtasks/metrics"
            metrics = requests.get(url).json()
            lat_metrics = [m["id"] for m in metrics
                           if any(kw in m["id"].lower()
                                  for kw in ['latency', 'e2e'])]

            print(f"\n延迟相关指标 ({len(lat_metrics)} 个):")
            for m in lat_metrics:
                print(f"  {m}")

            # 查值
            if lat_metrics:
                resp2 = requests.get(url, params={"get": ",".join(lat_metrics)})
                print(f"\n当前值:")
                for item in resp2.json():
                    print(f"  {item['id']}: max={item.get('max')}, "
                          f"avg={item.get('avg')}")
            break
else:
    print("\n没有运行中的 job")

print("\n✅ 完成")
