#!/usr/bin/env python3
"""调试 Flink 背压指标"""
import requests
import json

REST = "http://192.168.56.151:8081"

# 1. 找 running job
resp = requests.get(f"{REST}/jobs/overview").json()
running = [j for j in resp.get("jobs", []) if j["state"] == "RUNNING"]
if not running:
    print("没有运行中的作业!")
    exit(1)

jid = running[0]["jid"]
print(f"Job: {jid}\n")

# 2. 获取所有 vertex
vertices = requests.get(f"{REST}/jobs/{jid}").json()
for v in vertices.get("vertices", []):
    vid = v["id"]
    name = v["name"]

    # 3. 查每个算子的 busy/idle
    url = (f"{REST}/jobs/{jid}/vertices/{vid}"
           f"/subtasks/metrics?get=busyTimeMsPerSecond,idleTimeMsPerSecond")
    metrics = requests.get(url).json()

    busy = idle = "N/A"
    for m in metrics:
        if m["id"] == "busyTimeMsPerSecond":
            busy = m.get("sum", m.get("avg", "N/A"))
        if m["id"] == "idleTimeMsPerSecond":
            idle = m.get("sum", m.get("avg", "N/A"))

    # 计算 backpressure
    try:
        bp_ms = max(0, 1000 - float(busy) - float(idle))
        bp_ratio = bp_ms / 1000
    except:
        bp_ratio = "计算失败"

    print(f"  {name[:50]:<50}")
    print(f"    busy={busy}, idle={idle}, bp_ratio={bp_ratio}")
    print(f"    原始返回: {json.dumps(metrics, indent=2)}")
    print()

