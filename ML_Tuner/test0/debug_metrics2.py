#!/usr/bin/env python3
"""诊断 Flink 指标 API 的正确格式"""
import requests
import json

REST = "http://192.168.56.151:8081"

resp = requests.get(f"{REST}/jobs/overview").json()
running = [j for j in resp.get("jobs", []) if j["state"] == "RUNNING"]
if not running:
    print("没有运行中的作业!")
    exit(1)

jid = running[0]["jid"]
print(f"Job: {jid}\n")

vertices = requests.get(f"{REST}/jobs/{jid}").json()
v = vertices["vertices"][1]  # 取 filter 算子
vid = v["id"]
name = v["name"]
print(f"测试算子: {name} ({vid})\n")

# ============ 测试各种 API 端点 ============

print("=" * 70)
print("1. /subtasks/metrics (当前代码用的)")
print("=" * 70)
url = f"{REST}/jobs/{jid}/vertices/{vid}/subtasks/metrics"
r = requests.get(url)
print(f"  不带参数: {r.status_code} → {r.text[:500]}")

r2 = requests.get(url, params={"get": "busyTimeMsPerSecond,idleTimeMsPerSecond"})
print(f"  带get参数: {r2.status_code} → {r2.text[:500]}")

print()
print("=" * 70)
print("2. /metrics (vertex 级别)")
print("=" * 70)
url2 = f"{REST}/jobs/{jid}/vertices/{vid}/metrics"
r3 = requests.get(url2)
print(f"  可用指标列表: {r3.status_code}")
metrics_list = r3.json() if r3.ok else []
for m in metrics_list[:30]:
    print(f"    {m.get('id', m)}")
if len(metrics_list) > 30:
    print(f"    ... 共 {len(metrics_list)} 个指标")

# 如果有 busy 相关的指标，尝试获取
busy_metrics = [m.get("id", "") for m in metrics_list
                if "busy" in m.get("id", "").lower()
                or "idle" in m.get("id", "").lower()
                or "backpressure" in m.get("id", "").lower()
                or "backPressure" in m.get("id", "").lower()]
print(f"\n  背压相关指标: {busy_metrics}")

if busy_metrics:
    r4 = requests.get(url2, params={"get": ",".join(busy_metrics)})
    print(f"  获取值: {r4.text[:500]}")

print()
print("=" * 70)
print("3. /subtasks (subtask 列表)")
print("=" * 70)
url3 = f"{REST}/jobs/{jid}/vertices/{vid}"
r5 = requests.get(url3)
if r5.ok:
    vdata = r5.json()
    subtasks = vdata.get("subtasks", [])
    print(f"  subtask 数量: {len(subtasks)}")
    if subtasks:
        st = subtasks[0]
        st_idx = st.get("subtask", 0)
        print(f"  第一个 subtask: index={st_idx}")

        # 尝试 subtask 级别的 metrics
        url4 = f"{REST}/jobs/{jid}/vertices/{vid}/subtasks/{st_idx}/metrics"
        r6 = requests.get(url4)
        print(f"\n  subtask metrics 列表: {r6.status_code}")
        st_metrics = r6.json() if r6.ok else []
        for m in st_metrics[:30]:
            print(f"    {m.get('id', m)}")
        if len(st_metrics) > 30:
            print(f"    ... 共 {len(st_metrics)} 个指标")

        st_busy = [m.get("id", "") for m in st_metrics
                   if "busy" in m.get("id", "").lower()
                   or "idle" in m.get("id", "").lower()
                   or "back" in m.get("id", "").lower()]
        print(f"\n  subtask 背压相关: {st_busy}")

        if st_busy:
            r7 = requests.get(url4, params={"get": ",".join(st_busy)})
            print(f"  获取值: {r7.text[:500]}")

print()
print("=" * 70)
print("4. TaskManager 级别指标")
print("=" * 70)
tms = requests.get(f"{REST}/taskmanagers").json()
for tm in tms.get("taskmanagers", [])[:2]:
    tmid = tm["id"]
    print(f"\n  TM: {tmid}")
    tm_url = f"{REST}/taskmanagers/{tmid}/metrics"
    r8 = requests.get(tm_url)
    tm_metrics = r8.json() if r8.ok else []
    tm_busy = [m.get("id", "") for m in tm_metrics
               if "busy" in m.get("id", "").lower()
               or "idle" in m.get("id", "").lower()
               or "backPressure" in m.get("id", "").lower()]
    print(f"  背压相关: {tm_busy[:10]}")

print()
print("=" * 70)
print("5. Flink 版本")
print("=" * 70)
config = requests.get(f"{REST}/config").json()
print(f"  {json.dumps(config, indent=2)[:500]}")
