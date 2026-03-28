#!/usr/bin/env python3
"""
完整诊断：提交全新 job → 灌数据 → 检查指标
"""
import requests
import time
import json
import subprocess
import sys
import os

REST = "http://192.168.56.151:8081"

print("=" * 70)
print("阶段 1：确认没有残留 job")
print("=" * 70)

resp = requests.get(f"{REST}/jobs/overview").json()
running = [j for j in resp.get("jobs", []) if j["state"] == "RUNNING"]
if running:
    for j in running:
        jid = j["jid"]
        print(f"  Cancel 残留 job: {jid}")
        requests.patch(f"{REST}/jobs/{jid}?mode=cancel")
    time.sleep(10)
    print("  等待 10s...")
else:
    print("  无残留 job ✓")

print()
print("=" * 70)
print("阶段 2：提交全新 job")
print("=" * 70)

# 找 JAR
jars = requests.get(f"{REST}/jars").json()
jar_id = None
for f in jars.get("files", []):
    if "flink-rocketmq" in f["name"]:
        jar_id = f["id"]
        break

if not jar_id:
    print("  ❌ 没找到 JAR，请先上传")
    sys.exit(1)

print(f"  JAR: {jar_id}")

# 提交（默认参数，无 savepoint）
submit_url = f"{REST}/jars/{jar_id}/run"
submit_body = {
    "programArgs": (
        "--checkpoint-interval 30000 "
        "--buffer-timeout 100 "
        "--watermark-delay 5000 "
        "--p-source 2 --p-filter 4 --p-signal 4 "
        "--p-feature 4 --p-grid 4 --p-sink 2"
    ),
    "allowNonRestoredState": True,
}
r = requests.post(submit_url, json=submit_body)
if not r.ok:
    print(f"  ❌ 提交失败: {r.status_code} {r.text}")
    sys.exit(1)

jid = r.json()["jobid"]
print(f"  ✅ Job 提交成功: {jid}")

# 等待 RUNNING
for i in range(30):
    time.sleep(2)
    st = requests.get(f"{REST}/jobs/{jid}").json().get("state", "")
    if st == "RUNNING":
        print(f"  ✅ Job RUNNING")
        break
else:
    print(f"  ❌ Job 未能启动")
    sys.exit(1)

print()
print("=" * 70)
print("阶段 3：启动 Producer，等待数据流动")
print("=" * 70)
print("  请确认 Producer 正在运行！")
print("  如果没有，请在另一个终端启动 DS2 或手动启动 Producer")
print("  等待 120 秒让数据充分流入...")

for countdown in range(120, 0, -30):
    print(f"  剩余 {countdown}s...")
    time.sleep(30)

print()
print("=" * 70)
print("阶段 4：全面指标检查")
print("=" * 70)

vertices = requests.get(f"{REST}/jobs/{jid}").json()

for v in vertices.get("vertices", []):
    vid = v["id"]
    name = v["name"]
    parallelism = v.get("parallelism", 1)

    print(f"\n{'─' * 60}")
    print(f"算子: {name} (并行度={parallelism})")
    print(f"{'─' * 60}")

    # 方式 A：vertex 级别 /metrics
    url_vertex = f"{REST}/jobs/{jid}/vertices/{vid}/metrics"
    r_list = requests.get(url_vertex)
    all_metrics = r_list.json() if r_list.ok else []
    metric_names = [m.get("id", str(m)) for m in all_metrics]
    print(f"  vertex /metrics 共 {len(metric_names)} 个指标")

    # 筛选关键指标
    keywords = ["busy", "idle", "backpressure", "backPressure",
                "numRecords", "Latency", "latency", "checkpoint",
                "buffers", "inputQueue", "outputQueue"]
    interesting = [n for n in metric_names
                   if any(k.lower() in n.lower() for k in keywords)]
    if interesting:
        print(f"  关键指标:")
        for m in interesting[:20]:
            print(f"    {m}")

    # 获取所有指标的值（取前 20 个试试）
    if metric_names:
        sample = metric_names[:20]
        r_vals = requests.get(url_vertex,
                              params={"get": ",".join(sample)})
        vals = r_vals.json() if r_vals.ok else []
        if vals:
            print(f"  指标值采样 ({len(vals)} 个):")
            for item in vals[:10]:
                mid = item.get("id", "?")
                print(f"    {mid}: sum={item.get('sum')}, "
                      f"avg={item.get('avg')}, "
                      f"min={item.get('min')}, "
                      f"max={item.get('max')}")

    # 方式 B：subtask 聚合 /subtasks/metrics
    url_st_agg = f"{REST}/jobs/{jid}/vertices/{vid}/subtasks/metrics"
    r_st_list = requests.get(url_st_agg)
    st_agg_metrics = r_st_list.json() if r_st_list.ok else []
    st_agg_names = [m.get("id", str(m)) for m in st_agg_metrics]
    print(f"\n  subtasks/metrics 共 {len(st_agg_names)} 个指标")

    st_interesting = [n for n in st_agg_names
                      if any(k.lower() in n.lower() for k in keywords)]
    if st_interesting:
        print(f"  关键指标:")
        for m in st_interesting[:20]:
            print(f"    {m}")

    if st_agg_names:
        sample2 = st_agg_names[:20]
        r_vals2 = requests.get(url_st_agg,
                               params={"get": ",".join(sample2)})
        vals2 = r_vals2.json() if r_vals2.ok else []
        if vals2:
            print(f"  指标值采样 ({len(vals2)} 个):")
            for item in vals2[:10]:
                mid = item.get("id", "?")
                print(f"    {mid}: sum={item.get('sum')}, "
                      f"avg={item.get('avg')}")

    # 方式 C：单个 subtask /subtasks/0/metrics
    url_st0 = f"{REST}/jobs/{jid}/vertices/{vid}/subtasks/0/metrics"
    r_st0_list = requests.get(url_st0)
    st0_metrics = r_st0_list.json() if r_st0_list.ok else []
    st0_names = [m.get("id", str(m)) for m in st0_metrics]
    print(f"\n  subtask[0] /metrics 共 {len(st0_names)} 个指标")

    st0_interesting = [n for n in st0_names
                       if any(k.lower() in n.lower() for k in keywords)]
    if st0_interesting:
        print(f"  关键指标:")
        for m in st0_interesting[:20]:
            print(f"    {m}")

    if st0_names:
        sample3 = st0_names[:20]
        r_vals3 = requests.get(url_st0,
                               params={"get": ",".join(sample3)})
        vals3 = r_vals3.json() if r_vals3.ok else []
        if vals3:
            print(f"  指标值采样 ({len(vals3)} 个):")
            for item in vals3[:10]:
                mid = item.get("id", "?")
                print(f"    {mid}: value={item.get('value')}")

print()
print("=" * 70)
print("阶段 5：Checkpoint 信息")
print("=" * 70)
ckpt = requests.get(f"{REST}/jobs/{jid}/checkpoints").json()
counts = ckpt.get("counts", {})
print(f"  completed={counts.get('completed', 0)}, "
      f"failed={counts.get('failed', 0)}, "
      f"in_progress={counts.get('in_progress', 0)}")
latest = ckpt.get("latest", {}).get("completed")
if latest:
    print(f"  最近完成: duration={latest.get('duration')}ms, "
          f"size={latest.get('state_size', 0)} bytes")

print()
print("=" * 70)
print("阶段 6：Flink 配置检查")
print("=" * 70)
# 检查 metrics 相关配置
try:
    import paramiko

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('192.168.56.151', username='root', password='hadoop')

    stdin, stdout, stderr = ssh.exec_command(
        'cat /opt/flink/conf/flink-conf.yaml | grep -i metric'
    )
    metrics_conf = stdout.read().decode().strip()
    if metrics_conf:
        print("  flink-conf.yaml 中的 metrics 配置:")
        for line in metrics_conf.split('\n'):
            print(f"    {line}")
    else:
        print("  ⚠️ flink-conf.yaml 中没有 metrics 相关配置!")

    stdin, stdout, stderr = ssh.exec_command(
        'cat /opt/flink/conf/flink-conf.yaml | grep -i "rest\\.server"'
    )
    rest_conf = stdout.read().decode().strip()
    if rest_conf:
        print(f"  REST 配置: {rest_conf}")

    ssh.close()
except Exception as e:
    print(f"  SSH 检查失败: {e}")
    print("  请手动检查: cat /opt/flink/conf/flink-conf.yaml | grep -i metric")

print("\n\n✅ 诊断完成！请将以上全部输出贴给我。")
