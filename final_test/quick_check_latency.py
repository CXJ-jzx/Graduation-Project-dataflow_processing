#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
final_test/quick_check_latency.py

端到端延迟指标可用性检测脚本（v3 — 含 e2e_latency_p99_ms 检测）

检测目标:
  1. Sink__EventFeature-Sink.e2e_latency_p99_ms  ← 新增：Source→Sink 端到端延迟
  2. Sink__EventFeature-Sink.latency_p99          ← 保留：Sink 内部处理耗时
  3. mailboxLatencyMs_p99                         ← Flink 内置邮箱延迟
  4. 各算子吞吐量、繁忙率、Checkpoint 状态

自动化流程:
  重启集群 → 上传JAR → 提交Job → 启动Producer → 预热 → 检测 → 清理

使用方式:
  cd E:\\Desktop\\bs_code\\flink-rocketmq-demo\\final_test
  python quick_check_latency.py
  python quick_check_latency.py --warmup 60
  python quick_check_latency.py --skip-restart
  python quick_check_latency.py --skip-upload       # JAR已在虚拟机上，跳过上传
"""

import os
import sys
import math
import time
import json
import subprocess
import argparse
import requests
from datetime import datetime
from typing import Dict, List, Optional

try:
    import yaml
    import paramiko
except ImportError:
    print("请先安装依赖: pip install pyyaml paramiko requests")
    sys.exit(1)

# ==================== 路径与常量 ====================

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FINAL_TEST_DIR = os.path.dirname(os.path.abspath(__file__))

LOCAL_JAR_PATH = os.path.join(
    PROJECT_ROOT, "target",
    "flink-rocketmq-demo-1.0-SNAPSHOT.jar"
)

DEFAULT_CONFIG_PATH = os.path.join(
    PROJECT_ROOT, "ML_Tuner", "ml_tuner_config.yaml"
)

PRODUCER_CLASS = "org.jzx.producer.SeismicProducer"
PRODUCER_WORK_DIR = PROJECT_ROOT

ROCKETMQ_HOME = "/opt/app/rocketmq-5.3.1"

FLINK_JOB_PARAMS = {
    "p-source": 2,
    "p-filter": 4,
    "p-signal": 4,
    "p-feature": 4,
    "p-grid": 4,
    "p-sink": 2,
}

CLUSTER_NODES = [
    {"host": "192.168.56.151", "name": "node01"},
    {"host": "192.168.56.152", "name": "node02"},
    {"host": "192.168.56.153", "name": "node03"},
]

# 等待时间
WARMUP_SECONDS = 40
METRIC_RETRY_COUNT = 6
METRIC_RETRY_INTERVAL = 10

# ==================== 算子发现 ====================

OPERATOR_KEYWORDS = {
    "source":     "RocketMQ-Source",
    "filter":     "Hardware-Filter",
    "signal":     "Signal-Extraction",
    "feature":    "Feature-Extraction",
    "grid":       "Grid-Aggregation",
    "grid_sink":  "GridSummary-Sink",
    "event_sink": "EventFeature-Sink",
}

# ==================== 要检测的全部延迟指标 ====================

# EventFeature-Sink 上的自定义 Gauge 指标
EVENT_SINK_LATENCY_METRICS = [
    "Sink__EventFeature-Sink.e2e_latency_p99_ms",   # ★ 新增：Source→Sink 端到端延迟 P99
    "Sink__EventFeature-Sink.latency_p99",           # 保留：Sink invoke() 内部处理耗时 P99
]

# Flink 内置 mailboxLatencyMs（所有算子通用）
MAILBOX_METRICS = [
    "mailboxLatencyMs_p50",
    "mailboxLatencyMs_p95",
    "mailboxLatencyMs_p99",
    "mailboxLatencyMs_max",
    "mailboxLatencyMs_mean",
]

# 吞吐量
THROUGHPUT_METRICS = [
    "numRecordsInPerSecond",
    "numRecordsOutPerSecond",
]

# 繁忙率
BUSY_METRICS = [
    "busyTimeMsPerSecond",
    "idleTimeMsPerSecond",
]

# 缓存指标
CACHE_METRICS = {
    "feature": [
        "Feature-Extraction.l2_hit_rate",
        "Feature-Extraction.l2_occupancy",
    ],
    "grid": [
        "Grid-Aggregation.l3_hit_rate",
        "Grid-Aggregation.l3_occupancy",
    ],
}


# ==================== 配置加载 ====================

def load_config(config_path):
    if not os.path.exists(config_path):
        print(f"[ERROR] 配置文件不存在: {config_path}")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    config = {
        "flink_rest_url": cfg["flink"]["rest_url"],
        "flink_home": cfg["flink"]["flink_home"],
        "remote_jar_path": cfg["flink"]["jar_path"],
        "main_class": cfg["flink"]["main_class"],
        "job_name": cfg["flink"]["job_name"],
        "ssh_host": cfg["ssh"]["host"],
        "ssh_port": cfg["ssh"]["port"],
        "ssh_user": cfg["ssh"]["user"],
        "ssh_password": cfg["ssh"]["password"],
    }

    config["flink_log_dir"] = config["flink_home"] + "/log"
    config["flink_bin"] = config["flink_home"] + "/bin/flink"
    config["namesrv_addr"] = f"{cfg['ssh']['host']}:9876"
    config["topic"] = "seismic-raw"
    config["consumer_group"] = "seismic-flink-consumer-group"

    return config


# ==================== 安全类型转换 ====================

def safe_float(val, default=0.0) -> float:
    if val is None:
        return default
    if isinstance(val, str):
        if val.lower() == "nan":
            return default
        try:
            f = float(val)
            return default if math.isnan(f) else f
        except ValueError:
            return default
    try:
        f = float(val)
        return default if math.isnan(f) else f
    except (ValueError, TypeError):
        return default


# ==================== Flink REST 客户端 ====================

class FlinkRESTClient:
    def __init__(self, rest_url):
        self.base_url = rest_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.timeout = 10

    def _get(self, path):
        url = f"{self.base_url}{path}"
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _patch(self, path):
        try:
            resp = requests.patch(f"{self.base_url}{path}", timeout=self.timeout)
            return resp.status_code
        except Exception:
            return None

    def get_overview(self):
        try:
            return self._get("/overview")
        except Exception:
            return None

    def get_running_jobs(self):
        try:
            data = self._get("/jobs/overview")
            return [j for j in data.get("jobs", []) if j.get("state") == "RUNNING"]
        except Exception:
            return []

    def cancel_job(self, job_id):
        print(f"  [REST] 取消 Job: {job_id}")
        self._patch(f"/jobs/{job_id}?mode=cancel")

    def cancel_all_running_jobs(self):
        jobs = self.get_running_jobs()
        if not jobs:
            print("  [REST] 无运行中的 Job")
            return
        for job in jobs:
            self.cancel_job(job["jid"])
        for _ in range(15):
            time.sleep(2)
            if not self.get_running_jobs():
                print("  [REST] 所有 Job 已停止")
                return
        print("  [WARN] 部分 Job 可能仍在运行")

    def wait_for_job_running(self, job_name, timeout=90):
        print(f"  [REST] 等待 Job '{job_name}' 进入 RUNNING...")
        start = time.time()
        while time.time() - start < timeout:
            jobs = self.get_running_jobs()
            for j in jobs:
                if j.get("name", "") == job_name:
                    print(f"  [REST] ✅ Job 已运行: {j['jid']}")
                    return j["jid"]
            time.sleep(3)
        print(f"  [WARN] 等待超时（{timeout}s）")
        return None

    def discover_vertices(self, job_id) -> Dict[str, str]:
        try:
            resp = self._get(f"/jobs/{job_id}")
            vertices = resp.get("vertices", [])
        except Exception as e:
            print(f"  [ERROR] 获取 vertices 失败: {e}")
            return {}

        vertex_map = {}
        for op_key, keyword in OPERATOR_KEYWORDS.items():
            for v in vertices:
                if keyword in v["name"]:
                    vertex_map[op_key] = v["id"]
                    break
        return vertex_map

    def query_subtask_metrics(self, job_id: str, vertex_id: str,
                               metric_names: List[str]) -> Dict[str, Dict[str, float]]:
        result = {}
        try:
            names_str = ",".join(metric_names)
            resp = self._get(
                f"/jobs/{job_id}/vertices/{vertex_id}"
                f"/subtasks/metrics?get={names_str}"
            )
            for item in resp:
                mid = item["id"]
                result[mid] = {
                    "min": safe_float(item.get("min", 0)),
                    "max": safe_float(item.get("max", 0)),
                    "avg": safe_float(item.get("avg", 0)),
                    "sum": safe_float(item.get("sum", 0)),
                }
        except Exception:
            pass
        return result

    def list_subtask_metrics(self, job_id: str, vertex_id: str) -> List[str]:
        try:
            resp = self._get(
                f"/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics"
            )
            return [m["id"] for m in resp]
        except Exception:
            return []

    def get_checkpoint_stats(self, job_id: str) -> dict:
        try:
            return self._get(f"/jobs/{job_id}/checkpoints")
        except Exception:
            return {}

    def close(self):
        self.session.close()


# ==================== SSH 控制器 ====================

class SSHController:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.connected = False

    def connect(self):
        self.client.connect(
            hostname=self.host, port=self.port,
            username=self.user, password=self.password,
        )
        self.connected = True

    def exec_cmd(self, cmd, timeout=30, ignore_error=False):
        stdin, stdout, stderr = self.client.exec_command(cmd, timeout=timeout)
        exit_code = stdout.channel.recv_exit_status()
        out = stdout.read().decode("utf-8").strip()
        err = stderr.read().decode("utf-8").strip()
        if exit_code != 0 and not ignore_error and err:
            print(f"  [WARN] exit={exit_code}, stderr={err[:200]}")
        return out

    def upload_file(self, local_path, remote_path):
        sftp = self.client.open_sftp()
        sftp.put(local_path, remote_path)
        sftp.close()

    def close(self):
        if self.connected:
            self.client.close()
            self.connected = False


class ClusterSSH:
    def __init__(self, config):
        self.config = config
        self.connections = {}

    def connect_all(self):
        print("[SSH] 连接集群所有节点...")
        for node in CLUSTER_NODES:
            ssh = SSHController(
                host=node["host"], port=self.config["ssh_port"],
                user=self.config["ssh_user"], password=self.config["ssh_password"],
            )
            try:
                ssh.connect()
                self.connections[node["name"]] = ssh
                print(f"  ✅ {node['name']} ({node['host']}) 已连接")
            except Exception as e:
                print(f"  ❌ {node['name']} ({node['host']}) 连接失败: {e}")

    def get_master(self):
        return self.connections.get("node01")

    def restart_flink_cluster(self, config, flink_client):
        master = self.get_master()
        if not master:
            return False

        flink_home = config["flink_home"]

        print("[STEP] 停止 Flink 集群...")
        master.exec_cmd(f"{flink_home}/bin/stop-cluster.sh",
                        timeout=30, ignore_error=True)
        time.sleep(5)

        print("[STEP] 启动 Flink 集群...")
        master.exec_cmd(f"{flink_home}/bin/start-cluster.sh",
                        timeout=30, ignore_error=True)

        print("[STEP] 等待 TaskManager 注册...")
        for attempt in range(20):
            time.sleep(3)
            overview = flink_client.get_overview()
            if overview:
                tm_count = overview.get("taskmanagers", 0)
                slots_total = overview.get("slots-total", 0)
                print(f"  第{attempt + 1}次检查: TM={tm_count}, Slots={slots_total}")
                if tm_count >= 3 and slots_total >= 12:
                    print(f"  ✅ Flink 集群就绪: {tm_count} TM, {slots_total} Slots")
                    return True

        print("  [WARN] 等待 TM 注册超时")
        return False

    def upload_jar_to_master(self, local_jar, remote_jar):
        master = self.get_master()
        if not master:
            print("  [ERROR] 无法获取主节点")
            return False

        jar_mb = os.path.getsize(local_jar) / 1024 / 1024
        print(f"[STEP] 上传 JAR 到虚拟机主节点...")
        print(f"  本地: {local_jar}")
        print(f"  远程: {remote_jar}")
        print(f"  大小: {jar_mb:.1f} MB")

        start = time.time()
        try:
            master.upload_file(local_jar, remote_jar)
            elapsed = time.time() - start
            print(f"  ✅ 上传完成 ({elapsed:.1f}s, {jar_mb / elapsed:.1f} MB/s)")
            return True
        except Exception as e:
            print(f"  ❌ 上传失败: {e}")
            return False

    def close_all(self):
        for name, ssh in self.connections.items():
            ssh.close()
        self.connections.clear()


# ==================== 本地 Producer ====================

class LocalProducer:
    def __init__(self, jar_path, work_dir):
        self.jar_path = jar_path
        self.work_dir = work_dir
        self.process = None
        self.log_file_handle = None

    def start(self):
        print("[STEP] 启动 Producer（宿主机本地）...")
        if not os.path.exists(self.jar_path):
            print(f"  [ERROR] JAR 不存在: {self.jar_path}")
            return False

        log_path = os.path.join(
            FINAL_TEST_DIR,
            f"producer_check_{datetime.now().strftime('%H%M%S')}.log"
        )
        self.log_file_handle = open(log_path, "w", encoding="utf-8")

        cmd = ["java", "-cp", self.jar_path, PRODUCER_CLASS]
        self.process = subprocess.Popen(
            cmd, cwd=self.work_dir,
            stdout=self.log_file_handle, stderr=subprocess.STDOUT,
        )
        time.sleep(3)

        if self.process.poll() is not None:
            print(f"  [ERROR] Producer 启动后退出, exit_code={self.process.returncode}")
            print(f"  查看日志: {log_path}")
            return False

        print(f"  ✅ Producer 已启动, PID={self.process.pid}")
        print(f"     日志: {log_path}")
        return True

    def stop(self):
        if self.process and self.process.poll() is None:
            print("[STEP] 停止 Producer...")
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
                print(f"  ✅ Producer 已停止")
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
                print(f"  ✅ Producer 已强制停止")

        if self.log_file_handle:
            self.log_file_handle.close()
            self.log_file_handle = None

    def is_running(self):
        return self.process is not None and self.process.poll() is None


# ==================== 工具函数 ====================

def reset_rocketmq(master_ssh, config):
    print("[STEP] 重置 RocketMQ 消费进度...")
    master_ssh.exec_cmd(
        f"{ROCKETMQ_HOME}/bin/mqadmin resetOffsetByTime "
        f"-n {config['namesrv_addr']} -t {config['topic']} "
        f"-g {config['consumer_group']} -s -1",
        timeout=15, ignore_error=True
    )
    time.sleep(2)


def submit_flink_job(master_ssh, flink_client, config):
    print("[STEP] 提交 Flink Job...")
    params_str = " ".join(f"--{k} {v}" for k, v in FLINK_JOB_PARAMS.items())
    cmd = (f"{config['flink_bin']} run -d "
           f"-c {config['main_class']} {config['remote_jar_path']} "
           f"{params_str}")
    output = master_ssh.exec_cmd(cmd, timeout=60)
    print(f"  提交输出: {output[:300]}")
    return flink_client.wait_for_job_running(config["job_name"], timeout=90)


def print_progress(phase, total_seconds):
    interval = 5 if total_seconds <= 30 else 10
    for remaining in range(total_seconds, 0, -interval):
        elapsed = total_seconds - remaining
        bar_len = 30
        filled = int(bar_len * elapsed / total_seconds)
        bar = "█" * filled + "░" * (bar_len - filled)
        print(f"\r  [{phase}] {bar} {remaining:>4d}s 剩余",
              end="", flush=True)
        time.sleep(min(interval, remaining))
    print(f"\r  [{phase}] {'█' * 30} 完成!{'':>20s}")


# ==================== 核心检测逻辑 ====================

def wait_for_data_flowing(flink_client: FlinkRESTClient, job_id: str,
                           vertex_map: Dict[str, str]) -> bool:
    """等待数据流通：Source 输出速率 > 0 且 EventFeature-Sink 输入速率 > 0"""
    source_vid = vertex_map.get("source")
    sink_vid = vertex_map.get("event_sink")

    if not source_vid:
        print("  [WARN] 未找到 Source vertex")
        return False

    print(f"\n[STEP] 等待数据流通各算子"
          f"（最多 {METRIC_RETRY_COUNT} 次, 间隔 {METRIC_RETRY_INTERVAL}s）...")

    for attempt in range(1, METRIC_RETRY_COUNT + 1):
        # 查 Source 输出
        src_data = flink_client.query_subtask_metrics(
            job_id, source_vid, ["numRecordsOutPerSecond"]
        )
        src_rate = src_data.get("numRecordsOutPerSecond", {}).get("sum", 0.0)

        # 查 Sink 输入
        sink_rate = 0.0
        if sink_vid:
            sink_data = flink_client.query_subtask_metrics(
                job_id, sink_vid, ["numRecordsInPerSecond"]
            )
            sink_rate = sink_data.get("numRecordsInPerSecond", {}).get("sum", 0.0)

        print(f"  第 {attempt}/{METRIC_RETRY_COUNT} 次: "
              f"Source.out={src_rate:.0f}/s  Sink.in={sink_rate:.0f}/s")

        if src_rate > 0 and sink_rate > 0:
            print(f"  ✅ 数据已流通全链路")
            return True

        if attempt < METRIC_RETRY_COUNT:
            time.sleep(METRIC_RETRY_INTERVAL)

    if src_rate > 0:
        print(f"  ⚠️ Source 有输出但 Sink 无输入，可能链路尚未完全打通")
        return True  # Source 有数据就继续检测

    print(f"  ❌ 数据未流动")
    return False


def check_all_metrics(flink_client: FlinkRESTClient, job_id: str,
                       vertex_map: Dict[str, str]) -> dict:
    """全面检测延迟、吞吐量、缓存、繁忙率指标"""

    report = {
        "e2e_latency": {},          # ★ 新增：端到端延迟
        "sink_latency": {},         # Sink 内部处理耗时
        "mailbox_latency": {},      # mailboxLatencyMs
        "throughput": {},
        "busy_ratio": {},
        "cache": {},
        "checkpoint": {},
        "data_flowing": False,
        "has_e2e_latency": False,   # ★ 核心判断
        "has_sink_latency": False,
        "has_mailbox_latency": False,
        "sink_all_metrics": [],
    }

    print()
    print("═" * 65)
    print("  全量指标检测报告")
    print("═" * 65)

    event_sink_vid = vertex_map.get("event_sink")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 1. ★ 核心：端到端延迟 + Sink 内部耗时
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print("\n┌─ [1/7] ★ 延迟指标（核心）")
    print("│")

    if event_sink_vid:
        data = flink_client.query_subtask_metrics(
            job_id, event_sink_vid, EVENT_SINK_LATENCY_METRICS
        )

        # 端到端延迟
        e2e_key = "Sink__EventFeature-Sink.e2e_latency_p99_ms"
        e2e_data = data.get(e2e_key, {})
        if e2e_data and e2e_data.get("max", 0) > 0:
            report["has_e2e_latency"] = True
            report["e2e_latency"] = e2e_data
            print(f"│  ✅ {e2e_key}")
            print(f"│     min  = {e2e_data.get('min', 0):>10.2f} ms")
            print(f"│     avg  = {e2e_data.get('avg', 0):>10.2f} ms")
            print(f"│     max  = {e2e_data.get('max', 0):>10.2f} ms ← 最差 subtask P99")
            print(f"│")
            print(f"│     含义: Source 反序列化 → Filter → Signal → Feature(L2缓存)")
            print(f"│           → Grid(L3缓存) → Sink 写MQ 的完整处理链路延迟")
        elif e2e_data:
            report["e2e_latency"] = e2e_data
            print(f"│  ⚠️ {e2e_key}: 值为 0（可能尚未累积足够数据）")
            print(f"│     raw = {e2e_data}")
        else:
            print(f"│  ❌ {e2e_key}: 指标不存在")
            print(f"│     → 确认 EventFeatureSink.java 中已注册 e2e_latency_p99_ms Gauge")
            print(f"│     → 确认 Proto 已加 source_processing_time_ms 字段并重新编译")

        print(f"│")

        # Sink 内部处理耗时
        sink_key = "Sink__EventFeature-Sink.latency_p99"
        sink_data = data.get(sink_key, {})
        if sink_data and sink_data.get("max", 0) > 0:
            report["has_sink_latency"] = True
            report["sink_latency"] = sink_data
            print(f"│  ✅ {sink_key}")
            print(f"│     min  = {sink_data.get('min', 0):>10.2f} μs")
            print(f"│     avg  = {sink_data.get('avg', 0):>10.2f} μs")
            print(f"│     max  = {sink_data.get('max', 0):>10.2f} μs ← Sink invoke() 内部耗时")
        elif sink_data:
            report["sink_latency"] = sink_data
            print(f"│  ⚠️ {sink_key}: 值为 0")
        else:
            print(f"│  ❌ {sink_key}: 指标不存在")
    else:
        print(f"│  ❌ EventFeature-Sink vertex 未找到")

    print("│")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 2. Flink 内置 mailboxLatencyMs
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print("├─ [2/7] Flink 内置 mailboxLatencyMs")
    print("│")

    mailbox_results = {}
    for op_key, vid in vertex_map.items():
        data = flink_client.query_subtask_metrics(job_id, vid, MAILBOX_METRICS)
        if data:
            mailbox_results[op_key] = data
            p99 = data.get("mailboxLatencyMs_p99", {})
            mx = data.get("mailboxLatencyMs_max", {})
            mean = data.get("mailboxLatencyMs_mean", {})
            op_name = op_key.replace("_", " ").title()
            print(f"│  {op_name:<16s}  "
                  f"mean={mean.get('avg', 0):>8.2f}ms  "
                  f"p99={p99.get('avg', 0):>8.2f}ms  "
                  f"max={mx.get('max', 0):>8.2f}ms")

    if mailbox_results:
        report["has_mailbox_latency"] = True
        report["mailbox_latency"] = mailbox_results

    if not mailbox_results:
        print(f"│  （所有算子均无 mailbox 指标返回）")

    print("│")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 3. 各算子吞吐量
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print("├─ [3/7] 各算子吞吐量")
    print("│")

    for op_key, vid in vertex_map.items():
        data = flink_client.query_subtask_metrics(job_id, vid, THROUGHPUT_METRICS)
        in_rate = data.get("numRecordsInPerSecond", {}).get("sum", 0)
        out_rate = data.get("numRecordsOutPerSecond", {}).get("sum", 0)
        report["throughput"][op_key] = {"in": in_rate, "out": out_rate}

        if in_rate > 0 or out_rate > 0:
            report["data_flowing"] = True

        op_name = op_key.replace("_", " ").title()
        print(f"│  {op_name:<16s}  in={in_rate:>8.0f}/s  out={out_rate:>8.0f}/s")

    print("│")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 4. 各算子繁忙率
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print("├─ [4/7] 各算子繁忙率")
    print("│")

    for op_key, vid in vertex_map.items():
        if op_key == "source":
            continue

        data = flink_client.query_subtask_metrics(job_id, vid, BUSY_METRICS)
        busy = data.get("busyTimeMsPerSecond", {})
        idle = data.get("idleTimeMsPerSecond", {})

        busy_avg = busy.get("avg", 0) / 1000.0
        busy_max = busy.get("max", 0) / 1000.0
        idle_avg = idle.get("avg", 0) / 1000.0
        bp_ratio = max(0, 1.0 - busy_avg - idle_avg)

        report["busy_ratio"][op_key] = {
            "busy_avg": busy_avg,
            "busy_max": busy_max,
            "backpressure": bp_ratio,
        }

        op_name = op_key.replace("_", " ").title()
        print(f"│  {op_name:<16s}  "
              f"busy_avg={busy_avg:>5.1%}  "
              f"busy_max={busy_max:>5.1%}  "
              f"bp={bp_ratio:>5.1%}")

    print("│")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 5. 缓存指标
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print("├─ [5/7] 缓存指标")
    print("│")

    for op_key, metrics in CACHE_METRICS.items():
        vid = vertex_map.get(op_key)
        if not vid:
            continue

        data = flink_client.query_subtask_metrics(job_id, vid, metrics)
        cache_info = {}
        for m in metrics:
            val = data.get(m, {}).get("avg", 0)
            short_name = m.split(".")[-1]
            cache_info[short_name] = val

        report["cache"][op_key] = cache_info

        op_name = op_key.replace("_", " ").title()
        parts = "  ".join(f"{k}={v:.4f}" for k, v in cache_info.items())
        print(f"│  {op_name:<16s}  {parts}")

    print("│")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 6. Checkpoint 状态
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print("├─ [6/7] Checkpoint 状态")
    print("│")

    ckp = flink_client.get_checkpoint_stats(job_id)
    if ckp:
        counts = ckp.get("counts", {})
        latest = ckp.get("latest", {}).get("completed", {})

        report["checkpoint"] = {
            "completed": counts.get("completed", 0),
            "failed": counts.get("failed", 0),
        }

        if latest:
            dur = latest.get("duration", 0)
            size = latest.get("state_size", 0)
            report["checkpoint"]["latest_duration_ms"] = dur
            report["checkpoint"]["latest_size_bytes"] = size
            print(f"│  ✅ 已完成 {counts.get('completed', 0)} 次")
            print(f"│     最近: 耗时={dur}ms, 大小={size / 1024:.1f}KB")
        else:
            print(f"│  ⚠️ 尚无已完成的 Checkpoint")
            print(f"│     统计: {json.dumps(counts)}")
    else:
        print(f"│  ❌ 无法查询 Checkpoint")

    print("│")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 7. Sink 完整指标扫描（发现隐藏指标）
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print("├─ [7/7] EventFeature-Sink 完整指标扫描")
    print("│")

    if event_sink_vid:
        all_names = flink_client.list_subtask_metrics(job_id, event_sink_vid)
        report["sink_all_metrics"] = all_names

        # 分类显示
        latency_names = [n for n in all_names if "latency" in n.lower()
                         or "e2e" in n.lower()]
        cache_names = [n for n in all_names if "cache" in n.lower()
                       or "hit" in n.lower() or "occupancy" in n.lower()]

        print(f"│  共 {len(all_names)} 个指标")
        print(f"│")

        if latency_names:
            print(f"│  延迟相关 ({len(latency_names)} 个):")
            for n in sorted(latency_names):
                marker = " ★ 新增" if "e2e" in n.lower() else ""
                print(f"│    ◀ {n}{marker}")

        other_count = len(all_names) - len(latency_names) - len(cache_names)
        if other_count > 0:
            print(f"│    ... 其余 {other_count} 个指标省略")
    else:
        print(f"│  EventFeature-Sink 未找到")

    print("│")
    print("└─ 扫描完成")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 结论
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print()
    print("═" * 65)
    print("  结论")
    print("═" * 65)

    print(f"\n  数据是否在流动:              {'✅ 是' if report['data_flowing'] else '❌ 否'}")
    print(f"  ★ e2e_latency_p99_ms:        {'✅ 可用' if report['has_e2e_latency'] else '❌ 不可用'}")
    print(f"    latency_p99 (Sink内部):    {'✅ 可用' if report['has_sink_latency'] else '❌ 不可用'}")
    print(f"    mailboxLatencyMs:          {'✅ 可用' if report['has_mailbox_latency'] else '❌ 不可用'}")

    if report["has_e2e_latency"]:
        e2e_max = report["e2e_latency"].get("max", 0)
        e2e_avg = report["e2e_latency"].get("avg", 0)
        print(f"\n  ★ 端到端延迟 P99: {e2e_max:.2f} ms (avg={e2e_avg:.2f} ms)")
        print(f"    测量范围: Source反序列化 → Filter → Signal → Feature(L2) → Sink写MQ")

    if report["has_sink_latency"]:
        sink_max = report["sink_latency"].get("max", 0)
        print(f"    Sink内部耗时 P99: {sink_max:.2f} μs")

    print()

    # 判断核心结论
    if report["has_e2e_latency"] and report["data_flowing"]:
        print("  ╔══════════════════════════════════════════════════════════════╗")
        print("  ║  ✅ 核心结论：端到端延迟指标 e2e_latency_p99_ms 已可用     ║")
        print("  ║                                                            ║")
        print("  ║  采集方式:                                                 ║")
        print("  ║    端点: /jobs/{jid}/vertices/{sink_vid}/subtasks/metrics   ║")
        print("  ║    参数: ?get=Sink__EventFeature-Sink.e2e_latency_p99_ms   ║")
        print("  ║    取值: response.max = 所有 subtask 中最差的 P99          ║")
        print("  ║                                                            ║")
        print("  ║  同时可用:                                                 ║")
        print("  ║    Sink__EventFeature-Sink.latency_p99  (Sink内部耗时)     ║")
        print("  ║                                                            ║")
        print("  ║  → 可直接进入基准性能测量阶段                               ║")
        print("  ╚══════════════════════════════════════════════════════════════╝")

    elif report["data_flowing"] and report["has_sink_latency"]:
        print("  ╔══════════════════════════════════════════════════════════════╗")
        print("  ║  ⚠️ e2e_latency_p99_ms 不可用，但 latency_p99 可用        ║")
        print("  ║                                                            ║")
        print("  ║  可能原因:                                                 ║")
        print("  ║  1. Proto 未加 source_processing_time_ms 字段              ║")
        print("  ║  2. Source 未打戳（setSourceProcessingTimeMs）              ║")
        print("  ║  3. Signal/Feature 未透传该字段                            ║")
        print("  ║  4. EventFeatureSink 未注册 e2e_latency_p99_ms Gauge       ║")
        print("  ║  5. JAR 未重新编译/上传                                    ║")
        print("  ║                                                            ║")
        print("  ║  排查: 检查 Sink 的完整指标列表中是否有 e2e 相关指标        ║")
        print("  ╚══════════════════════════════════════════════════════════════╝")

    elif report["data_flowing"]:
        print("  ╔══════════════════════════════════════════════════════════════╗")
        print("  ║  ❌ 数据在流动，但无任何延迟指标可用                        ║")
        print("  ║  → 增加预热时间 (--warmup 60) 重试                         ║")
        print("  ║  → 或检查 Java 代码中 Gauge 注册逻辑                        ║")
        print("  ╚══════════════════════════════════════════════════════════════╝")
    else:
        print("  ╔══════════════════════════════════════════════════════════════╗")
        print("  ║  ❌ 数据未流动，所有指标值不可靠                            ║")
        print("  ║  → 检查 Producer 日志                                      ║")
        print("  ║  → 检查 RocketMQ 连通性                                    ║")
        print("  ║  → 增加预热时间 (--warmup 60)                              ║")
        print("  ╚══════════════════════════════════════════════════════════════╝")

    return report


# ==================== 主流程 ====================

def main():
    parser = argparse.ArgumentParser(
        description="端到端延迟指标可用性检测（v3 — 含 e2e_latency_p99_ms）"
    )
    parser.add_argument("--config", type=str, default=DEFAULT_CONFIG_PATH,
                        help="ml_tuner_config.yaml 路径")
    parser.add_argument("--jar", type=str, default=None,
                        help="覆盖宿主机本地 JAR 路径")
    parser.add_argument("--warmup", type=int, default=WARMUP_SECONDS,
                        help=f"预热等待秒数（默认 {WARMUP_SECONDS}）")
    parser.add_argument("--skip-restart", action="store_true",
                        help="跳过 Flink 集群重启")
    parser.add_argument("--skip-producer", action="store_true",
                        help="跳过 Producer 启动")
    parser.add_argument("--skip-upload", action="store_true",
                        help="跳过 JAR 上传（虚拟机上已有最新 JAR）")
    args = parser.parse_args()

    local_jar = args.jar or LOCAL_JAR_PATH
    warmup = args.warmup

    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║    端到端延迟指标可用性检测 v3                                ║")
    print("║    ★ 重点检测: e2e_latency_p99_ms（Source→Sink 完整链路）    ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║  时间:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<49s}  ║")
    print(f"║  配置:  {os.path.basename(args.config):<49s}  ║")
    print(f"║  预热:  {warmup}s{'':>52s}║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()

    # 加载配置
    print(f"[STEP] 加载配置: {args.config}")
    config = load_config(args.config)

    # 前置检查
    print("\n[CHECK] 前置检查...")

    if not os.path.exists(local_jar):
        print(f"  ❌ 宿主机 JAR 不存在: {local_jar}")
        print(f"     请先执行: mvn clean package -DskipTests")
        sys.exit(1)
    jar_mb = os.path.getsize(local_jar) / 1024 / 1024
    jar_mtime = datetime.fromtimestamp(
        os.path.getmtime(local_jar)
    ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"  ✅ JAR: {os.path.basename(local_jar)}")
    print(f"     大小: {jar_mb:.1f} MB, 修改时间: {jar_mtime}")

    try:
        java_ver = subprocess.check_output(
            ["java", "-version"], stderr=subprocess.STDOUT
        ).decode().split("\n")[0]
        print(f"  ✅ Java: {java_ver}")
    except Exception:
        print("  ❌ java 不可用")
        sys.exit(1)

    dataset_dir = os.path.join(PRODUCER_WORK_DIR, "dataset")
    if os.path.isdir(dataset_dir):
        fc = len([f for f in os.listdir(dataset_dir)
                  if os.path.isdir(os.path.join(dataset_dir, f))])
        print(f"  ✅ dataset: {fc} 个节点文件夹")

    # 初始化
    flink_client = FlinkRESTClient(config["flink_rest_url"])
    cluster = ClusterSSH(config)
    producer = LocalProducer(local_jar, PRODUCER_WORK_DIR)

    report = None

    try:
        cluster.connect_all()
        master = cluster.get_master()
        if not master:
            print("[ERROR] 无法连接主节点 node01")
            sys.exit(1)

        # Step 0: 上传 JAR
        if not args.skip_upload:
            print()
            if not cluster.upload_jar_to_master(local_jar, config["remote_jar_path"]):
                print("[ERROR] JAR 上传失败")
                sys.exit(1)
        else:
            # 仍然检查远程 JAR 是否存在
            jar_check = master.exec_cmd(
                f"test -f {config['remote_jar_path']} && echo OK || echo MISSING",
                timeout=5, ignore_error=True
            )
            if "MISSING" in jar_check:
                print(f"  ❌ 虚拟机 JAR 不存在: {config['remote_jar_path']}")
                print(f"     去掉 --skip-upload 重新运行")
                sys.exit(1)
            print(f"  ✅ 虚拟机 JAR 已确认（跳过上传）")

        # Step 1: 清理残留 + 重启集群
        if not args.skip_restart:
            print()
            flink_client.cancel_all_running_jobs()
            time.sleep(3)
            if not cluster.restart_flink_cluster(config, flink_client):
                print("[ERROR] Flink 集群启动失败")
                sys.exit(1)
        else:
            print("\n[SKIP] 跳过集群重启")
            flink_client.cancel_all_running_jobs()
            time.sleep(3)

        # Step 2: 重置 RocketMQ
        print()
        reset_rocketmq(master, config)

        # Step 3: 提交 Job
        print()
        job_id = submit_flink_job(master, flink_client, config)
        if not job_id:
            print("[ERROR] Flink Job 未能启动")
            sys.exit(1)

        # Step 4: 发现 vertices
        print("\n[STEP] 发现算子 vertices...")
        vertex_map = flink_client.discover_vertices(job_id)
        for op_key, vid in vertex_map.items():
            print(f"  {op_key:<16s} → {vid[:16]}...")
        print(f"  共发现 {len(vertex_map)}/{len(OPERATOR_KEYWORDS)} 个算子")

        # Step 5: 启动 Producer
        if not args.skip_producer:
            print()
            if not producer.start():
                print("[ERROR] Producer 启动失败")
                flink_client.cancel_all_running_jobs()
                sys.exit(1)
        else:
            print("\n[SKIP] 跳过 Producer 启动")

        # Step 6: 预热
        print(f"\n[PHASE] 预热等待 {warmup}s...")
        print_progress("预热", warmup)

        # Step 7: 等待数据流通
        data_ok = wait_for_data_flowing(flink_client, job_id, vertex_map)

        # Step 8: 执行全量检测
        print()
        report = check_all_metrics(flink_client, job_id, vertex_map)

        # Step 9: 保存报告
        report_path = os.path.join(
            FINAL_TEST_DIR,
            f"latency_check_v3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        if report:
            def to_serializable(obj):
                if isinstance(obj, dict):
                    return {k: to_serializable(v) for k, v in obj.items()}
                elif isinstance(obj, (list, tuple)):
                    return [to_serializable(i) for i in obj]
                elif isinstance(obj, float):
                    if math.isnan(obj) or math.isinf(obj):
                        return 0.0
                    return obj
                return obj

            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(to_serializable(report), f, indent=2, ensure_ascii=False)
            print(f"\n  📄 报告已保存: {report_path}")

    except KeyboardInterrupt:
        print("\n\n[中断] 用户取消...")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n" + "═" * 65)
        print("  清理...")
        print("═" * 65)
        producer.stop()
        try:
            flink_client.cancel_all_running_jobs()
        except Exception:
            pass
        flink_client.close()
        cluster.close_all()
        print("\n  ✅ 清理完成")


if __name__ == "__main__":
    main()
