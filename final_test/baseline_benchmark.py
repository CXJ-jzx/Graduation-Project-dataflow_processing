#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
final_test/baseline_benchmark.py

基准性能测量脚本（Baseline Benchmark）

功能:
  对 5 种缓存策略（LRU / LRU-K / LFU / FIFO / W-TinyLFU），
  在固定默认并行度下逐一运行，采集完整指标快照，建立调优前的对比基线。

自动化流程（每种策略）:
  1. 重启 Flink 集群
  2. 重置 RocketMQ 消费进度
  3. 提交 Job（--cache-strategy {strategy}）
  4. 启动 Producer
  5. 预热 warmup_seconds
  6. 稳态采集 steady_seconds（每 sample_interval 秒采样一次）
  7. 停止 Producer + 取消 Job
  8. 保存该策略的完整指标快照

最终输出:
  - 每种策略的详细 JSON 报告
  - 5 策略汇总对比表（控制台 + CSV）

使用方式:
  cd E:\\Desktop\\bs_code\\flink-rocketmq-demo\\final_test
  python baseline_benchmark.py
  python baseline_benchmark.py --strategies lru,fifo          # 只跑部分策略
  python baseline_benchmark.py --warmup 90 --steady 180       # 调整时间
  python baseline_benchmark.py --skip-upload                  # JAR已在虚拟机
  python baseline_benchmark.py --output-dir ./results         # 指定输出目录
"""

import os
import sys
import math
import time
import json
import csv
import subprocess
import argparse
import requests
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any

try:
    import yaml
    import paramiko
except ImportError:
    print("请先安装依赖: pip install pyyaml paramiko requests numpy")
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

CLUSTER_NODES = [
    {"host": "192.168.56.151", "name": "node01"},
    {"host": "192.168.56.152", "name": "node02"},
    {"host": "192.168.56.153", "name": "node03"},
]

# ==================== 实验配置 ====================

# 5 种缓存策略
ALL_STRATEGIES = ["lru", "lru-k", "lfu", "fifo", "w-tinylfu"]

# 固定默认并行度（基线配置）
BASELINE_PARALLELISM = {
    "p-source": 2,
    "p-filter": 4,
    "p-signal": 4,
    "p-feature": 4,
    "p-grid": 4,
    "p-sink": 2,
}

# 采集时间参数（秒）
DEFAULT_WARMUP = 60        # 预热：等待数据流通 + 缓存预热
DEFAULT_STEADY = 120       # 稳态采集持续时间
DEFAULT_SAMPLE_INTERVAL = 10  # 采样间隔

# 数据流通检测
DATA_FLOW_RETRY_COUNT = 8
DATA_FLOW_RETRY_INTERVAL = 10

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

# ==================== 要采集的全部指标 ====================

# EventFeature-Sink 延迟指标
EVENT_SINK_LATENCY_METRICS = [
    "Sink__EventFeature-Sink.e2e_latency_p99_ms",
    "Sink__EventFeature-Sink.latency_p99",
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


def to_serializable(obj):
    """递归转换为 JSON 可序列化格式"""
    if isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [to_serializable(i) for i in obj]
    elif isinstance(obj, (np.floating, np.integer)):
        return float(obj)
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return 0.0
        return obj
    return obj


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

    config["flink_bin"] = config["flink_home"] + "/bin/flink"
    config["namesrv_addr"] = f"{cfg['ssh']['host']}:9876"
    config["topic"] = "seismic-raw"
    config["consumer_group"] = "seismic-flink-consumer-group"

    return config


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
        self._patch(f"/jobs/{job_id}?mode=cancel")

    def cancel_all_running_jobs(self):
        jobs = self.get_running_jobs()
        for job in jobs:
            self.cancel_job(job["jid"])
        for _ in range(15):
            time.sleep(2)
            if not self.get_running_jobs():
                return True
        return False

    def wait_for_job_running(self, job_name, timeout=90):
        start = time.time()
        while time.time() - start < timeout:
            jobs = self.get_running_jobs()
            for j in jobs:
                if j.get("name", "") == job_name:
                    return j["jid"]
            time.sleep(3)
        return None

    def discover_vertices(self, job_id) -> Dict[str, str]:
        try:
            resp = self._get(f"/jobs/{job_id}")
            vertices = resp.get("vertices", [])
        except Exception:
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

    def get_checkpoint_stats(self, job_id: str) -> dict:
        try:
            return self._get(f"/jobs/{job_id}/checkpoints")
        except Exception:
            return {}

    def get_tm_heap_usage(self) -> float:
        try:
            resp = self._get("/taskmanagers")
            tms = resp.get("taskmanagers", [])
            if not tms:
                return 0.0

            ratios = []
            for tm in tms:
                tmid = tm["id"]
                try:
                    metrics = self._get(
                        f"/taskmanagers/{tmid}/metrics"
                        f"?get=Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Max"
                    )
                    used = max_val = 0
                    for m in metrics:
                        if m["id"] == "Status.JVM.Memory.Heap.Used":
                            used = safe_float(m.get("value", 0))
                        elif m["id"] == "Status.JVM.Memory.Heap.Max":
                            max_val = safe_float(m.get("value", 0))
                    if max_val > 0:
                        ratios.append(used / max_val)
                except Exception:
                    pass

            return float(np.mean(ratios)) if ratios else 0.0
        except Exception:
            return 0.0

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
            pass  # 静默
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
        for node in CLUSTER_NODES:
            ssh = SSHController(
                host=node["host"], port=self.config["ssh_port"],
                user=self.config["ssh_user"], password=self.config["ssh_password"],
            )
            try:
                ssh.connect()
                self.connections[node["name"]] = ssh
                print(f"  ✅ {node['name']} ({node['host']})")
            except Exception as e:
                print(f"  ❌ {node['name']} ({node['host']}) 失败: {e}")

    def get_master(self):
        return self.connections.get("node01")

    def restart_flink_cluster(self, config, flink_client):
        master = self.get_master()
        if not master:
            return False

        flink_home = config["flink_home"]
        master.exec_cmd(f"{flink_home}/bin/stop-cluster.sh", timeout=30, ignore_error=True)
        time.sleep(5)
        master.exec_cmd(f"{flink_home}/bin/start-cluster.sh", timeout=30, ignore_error=True)

        for attempt in range(20):
            time.sleep(3)
            overview = flink_client.get_overview()
            if overview:
                tm = overview.get("taskmanagers", 0)
                slots = overview.get("slots-total", 0)
                if tm >= 3 and slots >= 12:
                    print(f"    集群就绪: {tm} TM, {slots} Slots")
                    return True
        print("    [WARN] 集群启动超时")
        return False

    def upload_jar(self, local_jar, remote_jar):
        master = self.get_master()
        if not master:
            return False
        try:
            master.upload_file(local_jar, remote_jar)
            return True
        except Exception as e:
            print(f"    上传失败: {e}")
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
        self.log_path = None

    def start(self, strategy_name=""):
        self.log_path = os.path.join(
            FINAL_TEST_DIR,
            f"producer_{strategy_name}_{datetime.now().strftime('%H%M%S')}.log"
        )
        self.log_file_handle = open(self.log_path, "w", encoding="utf-8")

        cmd = ["java", "-cp", self.jar_path, PRODUCER_CLASS]
        self.process = subprocess.Popen(
            cmd, cwd=self.work_dir,
            stdout=self.log_file_handle, stderr=subprocess.STDOUT,
        )
        time.sleep(3)

        if self.process.poll() is not None:
            return False
        return True

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.log_file_handle:
            self.log_file_handle.close()
            self.log_file_handle = None

    def is_running(self):
        return self.process is not None and self.process.poll() is None


# ==================== 指标采集器 ====================

class MetricsCollector:
    """单次指标快照采集"""

    def __init__(self, flink_client: FlinkRESTClient, job_id: str,
                 vertex_map: Dict[str, str]):
        self.client = flink_client
        self.job_id = job_id
        self.vertex_map = vertex_map

    def collect_snapshot(self) -> Dict[str, float]:
        """采集一次完整指标快照，返回扁平化的 dict"""
        snapshot = {}

        # 1. 端到端延迟
        event_sink_vid = self.vertex_map.get("event_sink")
        if event_sink_vid:
            data = self.client.query_subtask_metrics(
                self.job_id, event_sink_vid, EVENT_SINK_LATENCY_METRICS
            )
            e2e = data.get("Sink__EventFeature-Sink.e2e_latency_p99_ms", {})
            snapshot["e2e_latency_p99_ms"] = e2e.get("max", 0.0)
            snapshot["e2e_latency_avg_ms"] = e2e.get("avg", 0.0)

            sink_lat = data.get("Sink__EventFeature-Sink.latency_p99", {})
            snapshot["sink_latency_p99_us"] = sink_lat.get("max", 0.0)

        # 2. 吞吐量（Source 输出）
        source_vid = self.vertex_map.get("source")
        if source_vid:
            data = self.client.query_subtask_metrics(
                self.job_id, source_vid, THROUGHPUT_METRICS
            )
            snapshot["source_out_rate"] = data.get(
                "numRecordsOutPerSecond", {}
            ).get("sum", 0.0)

        # EventFeature-Sink 输入
        if event_sink_vid:
            data = self.client.query_subtask_metrics(
                self.job_id, event_sink_vid, THROUGHPUT_METRICS
            )
            snapshot["event_sink_in_rate"] = data.get(
                "numRecordsInPerSecond", {}
            ).get("sum", 0.0)

        # GridSummary-Sink 输入
        grid_sink_vid = self.vertex_map.get("grid_sink")
        if grid_sink_vid:
            data = self.client.query_subtask_metrics(
                self.job_id, grid_sink_vid, THROUGHPUT_METRICS
            )
            snapshot["grid_sink_in_rate"] = data.get(
                "numRecordsInPerSecond", {}
            ).get("sum", 0.0)

        # 3. 繁忙率（各算子）
        busy_avgs = []
        busy_maxes = []
        bp_ratios = []

        for op_key, vid in self.vertex_map.items():
            if op_key == "source":
                continue
            data = self.client.query_subtask_metrics(
                self.job_id, vid, BUSY_METRICS
            )
            busy = data.get("busyTimeMsPerSecond", {})
            idle = data.get("idleTimeMsPerSecond", {})

            b_avg = busy.get("avg", 0) / 1000.0
            b_max = busy.get("max", 0) / 1000.0
            i_avg = idle.get("avg", 0) / 1000.0
            bp = max(0, 1.0 - b_avg - i_avg)

            snapshot[f"{op_key}_busy_avg"] = b_avg
            snapshot[f"{op_key}_busy_max"] = b_max
            snapshot[f"{op_key}_bp_ratio"] = bp

            busy_avgs.append(b_avg)
            busy_maxes.append(b_max)
            bp_ratios.append(bp)

        snapshot["avg_busy_ratio"] = float(np.mean(busy_avgs)) if busy_avgs else 0.0
        snapshot["max_busy_ratio"] = float(max(busy_maxes)) if busy_maxes else 0.0
        snapshot["max_bp_ratio"] = float(max(bp_ratios)) if bp_ratios else 0.0

        # 4. 缓存指标
        for op_key, metrics in CACHE_METRICS.items():
            vid = self.vertex_map.get(op_key)
            if not vid:
                continue
            data = self.client.query_subtask_metrics(self.job_id, vid, metrics)
            for m in metrics:
                short_name = m.split(".")[-1]
                snapshot[f"{op_key}_{short_name}"] = data.get(m, {}).get("avg", 0.0)

        # 5. Checkpoint
        ckp = self.client.get_checkpoint_stats(self.job_id)
        if ckp:
            counts = ckp.get("counts", {})
            snapshot["checkpoint_completed"] = counts.get("completed", 0)
            snapshot["checkpoint_failed"] = counts.get("failed", 0)

            summary = ckp.get("summary", {})
            e2e_dur = summary.get("end_to_end_duration", {})
            snapshot["checkpoint_avg_dur_ms"] = safe_float(e2e_dur.get("avg", 0))

        # 6. 堆内存
        snapshot["heap_usage_ratio"] = self.client.get_tm_heap_usage()

        # 7. 时间戳
        snapshot["timestamp"] = time.time()

        return snapshot


# ==================== 单策略基准测试 ====================

def run_single_strategy(
    strategy: str,
    config: dict,
    flink_client: FlinkRESTClient,
    cluster: ClusterSSH,
    producer: LocalProducer,
    warmup_seconds: int,
    steady_seconds: int,
    sample_interval: int,
) -> Optional[Dict[str, Any]]:
    """
    运行单个缓存策略的基准测试

    Returns:
        包含所有采样数据和汇总统计的 dict，失败返回 None
    """

    strategy_upper = strategy.upper()
    print(f"\n{'━' * 65}")
    print(f"  策略: {strategy_upper}")
    print(f"{'━' * 65}")

    result = {
        "strategy": strategy,
        "parallelism": BASELINE_PARALLELISM.copy(),
        "warmup_seconds": warmup_seconds,
        "steady_seconds": steady_seconds,
        "sample_interval": sample_interval,
        "start_time": datetime.now().isoformat(),
        "samples": [],
        "summary": {},
        "status": "UNKNOWN",
    }

    try:
        # ── Step 1: 清理残留 ──
        print(f"  [1/8] 清理残留 Job...")
        flink_client.cancel_all_running_jobs()
        time.sleep(3)

        # ── Step 2: 重启集群 ──
        print(f"  [2/8] 重启 Flink 集群...")
        if not cluster.restart_flink_cluster(config, flink_client):
            print(f"  [ERROR] 集群启动失败")
            result["status"] = "CLUSTER_FAIL"
            return result

        # ── Step 3: 重置 RocketMQ ──
        print(f"  [3/8] 重置 RocketMQ 消费进度...")
        master = cluster.get_master()
        master.exec_cmd(
            f"{ROCKETMQ_HOME}/bin/mqadmin resetOffsetByTime "
            f"-n {config['namesrv_addr']} -t {config['topic']} "
            f"-g {config['consumer_group']} -s -1",
            timeout=15, ignore_error=True
        )
        time.sleep(2)

        # ── Step 4: 提交 Job ──
        print(f"  [4/8] 提交 Flink Job (cache-strategy={strategy})...")
        params_str = " ".join(f"--{k} {v}" for k, v in BASELINE_PARALLELISM.items())
        params_str += f" --cache-strategy {strategy}"

        cmd = (f"{config['flink_bin']} run -d "
               f"-c {config['main_class']} {config['remote_jar_path']} "
               f"{params_str}")
        output = master.exec_cmd(cmd, timeout=60)

        job_id = flink_client.wait_for_job_running(config["job_name"], timeout=90)
        if not job_id:
            print(f"  [ERROR] Job 未能启动")
            print(f"    提交输出: {output[:300]}")
            result["status"] = "JOB_FAIL"
            return result

        result["job_id"] = job_id
        print(f"    Job ID: {job_id}")

        # ── Step 5: 发现 Vertices ──
        vertex_map = flink_client.discover_vertices(job_id)
        if len(vertex_map) < 5:
            print(f"  [ERROR] Vertex 发现不完整: {len(vertex_map)}/7")
            result["status"] = "VERTEX_FAIL"
            return result

        print(f"    Vertices: {len(vertex_map)}/7 已发现")

        # ── Step 6: 启动 Producer ──
        print(f"  [5/8] 启动 Producer...")
        if not producer.start(strategy_name=strategy):
            print(f"  [ERROR] Producer 启动失败")
            result["status"] = "PRODUCER_FAIL"
            return result

        print(f"    PID: {producer.process.pid}")

        # ── Step 7: 预热 ──
        print(f"  [6/8] 预热 {warmup_seconds}s...")
        _progress_bar("预热", warmup_seconds)

        # 检查数据是否在流动
        print(f"  [6/8] 等待数据流通...")
        data_flowing = False
        for attempt in range(1, DATA_FLOW_RETRY_COUNT + 1):
            src_vid = vertex_map.get("source")
            sink_vid = vertex_map.get("event_sink")

            src_rate = 0.0
            sink_rate = 0.0

            if src_vid:
                d = flink_client.query_subtask_metrics(
                    job_id, src_vid, ["numRecordsOutPerSecond"]
                )
                src_rate = d.get("numRecordsOutPerSecond", {}).get("sum", 0.0)

            if sink_vid:
                d = flink_client.query_subtask_metrics(
                    job_id, sink_vid, ["numRecordsInPerSecond"]
                )
                sink_rate = d.get("numRecordsInPerSecond", {}).get("sum", 0.0)

            print(f"    第{attempt}次: Source.out={src_rate:.0f}/s  Sink.in={sink_rate:.0f}/s")

            if src_rate > 0 and sink_rate > 0:
                data_flowing = True
                print(f"    ✅ 数据已流通")
                break

            if attempt < DATA_FLOW_RETRY_COUNT:
                time.sleep(DATA_FLOW_RETRY_INTERVAL)

        if not data_flowing:
            print(f"  [WARN] 数据未流通，但继续采集")

        # ── Step 8: 稳态采集 ──
        sample_count = steady_seconds // sample_interval
        print(f"  [7/8] 稳态采集 {steady_seconds}s "
              f"({sample_count} 次采样, 间隔 {sample_interval}s)...")

        collector = MetricsCollector(flink_client, job_id, vertex_map)

        for i in range(1, sample_count + 1):
            try:
                snapshot = collector.collect_snapshot()
                snapshot["sample_index"] = i
                result["samples"].append(snapshot)

                # 实时进度
                e2e = snapshot.get("e2e_latency_p99_ms", 0)
                rate = snapshot.get("source_out_rate", 0)
                l2 = snapshot.get("feature_l2_hit_rate", 0)
                print(f"    [{i:>2d}/{sample_count}] "
                      f"e2e={e2e:>7.1f}ms  "
                      f"rate={rate:>6.0f}/s  "
                      f"L2_hit={l2:.4f}")

            except Exception as e:
                print(f"    [{i:>2d}/{sample_count}] 采样失败: {e}")

            if i < sample_count:
                time.sleep(sample_interval)

        # ── 计算汇总统计 ──
        print(f"  [8/8] 计算汇总统计...")
        result["summary"] = _compute_summary(result["samples"])
        result["end_time"] = datetime.now().isoformat()
        result["status"] = "SUCCESS"

        # 打印汇总
        _print_strategy_summary(strategy_upper, result["summary"])

        return result

    except Exception as e:
        print(f"  [ERROR] 策略 {strategy} 异常: {e}")
        import traceback
        traceback.print_exc()
        result["status"] = "EXCEPTION"
        result["error"] = str(e)
        return result

    finally:
        # 清理
        producer.stop()
        try:
            flink_client.cancel_all_running_jobs()
        except Exception:
            pass
        time.sleep(5)


def _compute_summary(samples: List[Dict[str, float]]) -> Dict[str, float]:
    """从多次采样中计算汇总统计（中位数为主）"""
    if not samples:
        return {}

    summary = {}

    # 需要汇总的关键指标
    keys = [
        "e2e_latency_p99_ms",
        "e2e_latency_avg_ms",
        "sink_latency_p99_us",
        "source_out_rate",
        "event_sink_in_rate",
        "grid_sink_in_rate",
        "avg_busy_ratio",
        "max_busy_ratio",
        "max_bp_ratio",
        "feature_l2_hit_rate",
        "feature_l2_occupancy",
        "grid_l3_hit_rate",
        "grid_l3_occupancy",
        "checkpoint_avg_dur_ms",
        "heap_usage_ratio",
    ]

    for key in keys:
        values = [s.get(key, 0.0) for s in samples if key in s]
        if values:
            arr = np.array(values, dtype=float)
            summary[f"{key}_median"] = float(np.median(arr))
            summary[f"{key}_mean"] = float(np.mean(arr))
            summary[f"{key}_std"] = float(np.std(arr))
            summary[f"{key}_min"] = float(np.min(arr))
            summary[f"{key}_max"] = float(np.max(arr))
            summary[f"{key}_p95"] = float(np.percentile(arr, 95))
        else:
            summary[f"{key}_median"] = 0.0

    summary["sample_count"] = len(samples)
    summary["valid_sample_count"] = len([
        s for s in samples if s.get("source_out_rate", 0) > 0
    ])

    # 峰值输入速率 & CV
    rates = [s.get("source_out_rate", 0.0) for s in samples]
    summary["peak_input_rate"] = float(max(rates)) if rates else 0.0
    mean_rate = float(np.mean(rates)) if rates else 0.0
    std_rate = float(np.std(rates)) if rates else 0.0
    summary["input_cv"] = std_rate / mean_rate if mean_rate > 0 else 0.0

    return summary


def _print_strategy_summary(strategy_name: str, summary: Dict[str, float]):
    """打印单策略汇总"""
    print(f"\n  ┌─ {strategy_name} 汇总")
    print(f"  │")
    print(f"  │  采样: {summary.get('valid_sample_count', 0)}"
          f"/{summary.get('sample_count', 0)} 有效")
    print(f"  │")
    print(f"  │  ★ 端到端延迟 P99 (中位数): "
          f"{summary.get('e2e_latency_p99_ms_median', 0):.1f} ms")
    print(f"  │    范围: [{summary.get('e2e_latency_p99_ms_min', 0):.1f}, "
          f"{summary.get('e2e_latency_p99_ms_max', 0):.1f}] ms")
    print(f"  │")
    print(f"  │  吞吐量 (中位数): "
          f"{summary.get('source_out_rate_median', 0):.0f} records/s")
    print(f"  │    峰值: {summary.get('peak_input_rate', 0):.0f} records/s"
          f"  CV: {summary.get('input_cv', 0):.4f}")
    print(f"  │")
    print(f"  │  L2 缓存命中率: "
          f"{summary.get('feature_l2_hit_rate_median', 0):.4f}")
    print(f"  │  L3 缓存命中率: "
          f"{summary.get('grid_l3_hit_rate_median', 0):.4f}")
    print(f"  │")
    print(f"  │  繁忙率 (avg): "
          f"{summary.get('avg_busy_ratio_median', 0):.1%}")
    print(f"  │  背压率 (max): "
          f"{summary.get('max_bp_ratio_median', 0):.1%}")
    print(f"  │")
    print(f"  │  堆内存: "
          f"{summary.get('heap_usage_ratio_median', 0):.1%}")
    print(f"  │  Checkpoint: "
          f"{summary.get('checkpoint_avg_dur_ms_median', 0):.1f} ms")
    print(f"  └─")


# ==================== 汇总对比 ====================

def generate_comparison(all_results: List[Dict], output_dir: str):
    """生成 5 策略汇总对比"""

    print(f"\n\n{'═' * 75}")
    print(f"  基 准 性 能 对 比 汇 总")
    print(f"{'═' * 75}")

    # 表头
    header_metrics = [
        ("策略",            "strategy",                       "{:<12s}"),
        ("状态",            "status",                         "{:<8s}"),
        ("e2e P99(ms)",     "e2e_latency_p99_ms_median",      "{:>11.1f}"),
        ("吞吐量(/s)",       "source_out_rate_median",          "{:>10.0f}"),
        ("L2命中率",         "feature_l2_hit_rate_median",      "{:>8.4f}"),
        ("L3命中率",         "grid_l3_hit_rate_median",         "{:>8.4f}"),
        ("繁忙率",           "avg_busy_ratio_median",           "{:>6.1%}"),
        ("背压率",           "max_bp_ratio_median",             "{:>6.1%}"),
        ("Heap",            "heap_usage_ratio_median",         "{:>6.1%}"),
        ("有效采样",         "valid_sample_count",              "{:>8d}"),
    ]

    # 打印控制台表格
    header_line = "  "
    for label, _, _ in header_metrics:
        header_line += f"{label:>12s} │"
    print(f"\n{header_line}")
    print(f"  {'─' * 12}─┼" * len(header_metrics))

    csv_rows = []
    csv_header = [label for label, _, _ in header_metrics]

    for res in all_results:
        strategy = res.get("strategy", "?")
        status = res.get("status", "?")
        summary = res.get("summary", {})

        row_line = "  "
        csv_row = []

        for label, key, fmt in header_metrics:
            if key == "strategy":
                val_str = fmt.format(strategy.upper())
                csv_row.append(strategy.upper())
            elif key == "status":
                val_str = fmt.format(status[:8])
                csv_row.append(status)
            elif key == "valid_sample_count":
                val = int(summary.get(key, 0))
                val_str = fmt.format(val)
                csv_row.append(val)
            else:
                val = summary.get(key, 0.0)
                try:
                    val_str = fmt.format(val)
                except (ValueError, TypeError):
                    val_str = f"{'N/A':>12s}"
                csv_row.append(round(val, 6) if isinstance(val, float) else val)

            row_line += f"{val_str:>12s} │"

        print(row_line)
        csv_rows.append(csv_row)

    print()

    # 找最优策略
    success_results = [r for r in all_results if r.get("status") == "SUCCESS"]
    if success_results:
        # 延迟最低
        best_latency = min(
            success_results,
            key=lambda r: r.get("summary", {}).get("e2e_latency_p99_ms_median", float("inf"))
        )
        # 吞吐量最高
        best_throughput = max(
            success_results,
            key=lambda r: r.get("summary", {}).get("source_out_rate_median", 0)
        )
        # L2 命中率最高
        best_l2 = max(
            success_results,
            key=lambda r: r.get("summary", {}).get("feature_l2_hit_rate_median", 0)
        )

        print(f"  ★ 延迟最低:   {best_latency['strategy'].upper()}"
              f"  ({best_latency['summary'].get('e2e_latency_p99_ms_median', 0):.1f} ms)")
        print(f"  ★ 吞吐最高:   {best_throughput['strategy'].upper()}"
              f"  ({best_throughput['summary'].get('source_out_rate_median', 0):.0f} /s)")
        print(f"  ★ L2命中最高: {best_l2['strategy'].upper()}"
              f"  ({best_l2['summary'].get('feature_l2_hit_rate_median', 0):.4f})")

    # 保存 CSV
    csv_path = os.path.join(output_dir, "baseline_comparison.csv")
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(csv_header)
        writer.writerows(csv_rows)
    print(f"\n  📊 对比表已保存: {csv_path}")

    # 保存汇总 JSON
    summary_path = os.path.join(output_dir, "baseline_summary.json")
    summary_data = {
        "generated_at": datetime.now().isoformat(),
        "parallelism": BASELINE_PARALLELISM,
        "strategies": {}
    }
    for res in all_results:
        strategy = res.get("strategy", "?")
        summary_data["strategies"][strategy] = {
            "status": res.get("status"),
            "summary": res.get("summary", {}),
        }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(to_serializable(summary_data), f, indent=2, ensure_ascii=False)
    print(f"  📄 汇总 JSON: {summary_path}")


# ==================== 进度条 ====================

def _progress_bar(phase, total_seconds):
    interval = 5 if total_seconds <= 30 else 10
    for remaining in range(total_seconds, 0, -interval):
        elapsed = total_seconds - remaining
        bar_len = 30
        filled = int(bar_len * elapsed / total_seconds)
        bar = "█" * filled + "░" * (bar_len - filled)
        print(f"\r    [{phase}] {bar} {remaining:>4d}s",
              end="", flush=True)
        time.sleep(min(interval, remaining))
    print(f"\r    [{phase}] {'█' * 30} 完成!{'':>10s}")


# ==================== 主流程 ====================

def main():
    parser = argparse.ArgumentParser(
        description="基准性能测量（5 种缓存策略对比）"
    )
    parser.add_argument("--config", type=str, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--jar", type=str, default=None)
    parser.add_argument("--strategies", type=str, default=None,
                        help="逗号分隔的策略列表，默认全部 5 种")
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP,
                        help=f"预热秒数（默认 {DEFAULT_WARMUP}）")
    parser.add_argument("--steady", type=int, default=DEFAULT_STEADY,
                        help=f"稳态采集秒数（默认 {DEFAULT_STEADY}）")
    parser.add_argument("--sample-interval", type=int, default=DEFAULT_SAMPLE_INTERVAL,
                        help=f"采样间隔（默认 {DEFAULT_SAMPLE_INTERVAL}s）")
    parser.add_argument("--skip-upload", action="store_true",
                        help="跳过 JAR 上传")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="输出目录（默认 final_test/baseline_YYYYMMDD_HHMMSS/）")
    args = parser.parse_args()

    local_jar = args.jar or LOCAL_JAR_PATH

    # 策略列表
    if args.strategies:
        strategies = [s.strip() for s in args.strategies.split(",")]
        for s in strategies:
            if s not in ALL_STRATEGIES:
                print(f"[ERROR] 未知策略: {s}")
                print(f"  可选: {', '.join(ALL_STRATEGIES)}")
                sys.exit(1)
    else:
        strategies = ALL_STRATEGIES.copy()

    # 输出目录
    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = os.path.join(
            FINAL_TEST_DIR,
            f"baseline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
    os.makedirs(output_dir, exist_ok=True)

    # 计算预计时间
    per_strategy_seconds = args.warmup + args.steady + 120  # +120 包含重启/预检
    total_estimated = per_strategy_seconds * len(strategies)
    total_min = total_estimated / 60

    # Banner
    print()
    print("╔════════════════════════════════════════════════════════════════╗")
    print("║          基 准 性 能 测 量（Baseline Benchmark）              ║")
    print("╠════════════════════════════════════════════════════════════════╣")
    print(f"║  时间:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<50s}║")
    print(f"║  策略:    {', '.join(s.upper() for s in strategies):<50s}║")
    print(f"║  并行度:  source=2 filter=4 signal=4 feature=4 grid=4 sink=2 ║")
    print(f"║  预热:    {args.warmup}s    稳态采集: {args.steady}s"
          f"    采样间隔: {args.sample_interval}s{'':>11s}║")
    print(f"║  预计耗时: ~{total_min:.0f} 分钟{'':>48s}║")
    print(f"║  输出目录: {os.path.basename(output_dir):<49s}║")
    print("╚════════════════════════════════════════════════════════════════╝")
    print()

    # 加载配置
    print(f"[STEP] 加载配置: {os.path.basename(args.config)}")
    config = load_config(args.config)

    # 前置检查
    print("\n[CHECK] 前置检查...")
    if not os.path.exists(local_jar):
        print(f"  ❌ JAR 不存在: {local_jar}")
        sys.exit(1)
    jar_mb = os.path.getsize(local_jar) / 1024 / 1024
    jar_mtime = datetime.fromtimestamp(os.path.getmtime(local_jar)).strftime("%m-%d %H:%M")
    print(f"  ✅ JAR: {jar_mb:.1f} MB, 修改: {jar_mtime}")

    try:
        java_ver = subprocess.check_output(
            ["java", "-version"], stderr=subprocess.STDOUT
        ).decode().split("\n")[0]
        print(f"  ✅ Java: {java_ver}")
    except Exception:
        print("  ❌ java 不可用")
        sys.exit(1)

    # 初始化
    flink_client = FlinkRESTClient(config["flink_rest_url"])
    cluster = ClusterSSH(config)
    producer = LocalProducer(local_jar, PRODUCER_WORK_DIR)

    all_results = []

    try:
        print("\n[SSH] 连接集群...")
        cluster.connect_all()

        master = cluster.get_master()
        if not master:
            print("[ERROR] 无法连接主节点")
            sys.exit(1)

        # 上传 JAR
        if not args.skip_upload:
            print()
            if not cluster.upload_jar(local_jar, config["remote_jar_path"]):
                sys.exit(1)
            print(f"  ✅ JAR 已上传")
        else:
            print(f"\n  [SKIP] 跳过 JAR 上传")

        # 逐策略运行
        for idx, strategy in enumerate(strategies, 1):
            print(f"\n\n{'▓' * 65}")
            print(f"  [{idx}/{len(strategies)}] 开始策略: {strategy.upper()}")
            remaining_strategies = len(strategies) - idx
            remaining_time = remaining_strategies * per_strategy_seconds / 60
            print(f"  预计剩余: ~{remaining_time:.0f} 分钟")
            print(f"{'▓' * 65}")

            result = run_single_strategy(
                strategy=strategy,
                config=config,
                flink_client=flink_client,
                cluster=cluster,
                producer=producer,
                warmup_seconds=args.warmup,
                steady_seconds=args.steady,
                sample_interval=args.sample_interval,
            )

            if result:
                all_results.append(result)

                # 保存单策略详细报告
                detail_path = os.path.join(
                    output_dir, f"baseline_{strategy}.json"
                )
                with open(detail_path, "w", encoding="utf-8") as f:
                    json.dump(to_serializable(result), f, indent=2, ensure_ascii=False)
                print(f"  📄 详细报告: {detail_path}")

            # 策略间冷却
            if idx < len(strategies):
                print(f"\n  策略间冷却 10s...")
                time.sleep(10)

        # 生成汇总对比
        if all_results:
            generate_comparison(all_results, output_dir)

    except KeyboardInterrupt:
        print(f"\n\n[中断] 用户取消")
        print(f"  已完成 {len(all_results)}/{len(strategies)} 个策略")
        if all_results:
            print(f"  保存已完成的结果...")
            generate_comparison(all_results, output_dir)

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

    finally:
        print(f"\n{'═' * 65}")
        print(f"  清理...")
        print(f"{'═' * 65}")
        producer.stop()
        try:
            flink_client.cancel_all_running_jobs()
        except Exception:
            pass
        flink_client.close()
        cluster.close_all()
        print(f"\n  ✅ 清理完成")
        print(f"  📁 输出目录: {output_dir}")
        print(f"  完成策略: {len(all_results)}/{len(strategies)}")

        if all_results:
            success_count = sum(1 for r in all_results if r["status"] == "SUCCESS")
            print(f"  成功: {success_count}  失败: {len(all_results) - success_count}")


if __name__ == "__main__":
    main()
