#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
calibrate_source.py - Source 算子单实例容量标定（全自动，无配置文件）

所有配置硬编码在代码顶部，直接运行即可。

使用:
  python calibrate_source.py
  python calibrate_source.py --parallelism-list 1 2 3
  python calibrate_source.py --warmup 120 --sample 90
  python calibrate_source.py --rate 30000
"""

import argparse
import csv
import math
import logging
import os
import re
import signal
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
import paramiko
import numpy as np

# ============================================================
# ★★★ 硬编码配置区（修改这里即可）★★★
# ============================================================

# Flink
FLINK_REST_URL      = "http://192.168.56.151:8081"
FLINK_HOME          = "/opt/app/flink-1.17.2"
JAR_PATH            = "/opt/flink_jobs/flink-rocketmq-demo-1.0-SNAPSHOT.jar"
MAIN_CLASS          = "org.jzx.SeismicStreamJob"

# SSH
SSH_HOST            = "192.168.56.151"
SSH_PORT            = 22
SSH_USER            = "root"
SSH_PASSWORD        = "123456"

# RocketMQ
ROCKETMQ_HOME       = "/usr/local/rocketmq"
ROCKETMQ_CLUSTER    = "RaftCluster"
ROCKETMQ_NAMESRV    = "192.168.56.151:9876"
CONSUMER_GROUP_PREFIX = "seismic-flink-consumer-group"
DEREGISTER_WAIT     = 15

# Producer
PRODUCER_CLASS      = "org.jzx.producer.HighThroughputSeismicProducer"
PRODUCER_JAR        = r"E:\Desktop\bs_code\flink-rocketmq-demo\target\flink-rocketmq-demo-1.0-SNAPSHOT.jar"
PRODUCER_WORK_DIR   = r"E:\Desktop\bs_code\flink-rocketmq-demo"
PRODUCER_NAMESRV    = "192.168.56.151:9876"
PRODUCER_RATE       = 20000

# 标定参数
PARALLELISM_LIST    = [1, 2, 3, 4]
WARMUP_SECONDS      = 90
SAMPLE_SECONDS      = 60
SAMPLE_INTERVAL     = 2.0
DRAIN_SECONDS       = 15

# 下游并行度
DOWNSTREAM_PARALLELISM = {
    "p-filter":  4,
    "p-signal":  4,
    "p-feature": 4,
    "p-grid":    4,
    "p-sink":    2,
}

# 超时
CANCEL_TIMEOUT      = 60
SUBMIT_TIMEOUT      = 120
JOB_START_TIMEOUT   = 90

# 输出
OUTPUT_DIR          = "calibration_results"

# ============================================================
# 日志
# ============================================================
LOG_FORMAT = "[%(asctime)s] %(levelname)-5s %(name)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("calibrate_source")

# 算子匹配（与 ds2_calibration.py 一致）
SOURCE_PATTERN = re.compile(r'Source.*RocketMQ', re.IGNORECASE)


# ============================================================
# SSH
# ============================================================
class SSHExecutor:

    def __init__(self):
        self.host = SSH_HOST
        self.port = SSH_PORT
        self.user = SSH_USER
        self.password = SSH_PASSWORD

    def exec_command(self, cmd: str, timeout: int = 120) -> str:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(self.host, self.port,
                           self.user, self.password, timeout=10)
            _, stdout, stderr = client.exec_command(cmd, timeout=timeout)
            out = stdout.read().decode('utf-8', errors='replace').strip()
            err = stderr.read().decode('utf-8', errors='replace').strip()
            return out if out else err
        finally:
            client.close()


# ============================================================
# Flink REST 客户端
# ============================================================
class FlinkClient:

    def __init__(self):
        self.rest_url = FLINK_REST_URL.rstrip('/')
        self._session = requests.Session()
        self._session.headers.update({'Accept': 'application/json'})
        self._timeout = 15
        # 缓存 source vertex 信息，避免每次查
        self._source_vertex_cache: Dict[str, dict] = {}

    def _get(self, path: str) -> dict:
        resp = self._session.get(f"{self.rest_url}{path}", timeout=self._timeout)
        resp.raise_for_status()
        return resp.json()

    def _get_safe(self, path: str) -> Optional[dict]:
        try:
            return self._get(path)
        except Exception:
            return None

    def check_connection(self) -> bool:
        data = self._get_safe("/overview")
        if data:
            logger.info("Flink 集群可用: version=%s, slots=%s, taskmanagers=%s",
                        data.get('flink-version', '?'),
                        data.get('slots-total', '?'),
                        data.get('taskmanagers', '?'))
            return True
        logger.error("无法连接 Flink: %s", self.rest_url)
        return False

    def ensure_jar_uploaded(self, ssh: SSHExecutor) -> Optional[str]:
        try:
            jars = self._get("/jars")
            for f in jars.get("files", []):
                if ('rocketmq-demo' in f['name'].lower()
                        or 'seismic' in f['name'].lower()):
                    logger.info("找到已上传 JAR: %s -> %s", f['name'], f["id"])
                    return f["id"]
        except Exception:
            pass

        logger.info("通过 SSH 上传 JAR: %s", JAR_PATH)
        try:
            upload_cmd = (
                f'curl -s -X POST -H "Expect:" '
                f'-F "jarfile=@{JAR_PATH}" '
                f'http://localhost:8081/jars/upload'
            )
            out = ssh.exec_command(upload_cmd)
            if '"status":"success"' in out:
                jars = self._get("/jars")
                for f in jars.get("files", []):
                    if ('rocketmq-demo' in f['name'].lower()
                            or 'seismic' in f['name'].lower()):
                        logger.info("JAR 上传成功: %s", f["id"])
                        return f["id"]
            logger.error("JAR 上传失败: %s", out[:300])
        except Exception as e:
            logger.error("JAR 上传异常: %s", e)
        return None

    def find_running_job(self) -> Optional[str]:
        try:
            jobs = self._get("/jobs")
            for j in jobs.get("jobs", []):
                if j["status"] == "RUNNING":
                    return j["id"]
        except Exception:
            pass
        return None

    def cancel_job(self, job_id: str) -> bool:
        logger.info("Cancel 作业: %s", job_id)
        try:
            self._session.patch(f"{self.rest_url}/jobs/{job_id}",
                                timeout=self._timeout)
        except Exception as e:
            logger.error("Cancel 请求异常: %s", e)

        for i in range(CANCEL_TIMEOUT // 2):
            time.sleep(2)
            try:
                state = self._get(f"/jobs/{job_id}").get("state", "UNKNOWN")
                if state not in ["RUNNING", "CANCELLING", "FAILING"]:
                    logger.info("作业已停止: %s", state)
                    return True
            except Exception:
                pass
        logger.error("等待作业停止超时")
        return False

    def cancel_all_running(self):
        while True:
            job_id = self.find_running_job()
            if not job_id:
                break
            self.cancel_job(job_id)

    def submit_job(self, jar_id: str, program_args: str) -> Optional[str]:
        logger.info("提交作业:")
        logger.info("  entryClass : %s", MAIN_CLASS)
        logger.info("  programArgs: %s", program_args)

        run_body = {
            "entryClass": MAIN_CLASS,
            "programArgs": program_args,
            "allowNonRestoredState": True,
        }

        url = f"{self.rest_url}/jars/{jar_id}/run"
        try:
            resp = self._session.post(url, json=run_body, timeout=SUBMIT_TIMEOUT)
            try:
                body = resp.json()
            except Exception:
                body = {"raw": resp.text[:500]}

            if resp.status_code == 200:
                job_id = body.get("jobid")
                logger.info("✅ 作业提交成功: %s", job_id)
                return job_id

            errors = body.get("errors", [])
            if errors:
                root = errors[0] if isinstance(errors[0], str) else str(errors[0])
                for line in root.split('\n'):
                    line = line.strip()
                    if line and not line.startswith('at '):
                        logger.error("❌ 提交失败 [%d]: %s",
                                     resp.status_code, line[:300])
                        break
            else:
                logger.error("❌ 提交失败 [%d]: %s",
                             resp.status_code, str(body)[:300])
            return None
        except Exception as e:
            logger.error("提交异常: %s", e)
            return None

    def wait_job_running(self, job_id: str, timeout: int = JOB_START_TIMEOUT) -> bool:
        for i in range(timeout // 3):
            time.sleep(3)
            try:
                state = self._get(f"/jobs/{job_id}").get("state", "UNKNOWN")
                if state == "RUNNING":
                    logger.info("✅ 作业 RUNNING: %s", job_id)
                    return True
                if state in ["FAILED", "CANCELED", "CANCELLED"]:
                    try:
                        exc = self._get(f"/jobs/{job_id}/exceptions")
                        root = exc.get("root-exception", "未知")
                        logger.error("作业 %s: %s", state, root[:300])
                    except Exception:
                        logger.error("作业终态: %s", state)
                    return False
            except Exception:
                pass
        logger.error("等待 RUNNING 超时 (%ds)", timeout)
        return False

    def wait_slots_available(self, min_slots: int = 4, timeout: int = 30):
        for _ in range(timeout // 2):
            time.sleep(2)
            try:
                overview = self._get("/overview")
                if overview.get("slots-available", 0) >= min_slots:
                    return
            except Exception:
                pass
        logger.warning("等待 slot 释放超时，继续")

    # ---- Source 算子发现（带完整诊断） ----

    def discover_source_vertex(self, job_id: str) -> Optional[dict]:
        """发现 Source 算子，首次调用时打印所有 vertex 供诊断"""
        if job_id in self._source_vertex_cache:
            return self._source_vertex_cache[job_id]

        try:
            data = self._get(f"/jobs/{job_id}")
            vertices = data.get("vertices", [])

            # 首次：打印所有 vertex
            logger.info("作业 vertex 列表 (%d 个):", len(vertices))
            for v in vertices:
                logger.info("  [%s] P=%d  %s",
                            v["id"][:8], v.get("parallelism", 0), v.get("name", ""))

            # 精确匹配
            for v in vertices:
                if SOURCE_PATTERN.search(v.get("name", "")):
                    logger.info("✅ Source 算子匹配: %s (P=%d)",
                                v.get("name", ""), v.get("parallelism", 0))
                    self._source_vertex_cache[job_id] = v
                    return v

            # 降级匹配
            for v in vertices:
                name = v.get("name", "").lower()
                if "source" in name:
                    logger.warning("⚠️ Source 降级匹配: %s", v.get("name", ""))
                    self._source_vertex_cache[job_id] = v
                    return v

            logger.error("❌ 未找到 Source 算子！")
            return None
        except Exception as e:
            logger.error("发现 vertex 异常: %s", e)
            return None

    def get_source_parallelism(self, job_id: str) -> int:
        source = self.discover_source_vertex(job_id)
        return source.get("parallelism", 0) if source else 0

    # ---- 指标采集（与 ds2_calibration.py 完全一致） ----

    def _query_subtask_metrics(self, job_id: str, vertex_id: str,
                                metric_names: list) -> dict:
        """
        subtask 聚合 API（与 ds2_calibration._query_subtask_metrics 一致）
        返回: {metric_name: {"sum": x, "avg": x, "min": x, "max": x}, ...}
        """
        url = (f"{self.rest_url}/jobs/{job_id}"
               f"/vertices/{vertex_id}/subtasks/metrics")
        params = {"get": ",".join(metric_names)}
        try:
            resp = self._session.get(url, params=params, timeout=self._timeout)
            if not resp.ok:
                return {}
            result = {}
            for item in resp.json():
                mid = item.get("id", "")
                result[mid] = {
                    "sum": item.get("sum", 0.0),
                    "avg": item.get("avg", 0.0),
                    "min": item.get("min", 0.0),
                    "max": item.get("max", 0.0),
                }
            return result
        except Exception:
            return {}

    def get_source_output_rate(self, job_id: str) -> Optional[float]:
        """
        获取 Source 算子输出速率
        三级降级：
          1. subtask 聚合 API (sum) ← ds2_calibration 用的就是这个
          2. vertex 级别 API
          3. 逐 subtask 累加
        """
        source = self.discover_source_vertex(job_id)
        if not source:
            return None

        vid = source["id"]
        par = source.get("parallelism", 1)

        # ★ 方式1: subtask 聚合 API（与 ds2_calibration 一致）
        data = self._query_subtask_metrics(
            job_id, vid, ["numRecordsOutPerSecond"])
        out_info = data.get("numRecordsOutPerSecond")
        if out_info:
            sum_val = float(out_info.get("sum", 0))
            if sum_val > 0:
                return sum_val
            avg_val = float(out_info.get("avg", 0))
            if avg_val > 0:
                return avg_val * par

        # 方式2: vertex 级别 API
        try:
            metrics = self._get(
                f"/jobs/{job_id}/vertices/{vid}"
                f"/metrics?get=numRecordsOutPerSecond")
            for m in metrics:
                if m.get("id") == "numRecordsOutPerSecond":
                    val = float(m["value"])
                    if val > 0 and not (math.isnan(val) or math.isinf(val)):
                        return val
        except Exception:
            pass

        # 方式3: 逐 subtask 累加
        total = 0.0
        found = False
        for idx in range(par):
            try:
                url = (f"{self.rest_url}/jobs/{job_id}"
                       f"/vertices/{vid}/subtasks/{idx}/metrics"
                       f"?get=numRecordsOutPerSecond")
                resp = self._session.get(url, timeout=self._timeout)
                if resp.ok:
                    for item in resp.json():
                        if item.get("id") == "numRecordsOutPerSecond":
                            val = float(item.get("value", 0))
                            if not (math.isnan(val) or math.isinf(val)):
                                total += val
                                found = True
                            break
            except Exception:
                pass

        if found:
            return total

        return None

    def diagnose_source_metrics(self, job_id: str):
        """首次采样失败时调用：打印 Source 算子所有可用指标名，帮助排查"""
        source = self.discover_source_vertex(job_id)
        if not source:
            return

        vid = source["id"]

        # 列出 vertex 级别所有可用指标
        try:
            metrics = self._get(f"/jobs/{job_id}/vertices/{vid}/metrics")
            names = [m.get("id", "") for m in metrics]
            logger.info("Source vertex 可用指标 (%d 个):", len(names))
            for n in sorted(names):
                if 'record' in n.lower() or 'byte' in n.lower() or 'rate' in n.lower():
                    logger.info("  ★ %s", n)
                else:
                    logger.debug("    %s", n)
        except Exception as e:
            logger.warning("列出 vertex 指标失败: %s", e)

        # 列出 subtask 聚合可用指标
        try:
            metrics = self._get(
                f"/jobs/{job_id}/vertices/{vid}/subtasks/metrics")
            names = [m.get("id", "") for m in metrics]
            logger.info("Source subtask 聚合可用指标 (%d 个):", len(names))
            for n in sorted(names):
                if 'record' in n.lower() or 'byte' in n.lower() or 'rate' in n.lower():
                    logger.info("  ★ %s", n)
        except Exception as e:
            logger.warning("列出 subtask 指标失败: %s", e)

        # 尝试读 subtask 0 的指标
        try:
            metrics = self._get(
                f"/jobs/{job_id}/vertices/{vid}/subtasks/0/metrics")
            names = [m.get("id", "") for m in metrics]
            logger.info("Source subtask[0] 可用指标 (%d 个):", len(names))
            for n in sorted(names):
                if 'record' in n.lower() or 'out' in n.lower():
                    logger.info("  ★ %s", n)
        except Exception as e:
            logger.warning("列出 subtask[0] 指标失败: %s", e)

    def close(self):
        self._session.close()


# ============================================================
# Producer 管理器
# ============================================================
class ProducerManager:

    def __init__(self):
        self._process: Optional[subprocess.Popen] = None
        self._log_handle = None
        self._log_dir = os.path.join(OUTPUT_DIR, "logs")

    def start(self, rate: int) -> bool:
        self.stop()

        jar = PRODUCER_JAR
        if not os.path.exists(jar):
            logger.error("Producer JAR 不存在: %s", jar)
            return False

        os.makedirs(self._log_dir, exist_ok=True)
        log_path = os.path.join(self._log_dir, f"producer_src_cal_{rate}.log")

        cmd = [
            'java', '-cp', jar,
            PRODUCER_CLASS,
            str(rate),
            PRODUCER_NAMESRV,
        ]

        logger.info("启动 Producer: rate=%d r/s", rate)
        logger.info("  命令: %s", ' '.join(cmd))

        try:
            self._log_handle = open(log_path, 'w', encoding='utf-8')
            self._process = subprocess.Popen(
                cmd,
                cwd=PRODUCER_WORK_DIR,
                stdout=self._log_handle,
                stderr=subprocess.STDOUT,
            )

            time.sleep(3)

            if self._process.poll() is not None:
                logger.error("Producer 启动后立即退出, returncode=%d",
                             self._process.returncode)
                try:
                    with open(log_path, 'r', encoding='utf-8') as lf:
                        tail = lf.read()[-500:]
                        logger.error("Producer 日志尾部:\n%s", tail)
                except Exception:
                    pass
                self._process = None
                return False

            logger.info("✅ Producer 已启动: PID=%d, rate=%d, log=%s",
                        self._process.pid, rate, log_path)
            return True

        except Exception as e:
            logger.error("Producer 启动失败: %s", e)
            self._process = None
            return False

    def stop(self):
        if self._process is not None and self._process.poll() is None:
            logger.info("停止 Producer: PID=%d", self._process.pid)
            try:
                self._process.terminate()
                self._process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)
            except Exception as e:
                logger.warning("停止 Producer 异常: %s", e)

        self._process = None

        if self._log_handle is not None:
            try:
                self._log_handle.close()
            except Exception:
                pass
            self._log_handle = None

    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None


# ============================================================
# 单轮结果
# ============================================================
@dataclass
class CalibrationRound:
    parallelism: int
    consumer_group: str = ""
    samples: List[float] = field(default_factory=list)
    total_output_rate: float = 0.0
    per_instance_rate: float = 0.0
    std_dev: float = 0.0
    min_rate: float = 0.0
    max_rate: float = 0.0
    valid_sample_count: int = 0
    timestamp: str = ""


# ============================================================
# 标定执行器
# ============================================================
class SourceCalibrator:

    def __init__(self, parallelism_list: List[int],
                 warmup: int, sample: int, rate: int):
        self.parallelism_list = parallelism_list
        self.warmup_seconds = warmup
        self.sample_seconds = sample
        self.producer_rate = rate

        self.flink = FlinkClient()
        self.ssh = SSHExecutor()
        self.producer = ProducerManager()
        self.results: List[CalibrationRound] = []
        self._last_consumer_group: Optional[str] = None
        self._jar_id: Optional[str] = None
        self._running = True
        self._diagnosed = False  # 是否已执行过诊断

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.warning("收到信号 %d，准备清理退出...", signum)
        self._running = False

    # ---- 前置检查 ----

    def preflight_check(self) -> bool:
        logger.info("=" * 60)
        logger.info("前置环境检查")
        logger.info("=" * 60)

        if not self.flink.check_connection():
            return False

        self._jar_id = self.flink.ensure_jar_uploaded(self.ssh)
        if not self._jar_id:
            return False

        if not os.path.exists(PRODUCER_JAR):
            logger.error("Producer JAR 不存在: %s", PRODUCER_JAR)
            return False
        logger.info("Producer JAR: %s", PRODUCER_JAR)

        try:
            out = self.ssh.exec_command("echo OK", timeout=10)
            if "OK" in out:
                logger.info("SSH 连接正常: %s@%s", SSH_USER, SSH_HOST)
        except Exception as e:
            logger.warning("SSH 连接失败: %s", e)

        running = self.flink.find_running_job()
        if running:
            logger.warning("发现运行中的作业 %s，清理...", running)
            self.flink.cancel_all_running()
            time.sleep(5)

        logger.info("前置检查通过 ✓")
        return True

    # ---- 参数构建 ----

    def _build_program_args(self, source_parallelism: int) -> tuple:
        parts = []
        suffix = f"_src_cal_{uuid.uuid4().hex[:8]}"
        consumer_group = f"{CONSUMER_GROUP_PREFIX}{suffix}"
        parts.append(f"--consumer-group-suffix {suffix}")
        parts.append("--consume-from-latest true")
        parts.append(f"--p-source {source_parallelism}")

        for arg_name, value in DOWNSTREAM_PARALLELISM.items():
            parts.append(f"--{arg_name} {value}")

        return " ".join(parts), consumer_group

    # ---- Consumer Group 清理 ----

    def _cleanup_consumer_group(self, group_name: str):
        logger.info("清理 Consumer Group: %s", group_name)
        cmd = (
            f"sh {ROCKETMQ_HOME}/bin/mqadmin deleteSubGroup "
            f"-n {ROCKETMQ_NAMESRV} "
            f"-c {ROCKETMQ_CLUSTER} "
            f"-g {group_name} || true"
        )
        try:
            out = self.ssh.exec_command(cmd)
            logger.info("清理完成: %s", out[:200] if out else "(无输出)")
        except Exception as e:
            logger.warning("清理失败(非致命): %s", e)

    # ---- 采样（带自动诊断） ----

    def _collect_samples(self, job_id: str) -> List[float]:
        samples = []
        total_attempts = int(self.sample_seconds / SAMPLE_INTERVAL)

        logger.info("稳态采样: %d 次 × %.0fs = %ds",
                     total_attempts, SAMPLE_INTERVAL, self.sample_seconds)

        consecutive_na = 0

        for i in range(total_attempts):
            if not self._running:
                break

            rate = self.flink.get_source_output_rate(job_id)

            if rate is not None and rate >= 0:
                samples.append(rate)
                consecutive_na = 0
                status = f"  [{i + 1:3d}/{total_attempts}] rate = {rate:8.1f} r/s"
            else:
                consecutive_na += 1
                status = f"  [{i + 1:3d}/{total_attempts}] rate = N/A"

                # ★ 连续 3 次 N/A 且未诊断过，自动触发诊断
                if consecutive_na == 3 and not self._diagnosed:
                    logger.warning("连续 %d 次采样失败，触发指标诊断...", consecutive_na)
                    self.flink.diagnose_source_metrics(job_id)
                    self._diagnosed = True

            if (i + 1) % 5 == 0 or i == 0 or i == total_attempts - 1:
                logger.info(status)

            if i < total_attempts - 1:
                time.sleep(SAMPLE_INTERVAL)

        return samples

    # ---- 分析 ----

    @staticmethod
    def _analyze_samples(parallelism: int, samples: List[float],
                         consumer_group: str = "") -> CalibrationRound:
        result = CalibrationRound(
            parallelism=parallelism,
            consumer_group=consumer_group,
            samples=samples,
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        )

        if not samples:
            logger.warning("没有有效样本！")
            return result

        stable = samples[2:] if len(samples) > 4 else samples
        if not stable:
            stable = samples

        arr = np.array(stable)
        result.valid_sample_count = len(stable)
        result.total_output_rate = float(np.mean(arr))
        result.per_instance_rate = result.total_output_rate / max(parallelism, 1)
        result.min_rate = float(np.min(arr))
        result.max_rate = float(np.max(arr))
        result.std_dev = float(np.std(arr))

        return result

    # ---- 单轮执行 ----

    def run_one_round(self, source_parallelism: int) -> Optional[CalibrationRound]:
        logger.info("")
        logger.info("=" * 60)
        logger.info("  标定轮次: Source 并行度 = %d", source_parallelism)
        logger.info("=" * 60)

        # 1. 停 Producer
        self.producer.stop()

        # 2. Cancel 旧作业
        current_job = self.flink.find_running_job()
        if current_job:
            self.flink.cancel_job(current_job)

        # 3. 等待 slot
        self.flink.wait_slots_available()

        # 4. 清理旧 CG
        if self._last_consumer_group:
            self._cleanup_consumer_group(self._last_consumer_group)

        # 5. 等待 deregister
        logger.info("等待 %ds 让旧 Consumer Group 注销...", DEREGISTER_WAIT)
        time.sleep(DEREGISTER_WAIT)

        if not self._running:
            return None

        # 6. 提交作业
        program_args, consumer_group = self._build_program_args(source_parallelism)
        logger.info("Consumer Group: %s", consumer_group)

        new_job_id = self.flink.submit_job(self._jar_id, program_args)
        if not new_job_id:
            logger.error("作业提交失败，跳过本轮")
            return None

        # 7. 等待 RUNNING
        if not self.flink.wait_job_running(new_job_id):
            logger.error("作业启动失败，跳过本轮")
            return None

        self._last_consumer_group = consumer_group

        # ★ 首次发现 vertex（打印完整列表 + 诊断指标可用性）
        source = self.flink.discover_source_vertex(new_job_id)
        if not source:
            logger.error("未找到 Source 算子，跳过本轮")
            self.flink.cancel_job(new_job_id)
            return None

        actual_p = source.get("parallelism", 0)
        if actual_p != source_parallelism:
            logger.warning("Source 实际并行度 %d != 预期 %d",
                           actual_p, source_parallelism)

        # 8. 启动 Producer
        if not self.producer.start(self.producer_rate):
            logger.error("Producer 启动失败，跳过本轮")
            self.flink.cancel_job(new_job_id)
            return None

        # 9. 预热（带指标可用性检测）
        logger.info("预热阶段: 等待 %ds...", self.warmup_seconds)
        first_rate_seen = False
        for elapsed in range(0, self.warmup_seconds, 5):
            if not self._running:
                self.producer.stop()
                return None

            if not self.flink.find_running_job():
                logger.error("Flink 作业在预热期间退出！")
                self.producer.stop()
                return None

            if not self.producer.is_running():
                logger.error("Producer 在预热期间退出！")
                self.flink.cancel_job(new_job_id)
                return None

            # 尝试读指标
            rate = self.flink.get_source_output_rate(new_job_id)
            rate_str = f"{rate:.1f} r/s" if rate is not None else "N/A"

            if rate is not None and not first_rate_seen:
                first_rate_seen = True
                logger.info("  ✅ 首次获取到指标: %s (预热 %ds 后)", rate_str, elapsed)

            if elapsed % 15 == 0:
                logger.info("  预热中... %d/%ds, 速率: %s",
                            elapsed, self.warmup_seconds, rate_str)

            # ★ 预热 30s 仍然 N/A，提前触发诊断
            if elapsed == 30 and not first_rate_seen and not self._diagnosed:
                logger.warning("预热 30s 仍未获取到指标，触发诊断...")
                self.flink.diagnose_source_metrics(new_job_id)
                self._diagnosed = True

            time.sleep(5)

        # 10. 稳态采样
        samples = self._collect_samples(new_job_id)

        # 11. 停 Producer
        self.producer.stop()

        # 12. 排空
        if DRAIN_SECONDS > 0:
            logger.info("排空阶段: 等待 %ds...", DRAIN_SECONDS)
            time.sleep(DRAIN_SECONDS)

        # 分析
        result = self._analyze_samples(source_parallelism, samples, consumer_group)

        logger.info("")
        logger.info("-" * 50)
        logger.info("| 本轮结果: Source P=%d", source_parallelism)
        logger.info("|----------------------------------------------")
        logger.info("| Consumer Group   : %s", consumer_group)
        logger.info("| 有效样本数       : %d", result.valid_sample_count)
        logger.info("| 平均总吞吐       : %.1f r/s", result.total_output_rate)
        logger.info("| 单实例吞吐       : %.1f r/s", result.per_instance_rate)
        logger.info("| 标准差           : %.1f", result.std_dev)
        logger.info("| 范围             : [%.1f, %.1f]",
                     result.min_rate, result.max_rate)
        logger.info("-" * 50)

        return result

    # ---- 保存 ----

    def _save_results(self):
        if not self.results:
            logger.error("没有有效结果！")
            return

        csv_path = os.path.join(OUTPUT_DIR, "source_calibration.csv")

        with open(csv_path, "w", newline="", encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                "parallelism", "total_output_rate", "per_instance_rate",
                "std_dev", "min_rate", "max_rate", "valid_samples",
                "consumer_group", "timestamp"
            ])
            for r in self.results:
                writer.writerow([
                    r.parallelism,
                    f"{r.total_output_rate:.2f}",
                    f"{r.per_instance_rate:.2f}",
                    f"{r.std_dev:.2f}",
                    f"{r.min_rate:.2f}",
                    f"{r.max_rate:.2f}",
                    r.valid_sample_count,
                    r.consumer_group,
                    r.timestamp,
                ])
        logger.info("✅ 结果已保存: %s", csv_path)

    # ---- 汇总 ----

    def _print_summary(self):
        if not self.results:
            return

        per_rates = [r.per_instance_rate for r in self.results]
        sorted_rates = sorted(per_rates)
        n = len(sorted_rates)
        median = (
            sorted_rates[n // 2] if n % 2 == 1
            else (sorted_rates[n // 2 - 1] + sorted_rates[n // 2]) / 2
        )
        avg = sum(per_rates) / n

        logger.info("")
        logger.info("=" * 60)
        logger.info("  Source 容量标定 -- 最终汇总")
        logger.info("=" * 60)
        logger.info("")
        logger.info("  %6s  %12s  %12s  %8s  %s",
                     "并行度", "总吞吐(r/s)", "单实例(r/s)", "标准差", "范围")
        logger.info("  " + "-" * 65)

        for r in self.results:
            logger.info("  %6d  %12.1f  %12.1f  %8.1f  [%.0f, %.0f]",
                        r.parallelism, r.total_output_rate,
                        r.per_instance_rate, r.std_dev,
                        r.min_rate, r.max_rate)

        logger.info("  " + "-" * 65)
        logger.info("  均值:   %.0f r/s", avg)
        logger.info("  中位数: %.0f r/s", median)

        if n >= 2:
            cv = (max(per_rates) - min(per_rates)) / avg * 100
            if cv < 15:
                stability = "良好"
            elif cv < 30:
                stability = "一般"
            else:
                stability = "差"
            logger.info("  稳定性: %s (变异幅度 %.1f%%)", stability, cv)

        recommended = int(median)
        logger.info("")
        logger.info("  >>> 推荐 source_capacity_per_instance = %d <<<", recommended)
        logger.info("")

    # ---- 清理 ----

    def _final_cleanup(self):
        logger.info("开始清理...")
        self.producer.stop()
        self.flink.cancel_all_running()
        if self._last_consumer_group:
            time.sleep(5)
            self._cleanup_consumer_group(self._last_consumer_group)
        self.flink.close()
        logger.info("✅ 清理完成")

    # ---- 主流程 ----

    def run(self):
        logger.info("")
        logger.info("=" * 60)
        logger.info("  Source 容量标定实验（全自动）")
        logger.info("=" * 60)
        logger.info("")
        logger.info("  Flink REST         : %s", FLINK_REST_URL)
        logger.info("  Main class         : %s", MAIN_CLASS)
        logger.info("  SSH                : %s@%s:%d", SSH_USER, SSH_HOST, SSH_PORT)
        logger.info("  Consumer prefix    : %s", CONSUMER_GROUP_PREFIX)
        logger.info("  Producer class     : %s", PRODUCER_CLASS)
        logger.info("  Producer JAR       : %s", PRODUCER_JAR)
        logger.info("  Producer rate      : %d r/s", self.producer_rate)
        logger.info("  并行度列表         : %s", self.parallelism_list)
        logger.info("  预热               : %ds", self.warmup_seconds)
        logger.info("  采样               : %ds (间隔 %.0fs)",
                     self.sample_seconds, SAMPLE_INTERVAL)
        logger.info("  排空               : %ds", DRAIN_SECONDS)
        logger.info("  下游并行度         : %s", DOWNSTREAM_PARALLELISM)
        logger.info("  输出目录           : %s", OUTPUT_DIR)

        n = len(self.parallelism_list)
        per_round = (self.warmup_seconds + self.sample_seconds
                     + DEREGISTER_WAIT + DRAIN_SECONDS + 30)
        logger.info("  预计总耗时         : ~%dm (%d 轮 x ~%ds)",
                     n * per_round // 60, n, per_round)
        logger.info("")

        if not self.preflight_check():
            sys.exit(1)

        for i, p in enumerate(self.parallelism_list):
            if not self._running:
                logger.warning("实验被中断")
                break

            logger.info("")
            logger.info("【标定 %d/%d】Source P = %d", i + 1, n, p)

            try:
                result = self.run_one_round(p)
                if result and result.valid_sample_count > 0:
                    self.results.append(result)
                else:
                    logger.warning("并行度 %d 标定失败或无有效样本，跳过", p)
            except Exception as e:
                logger.error("轮次 p=%d 异常: %s", p, e, exc_info=True)

        self._final_cleanup()
        self._save_results()
        self._print_summary()


# ============================================================
# 入口
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="Source 算子单实例容量标定（全自动）")
    parser.add_argument("--parallelism-list", type=int, nargs="+",
                        default=None, help="Source 并行度列表")
    parser.add_argument("--warmup", type=int, default=None,
                        help="预热时间/秒")
    parser.add_argument("--sample", type=int, default=None,
                        help="采样时间/秒")
    parser.add_argument("--rate", type=int, default=None,
                        help="Producer 发送速率")

    args = parser.parse_args()

    calibrator = SourceCalibrator(
        parallelism_list=args.parallelism_list or PARALLELISM_LIST,
        warmup=args.warmup or WARMUP_SECONDS,
        sample=args.sample or SAMPLE_SECONDS,
        rate=args.rate or PRODUCER_RATE,
    )
    calibrator.run()


if __name__ == "__main__":
    main()
