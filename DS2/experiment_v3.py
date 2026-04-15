#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DS2 自动扩缩容 -- 三场景对比实验 (v3 - P10修订)
================================================
★ P10 修改点:
  1. 新增 fluctuate 综合测试场景 (高负载→低负载, 一次波动)
  2. 绘图/摘要适配 fluctuate 场景
  3. OUTPUT_ROOT 改为 experiment_results_p10
"""

import os
import sys
import time
import math
import re
import signal
import logging
import argparse
import subprocess
import csv
import uuid
import threading
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from collections import OrderedDict

import requests
import paramiko
import numpy as np

# ============================================================
# 硬编码配置区
# ============================================================

FLINK_REST_URL      = "http://192.168.56.151:8081"
FLINK_JOB_NAME      = "Seismic-Stream-Job"
JAR_PATH            = "/opt/flink_jobs/flink-rocketmq-demo-1.0-SNAPSHOT.jar"
MAIN_CLASS          = "org.jzx.SeismicStreamJob"
TARGET_JAR_NAME     = "flink-rocketmq-demo-1.0-SNAPSHOT"

SSH_HOST            = "192.168.56.151"
SSH_PORT            = 22
SSH_USER            = "root"
SSH_PASSWORD        = "123456"

ROCKETMQ_HOME       = "/usr/local/rocketmq"
ROCKETMQ_CLUSTER    = "RaftCluster"
ROCKETMQ_NAMESRV    = "192.168.56.151:9876"
CONSUMER_GROUP_PREFIX = "seismic-flink-consumer-group"

PRODUCER_CLASS      = "org.jzx.producer.HighThroughputSeismicProducer"
PRODUCER_JAR        = r"E:\Desktop\bs_code\flink-rocketmq-demo\target\flink-rocketmq-demo-1.0-SNAPSHOT.jar"
PRODUCER_WORK_DIR   = r"E:\Desktop\bs_code\flink-rocketmq-demo"
PRODUCER_NAMESRV    = "192.168.56.151:9876"

DS2_CONTROLLER_SCRIPT = "ds2_controller.py"
DS2_CONFIG_FILE       = "ds2_config.yaml"

SAMPLE_INTERVAL     = 5
JOB_WARMUP          = 60
CANCEL_TIMEOUT      = 60
SUBMIT_TIMEOUT      = 120
JOB_START_TIMEOUT   = 90

DS2_SCALING_TOLERANCE_S    = 120
DS2_SCALING_CHECK_INTERVAL = 5

OUTPUT_ROOT         = "experiment_results_p10"

LOG_FORMAT = "[%(asctime)s] %(levelname)-5s %(name)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("experiment")

SOURCE_PATTERN = re.compile(r'Source.*RocketMQ', re.IGNORECASE)


# ============================================================
# 场景配置
# ============================================================

@dataclass
class ScenarioConfig:
    name: str
    display_name: str
    description: str
    load_phases: List[Tuple[int, int]]
    fixed_parallelism: Dict[str, int]
    ds2_initial_parallelism: Dict[str, int]
    warmup: int = 60
    drain_enabled: bool = True
    drain_timeout: int = 600
    drain_idle_threshold: float = 500.0
    drain_stable_seconds: int = 15


SCENARIO_SCALE_UP = ScenarioConfig(
    name="scale_up",
    display_name="Scale-Up Test",
    description="Low P(2) + 6000 r/s x 15min, verify DS2 scale-up",
    load_phases=[
        (6000, 900),
    ],
    fixed_parallelism={
        "p-source": 4, "p-filter": 4, "p-signal": 4,
        "p-feature": 4, "p-grid": 4, "p-sink": 2,
    },
    ds2_initial_parallelism={
        "p-source": 2, "p-filter": 2, "p-signal": 2,
        "p-feature": 2, "p-grid": 2, "p-sink": 1,
    },
    drain_timeout=600,
    drain_idle_threshold=500.0,
    drain_stable_seconds=15,
)

SCENARIO_SCALE_DOWN = ScenarioConfig(
    name="scale_down0",
    display_name="Scale-Down Test",
    description="High P(6) + 2000 r/s x 10min, verify DS2 resource release",
    load_phases=[
        (2000, 600),
    ],
    fixed_parallelism={
        "p-source": 6, "p-filter": 6, "p-signal": 6,
        "p-feature": 6, "p-grid": 6, "p-sink": 3,
    },
    ds2_initial_parallelism={
        "p-source": 6, "p-filter": 6, "p-signal": 6,
        "p-feature": 6, "p-grid": 6, "p-sink": 3,
    },
    drain_timeout=300,
    drain_idle_threshold=500.0,
    drain_stable_seconds=15,
)

# ★ P10 新增: 综合测试 (高负载→低负载, 一次波动)
SCENARIO_FLUCTUATE = ScenarioConfig(
    name="fluctuate",
    display_name="Fluctuate Test",
    description="Low P(2), 6000 r/s x 10min -> 2000 r/s x 10min, verify scale-up then scale-down",
    load_phases=[
        (6000, 600),   # Phase 1: 高负载 10min, DS2 应扩容
        (2000, 600),   # Phase 2: 低负载 10min, DS2 应缩容
    ],
    fixed_parallelism={
        "p-source": 4, "p-filter": 4, "p-signal": 4,
        "p-feature": 4, "p-grid": 4, "p-sink": 2,
    },
    ds2_initial_parallelism={
        "p-source": 2, "p-filter": 2, "p-signal": 2,
        "p-feature": 2, "p-grid": 2, "p-sink": 1,
    },
    drain_timeout=600,
    drain_idle_threshold=500.0,
    drain_stable_seconds=15,
)

SCENARIOS: Dict[str, ScenarioConfig] = {
    "scale_up": SCENARIO_SCALE_UP,
    "scale_down0": SCENARIO_SCALE_DOWN,
    "fluctuate": SCENARIO_FLUCTUATE,
}


# ============================================================
# SSH 执行器
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

    METRICS = ["numRecordsIn", "numRecordsOut", "accumulateBusyTimeMs"]

    def __init__(self):
        self.rest_url = FLINK_REST_URL.rstrip('/')
        self._session = requests.Session()
        self._session.headers.update({'Accept': 'application/json'})
        self._timeout = 15

    def _get(self, path: str) -> Optional[dict]:
        try:
            resp = self._session.get(
                f"{self.rest_url}{path}", timeout=self._timeout)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return None

    def check_connection(self) -> bool:
        data = self._get("/overview")
        if data:
            logger.info("Flink cluster OK: version=%s, slots=%s, taskmanagers=%s",
                        data.get('flink-version', '?'),
                        data.get('slots-total', '?'),
                        data.get('taskmanagers', '?'))
            return True
        logger.error("Cannot connect Flink: %s", self.rest_url)
        return False

    def ensure_jar_uploaded(self, ssh: SSHExecutor) -> Optional[str]:
        try:
            jars = self._get("/jars")
            for f in jars.get("files", []):
                if ('rocketmq-demo' in f['name'].lower()
                        or 'seismic' in f['name'].lower()):
                    logger.info("Found JAR: %s -> %s", f['name'], f["id"])
                    return f["id"]
        except Exception:
            pass

        logger.info("Uploading JAR via SSH: %s", JAR_PATH)
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
                        logger.info("JAR uploaded: %s", f["id"])
                        return f["id"]
            logger.error("JAR upload failed: %s", out[:300])
        except Exception as e:
            logger.error("JAR upload error: %s", e)
        return None

    def find_running_job(self) -> Optional[str]:
        try:
            data = self._get("/jobs/overview")
            if not data:
                return None
            for job in data.get('jobs', []):
                if (job.get('name') == FLINK_JOB_NAME
                        and job.get('state') == 'RUNNING'):
                    return job['jid']
        except Exception:
            pass
        return None

    def get_vertices(self, job_id: str) -> List[dict]:
        data = self._get(f"/jobs/{job_id}")
        return data.get('vertices', []) if data else []

    def get_vertex_metrics(self, job_id: str, vertex_id: str) -> dict:
        params = "?get=" + ",".join(self.METRICS)
        endpoint = f"/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics"
        data = self._get(endpoint + params)
        if not data:
            return {}

        result = {m: [] for m in self.METRICS}

        def safe_float(val):
            try:
                v = float(str(val).strip())
                return v if not (math.isnan(v) or math.isinf(v)) else 0.0
            except (ValueError, TypeError):
                return 0.0

        if isinstance(data, list) and data and 'metrics' in data[0]:
            for subtask in data:
                row = {}
                for m in subtask.get('metrics', []):
                    row[m['id']] = safe_float(m.get('value', 0))
                for metric in self.METRICS:
                    result[metric].append(row.get(metric, 0.0))
        else:
            for m in data:
                mid = m.get('id', '')
                if mid in self.METRICS:
                    result[mid] = [safe_float(m.get('sum', m.get('value', 0)))]

        return result

    def get_source_output_rate(self, job_id: str) -> Optional[float]:
        vertices = self.get_vertices(job_id)
        source = None
        for v in vertices:
            if SOURCE_PATTERN.search(v.get("name", "")):
                source = v
                break
        if not source:
            return None

        data = self._get(
            f"/jobs/{job_id}/vertices/{source['id']}"
            f"/subtasks/metrics?get=numRecordsOutPerSecond"
        )
        if not data:
            return None
        for item in data:
            if item.get("id") == "numRecordsOutPerSecond":
                sum_val = float(item.get("sum", 0))
                if sum_val > 0:
                    return sum_val
                avg_val = float(item.get("avg", 0))
                if avg_val > 0:
                    return avg_val * source.get("parallelism", 1)
        return None

    def submit_job(self, jar_id: str, program_args: str) -> Optional[str]:
        logger.info("Submitting job:")
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
                logger.info("[OK] Job submitted: %s", job_id)
                return job_id

            errors = body.get("errors", [])
            if errors:
                root = errors[0] if isinstance(errors[0], str) else str(errors[0])
                for line in root.split('\n'):
                    line = line.strip()
                    if line and not line.startswith('at '):
                        logger.error("[FAIL] Submit [%d]: %s",
                                     resp.status_code, line[:300])
                        break
            else:
                logger.error("[FAIL] Submit [%d]: %s",
                             resp.status_code, str(body)[:300])
            return None
        except Exception as e:
            logger.error("Submit error: %s", e)
            return None

    def wait_job_running(self, job_id: str,
                         timeout: int = JOB_START_TIMEOUT) -> bool:
        for i in range(timeout // 3):
            time.sleep(3)
            try:
                data = self._get(f"/jobs/{job_id}")
                if not data:
                    continue
                state = data.get("state", "UNKNOWN")
                if state == "RUNNING":
                    logger.info("[OK] Job RUNNING: %s", job_id)
                    return True
                if state in ["FAILED", "CANCELED", "CANCELLED"]:
                    try:
                        exc = self._get(f"/jobs/{job_id}/exceptions")
                        root = exc.get("root-exception", "unknown")
                        logger.error("Job %s: %s", state, root[:300])
                    except Exception:
                        logger.error("Job terminal state: %s", state)
                    return False
            except Exception:
                pass
        logger.error("Wait RUNNING timeout (%ds)", timeout)
        return False

    def cancel_job(self, job_id: str) -> bool:
        logger.info("Cancel job: %s", job_id)
        try:
            self._session.patch(
                f"{self.rest_url}/jobs/{job_id}", timeout=self._timeout)
        except Exception as e:
            logger.error("Cancel request error: %s", e)

        for i in range(CANCEL_TIMEOUT // 2):
            time.sleep(2)
            try:
                data = self._get(f"/jobs/{job_id}")
                if data and data.get("state") not in [
                    "RUNNING", "CANCELLING", "FAILING"
                ]:
                    logger.info("Job stopped: %s", data.get("state"))
                    return True
            except Exception:
                pass
        logger.error("Cancel timeout")
        return False

    def cancel_all_running(self):
        while True:
            job_id = self.find_running_job()
            if not job_id:
                break
            self.cancel_job(job_id)

    def wait_slots_available(self, min_slots: int = 4, timeout: int = 30):
        for _ in range(timeout // 2):
            time.sleep(2)
            try:
                overview = self._get("/overview")
                if overview and overview.get("slots-available", 0) >= min_slots:
                    return
            except Exception:
                pass
        logger.warning("Wait slots timeout, continue")

    def close(self):
        self._session.close()


# ============================================================
# Producer 管理器
# ============================================================
class ProducerManager:

    def __init__(self):
        self._process: Optional[subprocess.Popen] = None
        self._log_handle = None
        self._current_rate: int = 0

    def start(self, rate: int, log_dir: str = None) -> bool:
        self.stop()

        if not os.path.exists(PRODUCER_JAR):
            logger.error("Producer JAR not found: %s", PRODUCER_JAR)
            return False

        if log_dir is None:
            log_dir = os.path.join(OUTPUT_ROOT, "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, f"producer_{rate}.log")

        cmd = [
            'java', '-cp', PRODUCER_JAR,
            PRODUCER_CLASS,
            str(rate),
            PRODUCER_NAMESRV,
        ]

        logger.info("Starting Producer: rate=%d r/s", rate)

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
                logger.error("Producer exited immediately, rc=%d",
                             self._process.returncode)
                self._process = None
                return False

            self._current_rate = rate
            logger.info("[OK] Producer PID=%d, rate=%d",
                        self._process.pid, rate)
            return True
        except Exception as e:
            logger.error("Producer start failed: %s", e)
            self._process = None
            return False

    def change_rate(self, new_rate: int, log_dir: str = None) -> bool:
        logger.info("Producer rate change: %d -> %d r/s",
                    self._current_rate, new_rate)
        return self.start(new_rate, log_dir)

    def stop(self):
        if self._process is not None and self._process.poll() is None:
            logger.info("Stopping Producer: PID=%d", self._process.pid)
            try:
                self._process.terminate()
                self._process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)
            except Exception as e:
                logger.warning("Stop Producer error: %s", e)

        self._process = None
        self._current_rate = 0

        if self._log_handle is not None:
            try:
                self._log_handle.close()
            except Exception:
                pass
            self._log_handle = None

    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None


# ============================================================
# 指标采集器
# ============================================================
class MetricsSampler:

    def __init__(self, flink: FlinkClient, output_path: str,
                 interval: int = SAMPLE_INTERVAL):
        self.flink = flink
        self.output_path = output_path
        self.interval = interval

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._prev_snapshot: Dict[str, Dict[str, float]] = {}
        self._prev_time: float = 0.0
        self._start_time: float = 0.0
        self._header_written = False
        self._current_phase: str = ""
        self._current_rate: int = 0
        self._lock = threading.Lock()

    def set_phase(self, phase: str, rate: int):
        with self._lock:
            self._current_phase = phase
            self._current_rate = rate

    def start(self):
        self._running = True
        self._start_time = time.time()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("Sampler started -> %s", self.output_path)

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=15)
        logger.info("Sampler stopped")

    def reset_baseline(self):
        self._prev_snapshot.clear()
        self._prev_time = 0.0

    def _loop(self):
        while self._running:
            try:
                self._sample_once()
            except Exception as e:
                logger.debug("Sample error: %s", e)
            time.sleep(self.interval)

    def _sample_once(self):
        job_id = self.flink.find_running_job()
        if not job_id:
            return

        vertices = self.flink.get_vertices(job_id)
        if not vertices:
            return

        now = time.time()
        current_snapshot: Dict[str, Dict[str, float]] = {}
        rows = []

        with self._lock:
            phase = self._current_phase
            target_rate = self._current_rate

        max_parallelism = max(
            (v['parallelism'] for v in vertices), default=0
        )

        for vertex in vertices:
            vid = vertex['id']
            vname = vertex['name']
            parallelism = vertex['parallelism']

            raw = self.flink.get_vertex_metrics(job_id, vid)
            if not raw:
                continue

            total_in = sum(raw.get("numRecordsIn", [0.0]))
            total_out = sum(raw.get("numRecordsOut", [0.0]))
            total_busy = sum(raw.get("accumulateBusyTimeMs", [0.0]))

            current_snapshot[vid] = {
                "in": total_in, "out": total_out, "busy": total_busy,
            }

            in_rate = out_rate = busy_ratio = 0.0

            if self._prev_time > 0 and vid in self._prev_snapshot:
                dt = now - self._prev_time
                if dt >= 1.0:
                    prev = self._prev_snapshot[vid]
                    in_rate = max(total_in - prev["in"], 0.0) / dt
                    out_rate = max(total_out - prev["out"], 0.0) / dt
                    delta_busy = max(total_busy - prev["busy"], 0.0)
                    busy_ratio = min(
                        delta_busy / (dt * 1000.0 * max(parallelism, 1)),
                        1.0
                    )

            rows.append({
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "elapsed_s": round(now - self._start_time, 1),
                "phase": phase,
                "target_rate": target_rate,
                "vertex_name": vname,
                "parallelism": parallelism,
                "slot_usage": max_parallelism,
                "in_rate": round(in_rate, 2),
                "out_rate": round(out_rate, 2),
                "busy_ratio": round(busy_ratio, 4),
            })

        self._prev_snapshot = current_snapshot
        self._prev_time = now

        if rows:
            self._write_csv(rows)

    def _write_csv(self, rows: List[dict]):
        try:
            need_header = not self._header_written
            with open(self.output_path, 'a', newline='',
                      encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                if need_header:
                    writer.writeheader()
                    self._header_written = True
                writer.writerows(rows)
        except Exception as e:
            logger.warning("CSV write failed: %s", e)


# ============================================================
# 场景实验执行器
# ============================================================
class ScenarioRunner:

    def __init__(self, scenario: ScenarioConfig):
        self.scenario = scenario
        self.flink = FlinkClient()
        self.ssh = SSHExecutor()
        self.producer = ProducerManager()
        self._jar_id: Optional[str] = None
        self._running = True

        self.output_dir = os.path.join(OUTPUT_ROOT, scenario.name)
        self.log_dir = os.path.join(self.output_dir, "logs")
        os.makedirs(self.log_dir, exist_ok=True)

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.warning("Signal %d received, cleaning up...", signum)
        self._running = False

    def preflight_check(self) -> bool:
        logger.info("=" * 60)
        logger.info("Preflight check")
        logger.info("=" * 60)

        if not self.flink.check_connection():
            return False

        self._jar_id = self.flink.ensure_jar_uploaded(self.ssh)
        if not self._jar_id:
            return False

        if not os.path.exists(PRODUCER_JAR):
            logger.error("Producer JAR not found: %s", PRODUCER_JAR)
            return False

        try:
            out = self.ssh.exec_command("echo OK", timeout=10)
            if "OK" in out:
                logger.info("SSH OK: %s@%s", SSH_USER, SSH_HOST)
        except Exception as e:
            logger.warning("SSH failed: %s", e)

        running = self.flink.find_running_job()
        if running:
            logger.warning("Found running job %s, cancelling...", running)
            self.flink.cancel_all_running()
            time.sleep(5)

        logger.info("Preflight OK")
        return True

    def _build_program_args(self, parallelism: Dict[str, int],
                            suffix: str) -> str:
        parts = [
            f"--consumer-group-suffix {suffix}",
            "--consume-from-latest true",
        ]
        for k, v in sorted(parallelism.items()):
            parts.append(f"--{k} {v}")
        return " ".join(parts)

    def _submit_and_wait(self, parallelism: Dict[str, int],
                         suffix: str) -> Optional[str]:
        program_args = self._build_program_args(parallelism, suffix)
        consumer_group = f"{CONSUMER_GROUP_PREFIX}{suffix}"
        logger.info("Consumer Group: %s", consumer_group)

        job_id = self.flink.submit_job(self._jar_id, program_args)
        if not job_id:
            return None

        if not self.flink.wait_job_running(job_id):
            return None

        vertices = self.flink.get_vertices(job_id)
        logger.info("Job vertices (%d):", len(vertices))
        for v in vertices:
            logger.info("  [%s] P=%d  %s",
                        v["id"][:8], v.get("parallelism", 0),
                        v.get("name", ""))

        return job_id

    def _cleanup_consumer_group(self, suffix: str):
        group_name = f"{CONSUMER_GROUP_PREFIX}{suffix}"
        logger.info("Cleanup Consumer Group: %s", group_name)
        cmd = (
            f"sh {ROCKETMQ_HOME}/bin/mqadmin deleteSubGroup "
            f"-n {ROCKETMQ_NAMESRV} "
            f"-c {ROCKETMQ_CLUSTER} "
            f"-g {group_name} || true"
        )
        try:
            out = self.ssh.exec_command(cmd)
            logger.info("Cleanup done: %s", out[:200] if out else "(empty)")
        except Exception as e:
            logger.warning("Cleanup failed (non-fatal): %s", e)

    def _full_cleanup(self, suffix: str):
        self.producer.stop()
        self.flink.cancel_all_running()
        time.sleep(5)
        self._cleanup_consumer_group(suffix)

    def _get_parallelism_str(self, job_id: str) -> str:
        vertices = self.flink.get_vertices(job_id)
        parts = []
        for v in vertices:
            short = v['name'].split(':')[0].strip().split('-')[0]
            parts.append(f"{short}={v['parallelism']}")
        return ", ".join(parts)[:100]

    def _run_phases(self, phases: List[Tuple[int, int]],
                    sampler: MetricsSampler,
                    allow_scaling: bool) -> bool:

        job_missing_since: Optional[float] = None

        for i, (rate, duration) in enumerate(phases):
            if not self._running:
                return False

            phase_label = f"phase_{i + 1}"

            logger.info("")
            logger.info("=" * 60)
            logger.info("  Phase %d/%d: %d r/s x %ds",
                        i + 1, len(phases), rate, duration)
            logger.info("=" * 60)

            sampler.set_phase(phase_label, rate)

            if not self.producer.change_rate(rate, self.log_dir):
                logger.error("Producer rate change failed, abort")
                return False

            elapsed = 0
            while elapsed < duration:
                if not self._running:
                    return False

                job_id = self.flink.find_running_job()

                if not job_id:
                    if allow_scaling:
                        if job_missing_since is None:
                            job_missing_since = time.time()
                            logger.warning(
                                "  [!] Job not visible (DS2 scaling), "
                                "waiting (max %ds)...",
                                DS2_SCALING_TOLERANCE_S
                            )
                        missing_duration = time.time() - job_missing_since
                        if missing_duration > DS2_SCALING_TOLERANCE_S:
                            logger.error("  [X] Job missing > %ds!",
                                         DS2_SCALING_TOLERANCE_S)
                            return False
                        time.sleep(DS2_SCALING_CHECK_INTERVAL)
                        continue
                    else:
                        logger.error("Flink job exited during run!")
                        return False
                else:
                    if job_missing_since is not None:
                        recover_time = time.time() - job_missing_since
                        logger.info("  [OK] Job recovered (missing %.1fs), new JobID=%s",
                                    recover_time, job_id)
                        sampler.reset_baseline()
                        job_missing_since = None

                if not self.producer.is_running():
                    logger.error("Producer exited during run!")
                    return False

                if elapsed % 30 == 0:
                    source_rate = self.flink.get_source_output_rate(job_id)
                    rate_str = (f"{source_rate:.1f} r/s"
                                if source_rate is not None else "N/A")
                    p_str = self._get_parallelism_str(job_id)
                    logger.info(
                        "  [%s] %d/%ds, target=%d, actual=%s, P=[%s]",
                        phase_label, elapsed, duration, rate, rate_str, p_str
                    )

                step = min(10, duration - elapsed)
                time.sleep(step)
                elapsed += step

        return True

    def _wait_for_drain(self, sampler: MetricsSampler,
                        allow_scaling: bool) -> bool:
        cfg = self.scenario
        if not cfg.drain_enabled:
            return True

        max_wait = cfg.drain_timeout
        idle_threshold = cfg.drain_idle_threshold
        stable_seconds = cfg.drain_stable_seconds

        logger.info("")
        logger.info("=" * 60)
        logger.info("  Drain phase (DS2 already stopped)")
        logger.info("  max %ds, idle threshold < %.0f r/s, stable %ds",
                     max_wait, idle_threshold, stable_seconds)
        logger.info("=" * 60)

        self.producer.stop()
        sampler.set_phase("drain", 0)

        start = time.time()
        job_missing_since = None

        check_interval = 10
        total_checks = 0
        idle_checks = 0
        required_checks = max(int(stable_seconds / check_interval), 2)
        required_ratio = 0.70

        while time.time() - start < max_wait:
            if not self._running:
                return False

            job_id = self.flink.find_running_job()
            if not job_id:
                if job_missing_since is None:
                    job_missing_since = time.time()
                    logger.info("  [drain] Job not visible, waiting...")
                if time.time() - job_missing_since > 30:
                    logger.warning("  [drain] Job gone > 30s, drain done (no data)")
                    return True
                time.sleep(5)
                continue

            if job_missing_since is not None:
                logger.info("  [drain] Job recovered")
                sampler.reset_baseline()
                job_missing_since = None

            source_rate = self.flink.get_source_output_rate(job_id)
            elapsed_drain = time.time() - start

            if source_rate is not None:
                total_checks += 1

                if source_rate < idle_threshold:
                    idle_checks += 1

                if total_checks >= required_checks:
                    ratio = idle_checks / total_checks
                    if ratio >= required_ratio:
                        logger.info(
                            "  [OK] Drain complete! "
                            "(idle %d/%d checks = %.0f%%, total %.0fs)",
                            idle_checks, total_checks,
                            ratio * 100, elapsed_drain
                        )
                        return True

                if int(elapsed_drain) % 30 == 0:
                    ratio_str = ("%.0f%%" % (idle_checks / max(total_checks, 1) * 100)
                                 if total_checks > 0 else "N/A")
                    p_str = self._get_parallelism_str(job_id)
                    logger.info(
                        "  [drain] %.0fs, Source=%.0f r/s, "
                        "idle=%d/%d (%s), need %d checks @ %.0f%%, P=[%s]",
                        elapsed_drain, source_rate,
                        idle_checks, total_checks, ratio_str,
                        required_checks, required_ratio * 100,
                        p_str
                    )

            time.sleep(check_interval)

        logger.warning(
            "  [!] Drain timeout (%ds), idle=%d/%d",
            max_wait, idle_checks, total_checks)
        return False

    # -- run fixed --

    def run_fixed(self) -> str:
        logger.info("")
        logger.info("#" * 60)
        logger.info("#  [%s] Fixed Parallelism", self.scenario.display_name)
        logger.info("#" * 60)

        csv_path = os.path.join(self.output_dir, "fixed.csv")
        if os.path.exists(csv_path):
            os.remove(csv_path)

        suffix = f"_{self.scenario.name}_fixed_{uuid.uuid4().hex[:6]}"

        self.flink.cancel_all_running()
        self.flink.wait_slots_available()

        logger.info("Submit fixed parallelism job: %s", self.scenario.fixed_parallelism)
        job_id = self._submit_and_wait(
            self.scenario.fixed_parallelism, suffix
        )
        if not job_id:
            logger.error("Fixed job submit failed")
            return csv_path

        logger.info("Warmup %ds ...", self.scenario.warmup)
        time.sleep(self.scenario.warmup)

        sampler = MetricsSampler(self.flink, csv_path)
        sampler.start()

        try:
            self._run_phases(
                self.scenario.load_phases, sampler, allow_scaling=False
            )
            self._wait_for_drain(sampler, allow_scaling=False)
        finally:
            sampler.stop()
            self.producer.stop()

        self._full_cleanup(suffix)
        logger.info("[OK] Fixed group done, CSV: %s", csv_path)
        return csv_path

    # -- run ds2 --

    def run_ds2(self) -> str:
        logger.info("")
        logger.info("#" * 60)
        logger.info("#  [%s] DS2 Auto-Scaling", self.scenario.display_name)
        logger.info("#" * 60)

        csv_path = os.path.join(self.output_dir, "ds2.csv")
        if os.path.exists(csv_path):
            os.remove(csv_path)

        suffix = f"_{self.scenario.name}_ds2_{uuid.uuid4().hex[:6]}"
        ds2_process = None
        ds2_log_handle = None

        self.flink.cancel_all_running()
        self.flink.wait_slots_available()

        logger.info("Submit DS2 initial parallelism job: %s",
                     self.scenario.ds2_initial_parallelism)
        job_id = self._submit_and_wait(
            self.scenario.ds2_initial_parallelism, suffix
        )
        if not job_id:
            logger.error("DS2 job submit failed")
            return csv_path

        logger.info("Warmup %ds ...", self.scenario.warmup)
        time.sleep(self.scenario.warmup)

        # start DS2 controller
        ds2_log = os.path.join(self.log_dir, "ds2_controller.log")
        logger.info("Starting DS2 controller: %s -c %s",
                     DS2_CONTROLLER_SCRIPT, DS2_CONFIG_FILE)

        ds2_log_handle = open(ds2_log, 'w', encoding='utf-8')
        ds2_process = subprocess.Popen(
            [sys.executable, DS2_CONTROLLER_SCRIPT,
             "-c", DS2_CONFIG_FILE,
             "--consumer-group-suffix", suffix],
            stdout=ds2_log_handle,
            stderr=subprocess.STDOUT,
        )

        logger.info("[OK] DS2 Controller PID=%d, log=%s",
                     ds2_process.pid, ds2_log)
        time.sleep(5)

        if ds2_process.poll() is not None:
            logger.error("DS2 controller exited immediately!")
            self._full_cleanup(suffix)
            return csv_path

        sampler = MetricsSampler(self.flink, csv_path)
        sampler.start()

        try:
            self._run_phases(
                self.scenario.load_phases, sampler, allow_scaling=True
            )

            # drain 前先停 DS2 控制器
            logger.info("")
            logger.info("=" * 60)
            logger.info("  Stopping DS2 controller BEFORE drain ...")
            logger.info("=" * 60)
            if ds2_process and ds2_process.poll() is None:
                try:
                    ds2_process.terminate()
                    ds2_process.wait(timeout=15)
                    logger.info("  [OK] DS2 controller stopped")
                except Exception:
                    ds2_process.kill()
                    logger.warning("  DS2 controller killed")
            ds2_process = None

            self._wait_for_drain(sampler, allow_scaling=False)

        finally:
            sampler.stop()
            self.producer.stop()

            if ds2_process and ds2_process.poll() is None:
                logger.info("Stopping DS2 controller (fallback) ...")
                try:
                    ds2_process.terminate()
                    ds2_process.wait(timeout=15)
                except Exception:
                    ds2_process.kill()
            if ds2_log_handle:
                ds2_log_handle.close()

        self._full_cleanup(suffix)
        logger.info("[OK] DS2 group done, CSV: %s", csv_path)
        return csv_path

    def run(self, mode: str = "both"):
        sc = self.scenario
        total_load_s = sum(d for _, d in sc.load_phases)

        logger.info("")
        logger.info("=" * 60)
        logger.info("  Scenario: %s", sc.display_name)
        logger.info("  %s", sc.description)
        logger.info("=" * 60)
        logger.info("  Output     : %s", self.output_dir)
        logger.info("  Mode       : %s", mode)
        logger.info("  Warmup     : %ds", sc.warmup)
        logger.info("  Load phases:")
        for i, (r, d) in enumerate(sc.load_phases):
            logger.info("    Phase%d: %6d r/s x %ds", i + 1, r, d)
        logger.info("  Drain      : %s (max %ds, threshold=%.0f, stable=%ds)",
                     "enabled" if sc.drain_enabled else "disabled",
                     sc.drain_timeout, sc.drain_idle_threshold,
                     sc.drain_stable_seconds)
        logger.info("  Fixed P    : %s", sc.fixed_parallelism)
        logger.info("  DS2 init P : %s", sc.ds2_initial_parallelism)
        logger.info("  Load total : %.1f min", total_load_s / 60.0)
        logger.info("=" * 60)

        if not self.preflight_check():
            sys.exit(1)

        try:
            if mode in ("both", "fixed"):
                self.run_fixed()

                if mode == "both":
                    logger.info("Fixed done, wait 30s before DS2 ...")
                    time.sleep(30)

            if mode in ("both", "ds2"):
                self.run_ds2()

        except Exception as e:
            logger.error("Experiment error: %s", e, exc_info=True)
            self.producer.stop()
            self.flink.cancel_all_running()

        logger.info("")
        logger.info("Generating [%s] plots ...", sc.name)
        generate_scenario_plots(self.output_dir, sc)

        self.flink.close()

        logger.info("")
        logger.info("=" * 60)
        logger.info("  Scenario [%s] done!", sc.display_name)
        logger.info("  Results: %s/", self.output_dir)
        logger.info("=" * 60)


# ============================================================
# Visualization
# ============================================================

def load_csv_data(path: str) -> List[dict]:
    if not os.path.exists(path):
        return []
    with open(path, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def extract_series(data, vertex_keyword, field_name):
    times, values = [], []
    for row in data:
        if vertex_keyword in row.get('vertex_name', ''):
            try:
                t = float(row.get('elapsed_s', 0))
                v = float(row.get(field_name, 0))
                times.append(t / 60.0)
                values.append(v)
            except (ValueError, TypeError):
                pass
    return times, values


def extract_slot_usage(data):
    if not data:
        return [], []
    time_slots = OrderedDict()
    for row in data:
        try:
            t = round(float(row.get('elapsed_s', 0)), 0)
            s = int(row.get('slot_usage', 0))
            time_slots[t] = s
        except (ValueError, TypeError):
            pass
    return ([k / 60.0 for k in time_slots.keys()],
            list(time_slots.values()))


def compute_stats(data: List[dict]) -> Optional[dict]:
    if not data:
        return None

    source_rates = [
        float(r.get('out_rate', 0)) for r in data
        if 'Source' in r.get('vertex_name', '')
        and float(r.get('out_rate', 0)) > 0
    ]
    busy_vals = [
        float(r.get('busy_ratio', 0)) for r in data
        if float(r.get('busy_ratio', 0)) > 0
    ]

    time_slots = OrderedDict()
    for r in data:
        try:
            t = round(float(r.get('elapsed_s', 0)), 0)
            s = int(r.get('slot_usage', 0))
            time_slots[t] = s
        except (ValueError, TypeError):
            pass
    slot_vals = list(time_slots.values()) if time_slots else [0]

    op_p_ranges = {}
    for r in data:
        vname = r.get('vertex_name', '')
        try:
            p = int(r.get('parallelism', 0))
            if vname not in op_p_ranges:
                op_p_ranges[vname] = []
            op_p_ranges[vname].append(p)
        except (ValueError, TypeError):
            pass

    op_summary = {}
    for vname, ps in op_p_ranges.items():
        short = vname.split(':')[0].strip()
        op_summary[short] = {
            "min": min(ps), "max": max(ps),
            "changes": len(set(ps)) > 1,
        }

    slot_minutes = sum(slot_vals) * SAMPLE_INTERVAL / 60.0

    return {
        "avg_thru": np.mean(source_rates) if source_rates else 0,
        "max_thru": max(source_rates) if source_rates else 0,
        "avg_busy": np.mean(busy_vals) if busy_vals else 0,
        "avg_slots": np.mean(slot_vals),
        "max_slots": max(slot_vals),
        "min_slots": min(slot_vals),
        "slot_minutes": slot_minutes,
        "op_summary": op_summary,
    }


def _find_reach_time(data: List[dict], threshold: float) -> Optional[float]:
    consecutive = 0
    for row in data:
        if 'Source' in row.get('vertex_name', ''):
            try:
                rate = float(row.get('out_rate', 0))
                t = float(row.get('elapsed_s', 0))
                if rate >= threshold:
                    consecutive += 1
                    if consecutive >= 3:
                        return t
                else:
                    consecutive = 0
            except (ValueError, TypeError):
                pass
    return None


def _compute_phase_stats(data: List[dict], phase_label: str) -> Optional[dict]:
    """★ P10: 按阶段计算统计数据, 用于 fluctuate 场景"""
    phase_data = [r for r in data if r.get('phase', '') == phase_label]
    if not phase_data:
        return None
    return compute_stats(phase_data)


def generate_scenario_plots(output_dir: str, scenario: ScenarioConfig):
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        logger.warning("matplotlib not installed, skip plotting")
        return

    fixed_data = load_csv_data(os.path.join(output_dir, "fixed.csv"))
    ds2_data = load_csv_data(os.path.join(output_dir, "ds2.csv"))

    if not fixed_data and not ds2_data:
        logger.warning("No data, skip plotting")
        return

    logger.info("Data: fixed=%d rows, ds2=%d rows",
                len(fixed_data), len(ds2_data))

    fig, axes = plt.subplots(3, 2, figsize=(16, 18))
    fig.suptitle(
        f'{scenario.display_name}\n{scenario.description}',
        fontsize=14, fontweight='bold'
    )

    # 1. Source throughput
    ax = axes[0][0]
    if fixed_data:
        t, v = extract_series(fixed_data, 'Source', 'out_rate')
        ax.plot(t, v, 'b-', alpha=0.7, label='Fixed', linewidth=1.2)
    if ds2_data:
        t, v = extract_series(ds2_data, 'Source', 'out_rate')
        ax.plot(t, v, 'r-', alpha=0.7, label='DS2', linewidth=1.2)

    cum_t = 0
    for rate, dur in scenario.load_phases:
        t0 = cum_t / 60.0
        t1 = (cum_t + dur) / 60.0
        ax.hlines(rate, t0, t1, colors='green', linestyles='--',
                  alpha=0.5, linewidth=1.5)
        cum_t += dur

    ax.set_title('Source Output Rate')
    ax.set_xlabel('Time (min)')
    ax.set_ylabel('Records/s')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 2. Busy Ratio
    ax = axes[0][1]
    op_colors = [('Filter', 'green'), ('Signal', 'blue'),
                 ('Feature', 'orange'), ('Grid', 'red')]
    if fixed_data:
        for kw, color in op_colors:
            t, v = extract_series(fixed_data, kw, 'busy_ratio')
            if t:
                ax.plot(t, v, color=color, alpha=0.3,
                        linestyle='--', linewidth=1,
                        label=f'{kw}(Fixed)')
    if ds2_data:
        for kw, color in op_colors:
            t, v = extract_series(ds2_data, kw, 'busy_ratio')
            if t:
                ax.plot(t, v, color=color, alpha=0.7,
                        linewidth=1, label=f'{kw}(DS2)')
    ax.set_title('Operator Busy Ratio')
    ax.set_xlabel('Time (min)')
    ax.set_ylabel('Busy Ratio')
    ax.set_ylim(-0.05, 1.05)
    ax.legend(fontsize=6, ncol=2)
    ax.grid(True, alpha=0.3)

    # 3. Slot usage
    ax = axes[1][0]
    fs = compute_stats(fixed_data)
    ds = compute_stats(ds2_data)

    if fixed_data:
        t, v = extract_slot_usage(fixed_data)
        ax.plot(t, v, 'b-', label='Fixed', linewidth=2, drawstyle='steps-post')
        ax.fill_between(t, v, alpha=0.1, color='blue', step='post')
    if ds2_data:
        t, v = extract_slot_usage(ds2_data)
        ax.plot(t, v, 'r-', label='DS2', linewidth=2, drawstyle='steps-post')
        ax.fill_between(t, v, alpha=0.1, color='red', step='post')

    title_extra = ""
    if fs and ds:
        saved = fs.get('slot_minutes', 0) - ds.get('slot_minutes', 0)
        if abs(saved) > 0.1:
            direction = 'saved' if saved > 0 else 'extra'
            title_extra = f" (DS2 {direction} {abs(saved):.1f} slot*min)"
    ax.set_title(f'Slot Usage{title_extra}')
    ax.set_xlabel('Time (min)')
    ax.set_ylabel('Slots')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 4. Fixed parallelism
    ax = axes[1][1]
    all_ops = [('Source', 'purple'), ('Filter', 'green'),
               ('Signal', 'blue'), ('Feature', 'orange'),
               ('Grid', 'red'), ('Sink', 'brown')]
    if fixed_data:
        for kw, color in all_ops:
            t, v = extract_series(fixed_data, kw, 'parallelism')
            if t:
                ax.plot(t, v, color=color, alpha=0.7, label=kw,
                        linewidth=1.5, drawstyle='steps-post')
    ax.set_title('Operator Parallelism (Fixed)')
    ax.set_xlabel('Time (min)')
    ax.set_ylabel('Parallelism')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    # 5. DS2 parallelism
    ax = axes[2][0]
    if ds2_data:
        for kw, color in all_ops:
            t, v = extract_series(ds2_data, kw, 'parallelism')
            if t:
                ax.plot(t, v, color=color, alpha=0.7, label=kw,
                        linewidth=1.5, drawstyle='steps-post')
    ax.set_title('Operator Parallelism (DS2 Auto-Scaling)')
    ax.set_xlabel('Time (min)')
    ax.set_ylabel('Parallelism')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    # 6. Scenario-specific panel
    ax = axes[2][1]
    if scenario.name == "scale_up":
        target = scenario.load_phases[0][0]
        for label, data, color in [("Fixed", fixed_data, 'blue'),
                                     ("DS2", ds2_data, 'red')]:
            if not data:
                continue
            t, v = extract_series(data, 'Source', 'out_rate')
            if t and v:
                ratios = [min(val / max(target, 1), 1.5) for val in v]
                ax.plot(t, ratios, color=color, alpha=0.7,
                        label=f'{label}', linewidth=1.2)
        ax.axhline(y=1.0, color='green', linestyle='--', alpha=0.5,
                   label='100%')
        ax.set_title(f'Throughput Achievement (target {target} r/s)')
        ax.set_ylabel('actual / target')
        ax.set_ylim(-0.05, 1.55)

    elif scenario.name == "scale_down0":
        for label, data, color in [("Fixed", fixed_data, 'blue'),
                                     ("DS2", ds2_data, 'red')]:
            if not data:
                continue
            t, v = extract_slot_usage(data)
            if t and v:
                ax.fill_between(t, v, alpha=0.3, color=color, step='post')
                ax.plot(t, v, color=color, alpha=0.7, label=label,
                        linewidth=1.5, drawstyle='steps-post')
        ax.set_title('Resource Consumption (Slot x Time)')
        ax.set_ylabel('Slots')

    elif scenario.name == "fluctuate":
        # ★ P10: 综合测试专属面板 — 分阶段吞吐对比
        phase_boundary = scenario.load_phases[0][1] / 60.0  # Phase 1 结束时间 (min)

        for label, data, color in [("Fixed", fixed_data, 'blue'),
                                     ("DS2", ds2_data, 'red')]:
            if not data:
                continue
            t, v = extract_series(data, 'Source', 'out_rate')
            if t and v:
                ax.plot(t, v, color=color, alpha=0.7,
                        label=label, linewidth=1.2)

        # 画两条目标线
        rate1, dur1 = scenario.load_phases[0]
        rate2, dur2 = scenario.load_phases[1]
        ax.hlines(rate1, 0, dur1 / 60.0, colors='green',
                  linestyles='--', alpha=0.5, linewidth=1.5)
        ax.hlines(rate2, dur1 / 60.0, (dur1 + dur2) / 60.0,
                  colors='green', linestyles='--', alpha=0.5, linewidth=1.5)

        ax.axvline(x=phase_boundary, color='gray', linestyle=':',
                   alpha=0.6, linewidth=2)
        ax.text(phase_boundary, ax.get_ylim()[1] * 0.95,
                'Load Change', ha='center', va='top',
                fontsize=9, color='gray', fontweight='bold')

        ax.set_title('Throughput vs Target (Fluctuate)')
        ax.set_ylabel('Records/s')

    else:
        if ds2_data:
            for kw, color in [('Filter', 'green'), ('Signal', 'blue'),
                               ('Feature', 'orange'), ('Grid', 'red')]:
                t, v = extract_series(ds2_data, kw, 'out_rate')
                if t:
                    ax.plot(t, v, color=color, alpha=0.7,
                            label=kw, linewidth=1)
        ax.set_title('Operator Output Rate (DS2)')
        ax.set_ylabel('Records/s')

    ax.set_xlabel('Time (min)')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    # Phase 标注
    for ax_row in axes:
        for a in ax_row:
            ct = 0
            for rate, dur in scenario.load_phases:
                mid = (ct + dur / 2) / 60.0
                y_top = a.get_ylim()[1]
                rate_label = f'{rate/1000:.0f}k' if rate >= 1000 else str(rate)
                a.text(mid, y_top * 0.95, rate_label,
                       ha='center', va='top', fontsize=8, color='gray')
                ct += dur
                total_s = sum(d for _, d in scenario.load_phases)
                if ct < total_s:
                    a.axvline(x=ct / 60.0, color='gray',
                              linestyle='--', alpha=0.3)

    plt.tight_layout()
    plot_path = os.path.join(output_dir, f"{scenario.name}_results.png")
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    plt.close()
    logger.info("[OK] Plot: %s", plot_path)

    _generate_summary(output_dir, scenario, fixed_data, ds2_data)


def _generate_summary(output_dir: str, scenario: ScenarioConfig,
                      fixed_data: List[dict], ds2_data: List[dict]):

    fs = compute_stats(fixed_data)
    ds = compute_stats(ds2_data)

    lines = []
    lines.append("=" * 60)
    lines.append(f"  {scenario.display_name} - Summary (P10)")
    lines.append("=" * 60)
    lines.append(f"  Time: {datetime.now().isoformat()}")
    lines.append(f"  Scenario: {scenario.description}")

    for label, s, p_cfg in [
        ("Fixed Parallelism", fs, scenario.fixed_parallelism),
        ("DS2 Auto-Scaling", ds, scenario.ds2_initial_parallelism),
    ]:
        lines.append(f"\n--- {label} ---")
        lines.append(f"  Config: {p_cfg}")
        if not s:
            lines.append("  (no data)")
            continue
        lines.append(f"  Avg throughput : {s['avg_thru']:.1f} r/s")
        lines.append(f"  Max throughput : {s['max_thru']:.1f} r/s")
        lines.append(f"  Avg busy       : {s['avg_busy']:.3f}")
        lines.append(f"  Avg slots      : {s['avg_slots']:.1f}")
        lines.append(f"  Slot range     : [{s['min_slots']}, {s['max_slots']}]")
        lines.append(f"  Resource cost  : {s['slot_minutes']:.1f} slot*min")
        lines.append(f"  Operator parallelism:")
        for op, info in sorted(s['op_summary'].items()):
            changed = " <- dynamic" if info['changes'] else ""
            lines.append(
                f"    {op:30s}: [{info['min']}, {info['max']}]{changed}"
            )

    if fs and ds:
        lines.append("\n--- Comparison ---")
        thru_diff = ((ds['avg_thru'] - fs['avg_thru'])
                     / max(fs['avg_thru'], 1) * 100)
        slot_diff = ((ds['avg_slots'] - fs['avg_slots'])
                     / max(fs['avg_slots'], 1) * 100)
        lines.append(f"  Throughput diff : {thru_diff:+.1f}%")
        lines.append(f"  Slot diff       : {slot_diff:+.1f}%")

        f_eff = fs['avg_thru'] / max(fs['avg_slots'], 0.1)
        d_eff = ds['avg_thru'] / max(ds['avg_slots'], 0.1)
        lines.append(f"  Fixed efficiency: {f_eff:.1f} r/s/slot")
        lines.append(f"  DS2 efficiency  : {d_eff:.1f} r/s/slot")
        eff_gain = (d_eff - f_eff) / max(f_eff, 1) * 100
        lines.append(f"  Efficiency gain : {eff_gain:+.1f}%")

        f_sm = fs.get('slot_minutes', 0)
        d_sm = ds.get('slot_minutes', 0)
        if f_sm > 0:
            saved = f_sm - d_sm
            saved_pct = saved / f_sm * 100
            direction = 'saved' if saved > 0 else 'extra'
            lines.append(f"  Resource compare: Fixed={f_sm:.1f} vs DS2={d_sm:.1f} slot*min")
            lines.append(f"  Resource {direction}: {abs(saved):.1f} slot*min ({abs(saved_pct):.1f}%)")

        # ★ P10: 场景专属统计
        if scenario.name == "scale_up":
            lines.append(f"\n--- Scale-Up Detail ---")
            target = scenario.load_phases[0][0]
            for lbl, data in [("Fixed", fixed_data), ("DS2", ds2_data)]:
                if data:
                    t80 = _find_reach_time(data, target * 0.8)
                    t_str = f"{t80:.0f}s" if t80 else "not reached"
                    lines.append(f"  {lbl} reach 80% target: {t_str}")

        elif scenario.name == "scale_down0":
            lines.append(f"\n--- Scale-Down Detail ---")
            if ds:
                lines.append(f"  DS2 final slot  : {ds['min_slots']}")
                lines.append(f"  DS2 range       : {ds['max_slots']} -> {ds['min_slots']}")
            if fs:
                lines.append(f"  Fixed always    : {fs['avg_slots']:.0f} slots")

        elif scenario.name == "fluctuate":
            lines.append(f"\n--- Fluctuate Detail ---")
            # 分阶段统计
            for phase_idx, (rate, dur) in enumerate(scenario.load_phases):
                phase_label = f"phase_{phase_idx + 1}"
                lines.append(f"\n  Phase {phase_idx + 1}: {rate} r/s x {dur}s")

                for lbl, data in [("Fixed", fixed_data), ("DS2", ds2_data)]:
                    ps = _compute_phase_stats(data, phase_label)
                    if ps:
                        lines.append(f"    {lbl}:")
                        lines.append(f"      Avg throughput : {ps['avg_thru']:.1f} r/s")
                        lines.append(f"      Avg slots      : {ps['avg_slots']:.1f}")
                        lines.append(f"      Slot range     : [{ps['min_slots']}, {ps['max_slots']}]")
                        lines.append(f"      Resource cost  : {ps['slot_minutes']:.1f} slot*min")
                        eff = ps['avg_thru'] / max(ps['avg_slots'], 0.1)
                        lines.append(f"      Efficiency     : {eff:.1f} r/s/slot")
                    else:
                        lines.append(f"    {lbl}: (no data)")

            # DS2 并行度变化
            if ds:
                lines.append(f"\n  DS2 Parallelism Changes:")
                lines.append(f"    Slot range     : [{ds['min_slots']}, {ds['max_slots']}]")
                lines.append(f"    Dynamic operators:")
                for op, info in sorted(ds['op_summary'].items()):
                    if info['changes']:
                        lines.append(f"      {op}: [{info['min']}, {info['max']}]")

            # 资源对比: 低负载阶段 DS2 应节省
            fp2 = _compute_phase_stats(fixed_data, "phase_2")
            dp2 = _compute_phase_stats(ds2_data, "phase_2")
            if fp2 and dp2:
                saved = fp2['slot_minutes'] - dp2['slot_minutes']
                lines.append(f"\n  Low-Load Phase Resource Saving:")
                lines.append(f"    Fixed : {fp2['slot_minutes']:.1f} slot*min")
                lines.append(f"    DS2   : {dp2['slot_minutes']:.1f} slot*min")
                if saved > 0:
                    lines.append(f"    DS2 saved {saved:.1f} slot*min ({saved/max(fp2['slot_minutes'],0.1)*100:.1f}%)")
                else:
                    lines.append(f"    DS2 extra {abs(saved):.1f} slot*min")

    lines.append("\n" + "=" * 60)

    summary = "\n".join(lines)
    path = os.path.join(output_dir, f"{scenario.name}_summary.txt")
    with open(path, 'w', encoding='utf-8') as f:
        f.write(summary)
    print(summary)
    logger.info("[OK] Summary: %s", path)


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="DS2 Three-Scenario Experiment (v3 - P10)",
        epilog="""
Scenarios:
  scale_up    : Low P(2) + 6000 r/s x 15min
  scale_down0 : High P(6) + 2000 r/s x 10min
  fluctuate   : Low P(2) + 6000->2000 r/s (10min each)
  all         : Run all three

Examples:
  python experiment_v3.py --scenario scale_up --only ds2
  python experiment_v3.py --scenario fluctuate
  python experiment_v3.py --scenario all
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--scenario",
        choices=list(SCENARIOS.keys()) + ["all"],
        default="all",
        help="Select experiment scenario",
    )
    parser.add_argument(
        "--only",
        choices=["fixed", "ds2"],
        help="Run only one group",
    )
    parser.add_argument(
        "--plot-only",
        action="store_true",
        help="Only generate plots from existing CSV",
    )

    args = parser.parse_args()

    if args.scenario == "all":
        scenario_names = list(SCENARIOS.keys())
    else:
        scenario_names = [args.scenario]

    os.makedirs(OUTPUT_ROOT, exist_ok=True)

    if args.plot_only:
        for name in scenario_names:
            sc = SCENARIOS[name]
            out_dir = os.path.join(OUTPUT_ROOT, sc.name)
            if os.path.exists(out_dir):
                generate_scenario_plots(out_dir, sc)
            else:
                logger.warning("No data for [%s]: %s", name, out_dir)
        return

    mode = args.only or "both"

    for i, name in enumerate(scenario_names):
        sc = SCENARIOS[name]

        logger.info("")
        logger.info("=" * 60)
        logger.info("  Scenario %d/%d: %s",
                     i + 1, len(scenario_names), sc.display_name)
        logger.info("=" * 60)

        runner = ScenarioRunner(sc)
        runner.run(mode)

        if i < len(scenario_names) - 1:
            logger.info("Interval 30s between scenarios ...")
            time.sleep(30)

    logger.info("")
    logger.info("=" * 60)
    logger.info("  All experiments done!")
    logger.info("  Results: %s/", OUTPUT_ROOT)
    for name in scenario_names:
        logger.info("    - %s/%s/", OUTPUT_ROOT, name)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()





