#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DS2 自动扩缩容控制器（算子级并行度版）—— P10 修复版
=========================================================
基于 OSDI '18 论文 "Three Steps is All You Need" 实现
适配 Apache Flink 1.17 Standalone 集群

修复记录:
  P1~P7: (略)
  P8:  方向锁 + 重启跳轮 — 消除扩→缩震荡
  P9:  累计扩容冷却 — 减少碎步连续扩容
  P10: 高负载保护 — Source 吞吐 >= 容量*70% 时禁止一切缩容
       方向锁 300s, penalty 180s
"""

import os
import sys
import time
import math
import signal
import logging
import argparse
import csv
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict, deque
from datetime import datetime

import requests

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False


# ============================================================================
# 1. 配置管理
# ============================================================================

@dataclass
class OperatorConfig:
    keyword: str
    arg_name: str
    default_parallelism: int = 4


@dataclass
class Config:

    flink_rest_url: str = "http://localhost:8081"
    flink_job_name: str = "Seismic-Stream-Job"
    flink_savepoint_dir: str = "/tmp/flink-savepoints"

    policy_interval: int = 15
    warm_up_time: int = 120
    activation_window: int = 2
    target_rate_ratio: float = 1.3
    min_busy_ratio: float = 0.05
    idle_busy_threshold: float = 0.15
    max_parallelism: int = 8
    min_parallelism: int = 2
    change_threshold: int = 0
    target_jar_name: str = "flink-rocketmq-demo-1.0-SNAPSHOT"

    max_scale_factor: float = 3.0
    recovery_busy_threshold: float = 0.95
    recovery_throughput_min: float = 100.0
    min_significant_busy: float = 0.30
    initial_grace_period: int = 60

    source_capacity_per_instance: float = 2000.0
    source_max_parallelism: int = 8

    use_savepoint: bool = True
    wait_running_timeout: int = 90

    # P8
    direction_lock_seconds: int = 300
    post_restart_skip_rounds: int = 3

    # P9
    consecutive_scale_limit: int = 2
    consecutive_scale_penalty: int = 180

    # ★ P10: 高负载保护
    high_load_protection_ratio: float = 0.70

    operator_mapping: Dict[str, OperatorConfig] = field(default_factory=dict)
    log_level: str = "INFO"

    @classmethod
    def from_yaml(cls, path: str) -> 'Config':
        config = cls()

        if not HAS_YAML:
            print("[WARN] PyYAML not installed, using default config")
            return config

        if not os.path.exists(path):
            print(f"[WARN] Config file not found: {path}, using defaults")
            return config

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)

            if 'flink' in data:
                flink = data['flink']
                config.flink_rest_url = flink.get(
                    'rest_url', config.flink_rest_url)
                config.flink_job_name = flink.get(
                    'job_name', config.flink_job_name)
                config.flink_savepoint_dir = flink.get(
                    'savepoint_dir', config.flink_savepoint_dir)

            if 'ds2' in data:
                ds2 = data['ds2']
                config.policy_interval = ds2.get(
                    'policy_interval', config.policy_interval)
                config.warm_up_time = ds2.get(
                    'warm_up_time', config.warm_up_time)
                config.activation_window = ds2.get(
                    'activation_window', config.activation_window)
                config.target_rate_ratio = ds2.get(
                    'target_rate_ratio', config.target_rate_ratio)
                config.min_busy_ratio = ds2.get(
                    'min_busy_ratio', config.min_busy_ratio)
                config.idle_busy_threshold = ds2.get(
                    'idle_busy_threshold', config.idle_busy_threshold)
                config.max_parallelism = ds2.get(
                    'max_parallelism', config.max_parallelism)
                config.min_parallelism = ds2.get(
                    'min_parallelism', config.min_parallelism)
                config.change_threshold = ds2.get(
                    'change_threshold', config.change_threshold)
                config.target_jar_name = ds2.get(
                    'target_jar_name', config.target_jar_name)

                config.max_scale_factor = ds2.get(
                    'max_scale_factor', config.max_scale_factor)
                config.recovery_busy_threshold = ds2.get(
                    'recovery_busy_threshold',
                    config.recovery_busy_threshold)
                config.recovery_throughput_min = ds2.get(
                    'recovery_throughput_min',
                    config.recovery_throughput_min)
                config.min_significant_busy = ds2.get(
                    'min_significant_busy', config.min_significant_busy)
                config.initial_grace_period = ds2.get(
                    'initial_grace_period', config.initial_grace_period)

                config.source_capacity_per_instance = ds2.get(
                    'source_capacity_per_instance',
                    config.source_capacity_per_instance)
                config.source_max_parallelism = ds2.get(
                    'source_max_parallelism',
                    config.source_max_parallelism)

                config.use_savepoint = ds2.get(
                    'use_savepoint', config.use_savepoint)
                config.wait_running_timeout = ds2.get(
                    'wait_running_timeout', config.wait_running_timeout)

                config.direction_lock_seconds = ds2.get(
                    'direction_lock_seconds',
                    config.direction_lock_seconds)
                config.post_restart_skip_rounds = ds2.get(
                    'post_restart_skip_rounds',
                    config.post_restart_skip_rounds)

                config.consecutive_scale_limit = ds2.get(
                    'consecutive_scale_limit',
                    config.consecutive_scale_limit)
                config.consecutive_scale_penalty = ds2.get(
                    'consecutive_scale_penalty',
                    config.consecutive_scale_penalty)

                # ★ P10
                config.high_load_protection_ratio = ds2.get(
                    'high_load_protection_ratio',
                    config.high_load_protection_ratio)

                if 'cooldown_seconds' in ds2:
                    config.warm_up_time = ds2['cooldown_seconds']

            if 'operator_mapping' in data:
                for key, val in data['operator_mapping'].items():
                    config.operator_mapping[key] = OperatorConfig(
                        keyword=val.get('keyword', key),
                        arg_name=val.get('arg_name', f'p-{key}'),
                        default_parallelism=val.get(
                            'default_parallelism', 4),
                    )

            if 'logging' in data:
                config.log_level = data['logging'].get(
                    'level', config.log_level)

            print(f"[INFO] Config loaded from {path}")
            print(f"[INFO] Operator mappings: "
                  f"{list(config.operator_mapping.keys())}")
            print(f"[INFO] Source capacity: "
                  f"{config.source_capacity_per_instance} r/s/instance")
            print(f"[INFO] Source max parallelism: "
                  f"{config.source_max_parallelism}")
            print(f"[INFO] Use savepoint: {config.use_savepoint}")
            print(f"[INFO] Direction lock: "
                  f"{config.direction_lock_seconds}s")
            print(f"[INFO] Post-restart skip rounds: "
                  f"{config.post_restart_skip_rounds}")
            print(f"[INFO] Consecutive scale limit: "
                  f"{config.consecutive_scale_limit}, "
                  f"penalty: {config.consecutive_scale_penalty}s")
            print(f"[INFO] High-load protection ratio: "
                  f"{config.high_load_protection_ratio}")

        except Exception as e:
            print(f"[WARN] Failed to load config: {e}, using defaults")

        return config

    def validate(self) -> bool:
        assert self.policy_interval >= 5
        assert self.warm_up_time >= 0
        assert self.activation_window >= 1
        assert 1.0 <= self.target_rate_ratio <= 2.0
        assert self.max_parallelism >= self.min_parallelism
        assert self.max_scale_factor >= 1.0
        assert self.source_capacity_per_instance > 0
        assert self.source_max_parallelism >= 1
        assert self.direction_lock_seconds >= 0
        assert self.post_restart_skip_rounds >= 0
        assert self.consecutive_scale_limit >= 1
        assert self.consecutive_scale_penalty >= 0
        assert 0.0 < self.high_load_protection_ratio <= 1.0
        return True

    def find_operator_config(
        self, vertex_name: str
    ) -> Optional[OperatorConfig]:
        for key, op_config in self.operator_mapping.items():
            if op_config.keyword in vertex_name:
                return op_config
        return None


# ============================================================================
# 2. 数据结构
# ============================================================================

@dataclass
class OperatorMetrics:
    vertex_id: str
    vertex_name: str
    parallelism: int

    records_in_per_second: float = 0.0
    records_out_per_second: float = 0.0
    busy_ratio: float = 0.0

    true_processing_rate: float = 0.0
    true_output_rate: float = 0.0
    selectivity: float = 1.0

    arg_name: str = ""
    has_valid_metrics: bool = False


@dataclass
class ScalingDecision:
    vertex_id: str
    vertex_name: str
    arg_name: str
    current_parallelism: int
    target_parallelism: int
    reason: str

    @property
    def needs_change(self) -> bool:
        return self.current_parallelism != self.target_parallelism

    @property
    def action(self) -> str:
        if self.target_parallelism > self.current_parallelism:
            return "SCALE_UP"
        elif self.target_parallelism < self.current_parallelism:
            return "SCALE_DOWN"
        return "NO_CHANGE"

    @property
    def direction(self) -> str:
        if self.target_parallelism > self.current_parallelism:
            return "UP"
        elif self.target_parallelism < self.current_parallelism:
            return "DOWN"
        return "NONE"


@dataclass
class JobTopology:
    job_id: str
    job_name: str
    vertices: Dict[str, OperatorMetrics] = field(default_factory=dict)
    edges: List[Tuple[str, str]] = field(default_factory=list)

    def get_upstream(self, vertex_id: str) -> List[str]:
        return [e[0] for e in self.edges if e[1] == vertex_id]

    def get_downstream(self, vertex_id: str) -> List[str]:
        return [e[1] for e in self.edges if e[0] == vertex_id]

    def topological_sort(self) -> List[str]:
        in_degree: Dict[str, int] = defaultdict(int)
        for _, dst in self.edges:
            in_degree[dst] += 1

        queue = [v for v in self.vertices if in_degree[v] == 0]
        result: List[str] = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            for downstream in self.get_downstream(node):
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    queue.append(downstream)

        if len(result) != len(self.vertices):
            return list(self.vertices.keys())

        return result


# ============================================================================
# 3. Flink REST API 客户端
# ============================================================================

class FlinkClient:

    REQUIRED_METRICS = [
        "numRecordsIn",
        "numRecordsOut",
        "accumulateBusyTimeMs",
    ]

    def __init__(self, config: Config):
        self.config = config
        self.base_url = config.flink_rest_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        })
        self.logger = logging.getLogger("DS2.FlinkClient")

    def _get(self, endpoint: str, timeout: int = 10) -> Optional[Dict]:
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self.session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            self.logger.debug("GET %s -> %d", endpoint, resp.status_code)
            return None
        except requests.exceptions.Timeout:
            self.logger.warning("GET %s timeout", endpoint)
            return None
        except requests.exceptions.ConnectionError:
            self.logger.warning("GET %s connection failed", endpoint)
            return None
        except Exception as e:
            self.logger.debug("GET %s error: %s", endpoint, e)
            return None

    def _post(self, endpoint: str, data: Optional[Dict] = None,
              timeout: int = 30) -> Tuple[bool, Optional[Dict]]:
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self.session.post(url, json=data or {}, timeout=timeout)
            ok = resp.status_code < 400
            body = resp.json() if resp.text.strip() else {}
            return ok, body
        except Exception as e:
            self.logger.error("POST %s error: %s", endpoint, e)
            return False, None

    def _patch(self, endpoint: str, timeout: int = 30) -> bool:
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self.session.patch(url, timeout=timeout)
            return resp.status_code < 400
        except Exception as e:
            self.logger.error("PATCH %s error: %s", endpoint, e)
            return False

    def check_connection(self) -> bool:
        data = self._get("/overview")
        if data:
            self.logger.info(
                "Connected to Flink %s, slots=%s, taskmanagers=%s",
                data.get('flink-version', 'unknown'),
                data.get('slots-total', '?'),
                data.get('taskmanagers', '?'),
            )
            return True
        return False

    def get_running_job_id(self) -> Optional[str]:
        data = self._get("/jobs/overview")
        if not data:
            return None
        for job in data.get('jobs', []):
            if (job.get('name') == self.config.flink_job_name
                    and job.get('state') == 'RUNNING'):
                return job['jid']
        self.logger.debug(
            "No running job named '%s'", self.config.flink_job_name)
        return None

    def get_job_topology(self, job_id: str) -> Optional[JobTopology]:
        data = self._get(f"/jobs/{job_id}")
        if not data:
            return None

        topology = JobTopology(
            job_id=job_id, job_name=data.get('name', ''))

        for vertex in data.get('vertices', []):
            vid = vertex['id']
            vname = vertex['name']
            op_cfg = self.config.find_operator_config(vname)
            arg_name = op_cfg.arg_name if op_cfg else ""

            topology.vertices[vid] = OperatorMetrics(
                vertex_id=vid,
                vertex_name=vname,
                parallelism=vertex['parallelism'],
                arg_name=arg_name,
            )

        plan = data.get('plan', {})
        for node in plan.get('nodes', []):
            node_id = node['id']
            for inp in node.get('inputs', []):
                topology.edges.append((inp['id'], node_id))

        return topology

    def get_vertex_metrics(self, job_id: str,
                           vertex_id: str) -> Dict[str, List[float]]:
        params = "?get=" + ",".join(self.REQUIRED_METRICS)
        endpoint = (f"/jobs/{job_id}/vertices/{vertex_id}"
                     f"/subtasks/metrics")
        data = self._get(endpoint + params)
        if not data:
            return {}

        result: Dict[str, List[float]] = {
            m: [] for m in self.REQUIRED_METRICS
        }

        def safe_float(val) -> float:
            if val is None:
                return 0.0
            s = str(val).strip()
            if s.lower() in ('nan', 'null', '', 'inf', '-inf'):
                return 0.0
            try:
                v = float(s)
                if math.isnan(v) or math.isinf(v):
                    return 0.0
                return v
            except (ValueError, TypeError):
                return 0.0

        if (isinstance(data, list) and data
                and 'metrics' in data[0]):
            for subtask in data:
                row: Dict[str, float] = {}
                for m in subtask.get('metrics', []):
                    row[m['id']] = safe_float(m.get('value', 0))
                for metric in self.REQUIRED_METRICS:
                    result[metric].append(row.get(metric, 0.0))
        else:
            agg: Dict[str, float] = {}
            for m in data:
                mid = m.get('id', '')
                if mid in self.REQUIRED_METRICS:
                    agg[mid] = safe_float(
                        m.get('sum', m.get('value', 0)))
            for metric in self.REQUIRED_METRICS:
                result[metric].append(agg.get(metric, 0.0))

        return result

    def trigger_savepoint(self, job_id: str) -> Optional[str]:
        self.logger.info("Triggering savepoint ...")
        self.logger.info(
            "  target dir: %s", self.config.flink_savepoint_dir)

        ok, resp = self._post(
            f"/jobs/{job_id}/savepoints",
            {
                "target-directory": self.config.flink_savepoint_dir,
                "cancel-job": False,
            },
        )
        if not ok or not resp:
            self.logger.error("Savepoint trigger failed: %s", resp)
            return None

        trigger_id = resp.get("request-id")
        if not trigger_id:
            self.logger.error("No request-id in response: %s", resp)
            return None

        for _ in range(60):
            time.sleep(1)
            status = self._get(
                f"/jobs/{job_id}/savepoints/{trigger_id}")
            if not status:
                continue
            state = status.get("status", {}).get("id")
            if state == "COMPLETED":
                location = (status.get("operation", {})
                            .get("location"))
                if not location:
                    cause = (status.get("operation", {})
                             .get("failure-cause", {}))
                    self.logger.error(
                        "Savepoint completed but no location: %s",
                        cause)
                    return None
                self.logger.info("Savepoint done: %s", location)
                return location
            elif state == "FAILED":
                cause = (status.get("operation", {})
                         .get("failure-cause", {}))
                self.logger.error("Savepoint failed: %s", cause)
                return None

        self.logger.error("Savepoint timeout (60 s)")
        return None

    def cancel_job(self, job_id: str) -> bool:
        self.logger.info("Cancelling job %s ...", job_id)
        if not self._patch(f"/jobs/{job_id}?mode=cancel"):
            self.logger.error("Cancel request failed")
            return False

        for _ in range(30):
            time.sleep(1)
            data = self._get(f"/jobs/{job_id}")
            if data and data.get("state") in (
                "CANCELED", "CANCELLED", "FINISHED"
            ):
                self.logger.info("Job cancelled")
                return True

        self.logger.error("Cancel timeout (30 s)")
        return False

    def find_jar_id(self) -> Optional[str]:
        data = self._get("/jars")
        if not data:
            return None
        for jar in data.get("files", []):
            if self.config.target_jar_name in jar.get("name", ""):
                return jar.get("id")
        self.logger.warning(
            "JAR not found: %s", self.config.target_jar_name)
        return None

    def submit_job(self, jar_id: str, parallelism: int,
                   savepoint_path: Optional[str] = None,
                   program_args: str = "") -> Optional[str]:
        body = {
            "parallelism": parallelism,
            "programArgs": program_args,
            "entryClass": "",
            "savepointPath": savepoint_path or "",
            "allowNonRestoredState": True,
        }
        self.logger.info(
            "Submitting job: global_parallelism=%d", parallelism)
        self.logger.info("  programArgs : %s", program_args)
        if savepoint_path:
            self.logger.info("  savepoint   : %s", savepoint_path)

        ok, resp = self._post(
            f"/jars/{jar_id}/run", body, timeout=60)
        if ok and resp:
            new_jid = resp.get("jobid")
            self.logger.info("Job submitted: %s", new_jid)
            return new_jid
        self.logger.error("Job submission failed: %s", resp)
        return None

    def wait_job_running(self, timeout: int = 90) -> Optional[str]:
        self.logger.info(
            "Waiting for job '%s' to become RUNNING (timeout=%ds) ...",
            self.config.flink_job_name, timeout)
        for i in range(timeout // 3):
            time.sleep(3)
            job_id = self.get_running_job_id()
            if job_id:
                self.logger.info(
                    "Job RUNNING: %s (waited %ds)", job_id, (i + 1) * 3)
                return job_id
        self.logger.error(
            "Timeout waiting for job RUNNING (%ds)", timeout)
        return None


# ============================================================================
# 4. 指标采集器（累积差分法）
# ============================================================================

class MetricsCollector:

    def __init__(self, client: FlinkClient, config: Config):
        self.client = client
        self.config = config
        self.logger = logging.getLogger("DS2.Metrics")
        self._prev_snapshot: Dict[str, Dict[str, float]] = {}
        self._prev_time: float = 0.0
        self._skip_rounds: int = 0

    def reset(self):
        self._prev_snapshot.clear()
        self._prev_time = 0.0
        self._skip_rounds = self.config.post_restart_skip_rounds
        self.logger.info(
            "Metrics collector reset (skip next %d rounds)",
            self._skip_rounds)

    def has_enough_samples(self) -> bool:
        return bool(self._prev_snapshot) and self._prev_time > 0

    def collect(self, topology: JobTopology) -> bool:
        if self._skip_rounds > 0:
            self._skip_rounds -= 1
            self.logger.info(
                "  [SKIP] Post-restart skip round "
                "(remaining %d rounds)", self._skip_rounds)
            self._collect_snapshot_only(topology)
            return False

        now = time.time()
        current_snapshot: Dict[str, Dict[str, float]] = {}
        has_prev = (bool(self._prev_snapshot)
                    and self._prev_time > 0)

        for vid, metrics in topology.vertices.items():
            raw = self.client.get_vertex_metrics(
                topology.job_id, vid)
            if not raw:
                self.logger.debug(
                    "No raw metrics for %s", metrics.vertex_name)
                metrics.has_valid_metrics = False
                continue

            total_in = sum(raw.get("numRecordsIn", [0.0]))
            total_out = sum(raw.get("numRecordsOut", [0.0]))
            total_busy = sum(
                raw.get("accumulateBusyTimeMs", [0.0]))

            current_snapshot[vid] = {
                "numRecordsIn": total_in,
                "numRecordsOut": total_out,
                "accumulateBusyTimeMs": total_busy,
            }

            if has_prev and vid in self._prev_snapshot:
                dt = now - self._prev_time
                if dt < 1.0:
                    metrics.has_valid_metrics = False
                    continue

                prev = self._prev_snapshot[vid]
                delta_in = max(
                    total_in - prev["numRecordsIn"], 0.0)
                delta_out = max(
                    total_out - prev["numRecordsOut"], 0.0)
                delta_busy = max(
                    total_busy - prev["accumulateBusyTimeMs"], 0.0)

                metrics.records_in_per_second = delta_in / dt
                metrics.records_out_per_second = delta_out / dt

                p = max(metrics.parallelism, 1)
                metrics.busy_ratio = min(
                    delta_busy / (dt * 1000.0 * p), 1.0)

                if metrics.records_in_per_second > 1.0:
                    metrics.selectivity = (
                        metrics.records_out_per_second
                        / metrics.records_in_per_second
                    )
                else:
                    metrics.selectivity = 1.0

                eff_busy = max(
                    metrics.busy_ratio, self.config.min_busy_ratio)
                metrics.true_processing_rate = (
                    metrics.records_in_per_second / eff_busy)
                metrics.true_output_rate = (
                    metrics.records_out_per_second / eff_busy)

                metrics.has_valid_metrics = True

                self.logger.debug(
                    "  %-25s in=%8.1f/s out=%8.1f/s "
                    "busy=%.3f true=%8.1f/s sel=%.3f",
                    metrics.vertex_name,
                    metrics.records_in_per_second,
                    metrics.records_out_per_second,
                    metrics.busy_ratio,
                    metrics.true_processing_rate,
                    metrics.selectivity,
                )
            else:
                metrics.has_valid_metrics = False

        self._prev_snapshot = current_snapshot
        self._prev_time = now

        valid = sum(
            1 for v in topology.vertices.values()
            if v.has_valid_metrics
        )
        self.logger.info(
            "Metrics collected: %d/%d vertices valid",
            valid, len(topology.vertices))
        return valid > 0

    def _collect_snapshot_only(self, topology: JobTopology):
        now = time.time()
        current_snapshot: Dict[str, Dict[str, float]] = {}

        for vid, metrics in topology.vertices.items():
            raw = self.client.get_vertex_metrics(
                topology.job_id, vid)
            if not raw:
                continue

            total_in = sum(raw.get("numRecordsIn", [0.0]))
            total_out = sum(raw.get("numRecordsOut", [0.0]))
            total_busy = sum(
                raw.get("accumulateBusyTimeMs", [0.0]))

            current_snapshot[vid] = {
                "numRecordsIn": total_in,
                "numRecordsOut": total_out,
                "accumulateBusyTimeMs": total_busy,
            }
            metrics.has_valid_metrics = False

        self._prev_snapshot = current_snapshot
        self._prev_time = now


# ============================================================================
# 5. DS2 决策模型
# ============================================================================

class DS2Model:

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("DS2.Model")
        self.idle_threshold = config.idle_busy_threshold

    def _apply_amplitude_limit(self, current_p: int,
                                raw_target_p: int) -> int:
        mf = self.config.max_scale_factor
        if raw_target_p > current_p:
            capped = min(raw_target_p,
                         math.ceil(current_p * mf))
            capped = min(capped, self.config.max_parallelism)
            capped = max(capped, self.config.min_parallelism)
            if capped != raw_target_p:
                self.logger.info(
                    "    amp_limit(UP): %d -> %d "
                    "(cur=%d, max %.1fx)",
                    raw_target_p, capped, current_p, mf)
            return capped
        elif raw_target_p < current_p:
            floored = max(raw_target_p,
                          math.ceil(current_p / mf))
            floored = max(floored, self.config.min_parallelism)
            if floored != raw_target_p:
                self.logger.info(
                    "    amp_limit(DOWN): %d -> %d "
                    "(cur=%d, min 1/%.1fx)",
                    raw_target_p, floored, current_p, mf)
            return floored
        return raw_target_p

    def _compensate_busy(self, metrics: OperatorMetrics) -> float:
        busy = metrics.busy_ratio
        p = max(metrics.parallelism, 1)

        if busy >= self.config.min_busy_ratio:
            return busy

        throughput = max(metrics.records_in_per_second,
                         metrics.records_out_per_second)
        if throughput < 1.0:
            return busy

        cap = self.config.source_capacity_per_instance
        per_inst_throughput = throughput / p
        estimated = per_inst_throughput / cap
        estimated = max(estimated, self.config.min_busy_ratio)
        estimated = min(estimated, 0.8)

        self.logger.info(
            "  [busy-compensate] %s: reported=%.3f -> "
            "estimated=%.3f (throughput=%.1f/s, P=%d, cap=%.0f/inst)",
            metrics.vertex_name, busy, estimated,
            throughput, p, cap)
        return estimated

    def _compute_source_parallelism(
        self, metrics: OperatorMetrics
    ) -> int:
        p = metrics.parallelism
        cap = self.config.source_capacity_per_instance

        busy = self._compensate_busy(metrics)
        target_util = 1.0 / self.config.target_rate_ratio

        if busy > target_util:
            raw_p = math.ceil(p * busy / target_util)
            clamped = max(self.config.min_parallelism,
                          min(raw_p, self.config.max_parallelism))
            clamped = min(clamped, self.config.source_max_parallelism)
            return self._apply_amplitude_limit(p, clamped)

        if (busy < target_util * 0.5
                and p > self.config.min_parallelism):
            raw_p = math.ceil(p * busy / target_util)
            raw_p = max(raw_p, self.config.min_parallelism)
            clamped = max(raw_p, p - 1)
            return self._apply_amplitude_limit(p, clamped)

        return p

    def _cascade_protection(
        self,
        topology: JobTopology,
        decisions: Dict[str, ScalingDecision],
    ):
        for vid in topology.topological_sort():
            if vid not in decisions:
                continue
            d = decisions[vid]
            upstream_ids = topology.get_upstream(vid)
            if not upstream_ids:
                continue

            any_up = False
            ratios: List[float] = []
            for uid in upstream_ids:
                if uid in decisions:
                    ud = decisions[uid]
                    if ud.target_parallelism > ud.current_parallelism:
                        any_up = True
                        if ud.current_parallelism > 0:
                            ratios.append(
                                ud.target_parallelism
                                / ud.current_parallelism
                            )

            if not any_up:
                continue

            if d.target_parallelism < d.current_parallelism:
                self.logger.warning(
                    "  [CASCADE-BLOCK] %s: upstream UP, "
                    "block %d -> %d (keep %d)",
                    d.vertex_name, d.target_parallelism,
                    d.current_parallelism, d.current_parallelism)
                d.target_parallelism = d.current_parallelism
                d.reason += " | CASCADE_BLOCKED"

            elif (d.target_parallelism == d.current_parallelism
                  and ratios):
                avg_r = sum(ratios) / len(ratios)
                if avg_r > 1.2:
                    follow_r = 1.0 + (avg_r - 1.0) * 0.5
                    follow_p = min(
                        math.ceil(d.current_parallelism * follow_r),
                        self.config.max_parallelism)
                    follow_p = self._apply_amplitude_limit(
                        d.current_parallelism, follow_p)
                    if follow_p > d.current_parallelism:
                        self.logger.info(
                            "  [CASCADE-FOLLOW] %s: upstream UP "
                            "(avg_ratio=%.2f), follow %d -> %d",
                            d.vertex_name, avg_r,
                            d.current_parallelism, follow_p)
                        d.target_parallelism = follow_p
                        d.reason += (
                            " | CASCADE_FOLLOW(%.2f)" % avg_r)

    def compute(
        self, topology: JobTopology
    ) -> Dict[str, ScalingDecision]:
        decisions: Dict[str, ScalingDecision] = {}
        target_output_rate: Dict[str, float] = {}

        self.logger.info("=" * 60)
        self.logger.info("DS2 Model Computing (P10) ...")
        self.logger.info("=" * 60)

        for vid in topology.topological_sort():
            metrics = topology.vertices[vid]
            upstream_ids = topology.get_upstream(vid)
            current_p = metrics.parallelism
            arg_name = metrics.arg_name

            self.logger.info("")
            self.logger.info(
                "--- %s (P=%d) ---",
                metrics.vertex_name, current_p)

            if not metrics.has_valid_metrics:
                decisions[vid] = ScalingDecision(
                    vid, metrics.vertex_name, arg_name,
                    current_p, current_p, "No valid metrics")
                target_output_rate[vid] = (
                    metrics.records_out_per_second)
                self.logger.info(
                    "  -> keep P=%d (no metrics)", current_p)
                continue

            if not upstream_ids:
                target_p = self._compute_source_parallelism(metrics)
                decisions[vid] = ScalingDecision(
                    vid, metrics.vertex_name, arg_name,
                    current_p, target_p,
                    "Source busy=%.3f cap=%.0f" % (
                        metrics.busy_ratio,
                        self.config.source_capacity_per_instance))

                if current_p > 0 and target_p != current_p:
                    projected = (
                        metrics.records_out_per_second
                        * target_p / current_p)
                    cap_proj = (
                        metrics.records_out_per_second
                        * self.config.max_scale_factor)
                    target_output_rate[vid] = min(
                        projected, cap_proj)
                else:
                    target_output_rate[vid] = (
                        metrics.records_out_per_second)

                self.logger.info(
                    "  out=%.1f/s  busy=%.3f  target_out=%.1f/s"
                    "  -> P: %d -> %d",
                    metrics.records_out_per_second,
                    metrics.busy_ratio,
                    target_output_rate[vid],
                    current_p, target_p)
                continue

            target_input = sum(
                target_output_rate.get(uid, 0.0)
                for uid in upstream_ids)

            actual_input = metrics.records_in_per_second
            if actual_input > self.config.recovery_throughput_min:
                max_growth = (self.config.max_scale_factor
                              * self.config.target_rate_ratio)
                ceiling = actual_input * max_growth
                if target_input > ceiling:
                    self.logger.info(
                        "  anti-cascade: target_input %.0f -> "
                        "%.0f (actual=%.0f x %.1f)",
                        target_input, ceiling,
                        actual_input, max_growth)
                    target_input = ceiling

            self.logger.info(
                "  target_input=%.1f/s", target_input)

            comp_busy = self._compensate_busy(metrics)

            if (target_input <= 0.0
                    and comp_busy < self.idle_threshold):
                target_p = self.config.min_parallelism
                decisions[vid] = ScalingDecision(
                    vid, metrics.vertex_name, arg_name,
                    current_p, target_p,
                    "No input data, set min_p=%d" % target_p)
                target_output_rate[vid] = 0.0
                self.logger.info(
                    "  no input + idle -> P: %d -> %d",
                    current_p, target_p)
                continue

            eff_busy = max(comp_busy, self.config.min_busy_ratio)
            true_rate = (actual_input / eff_busy
                         if actual_input > 0 else 0.0)
            per_inst = true_rate / max(current_p, 1)

            if per_inst < 1.0:
                self.logger.warning(
                    "  per_inst=%.2f too low, keep P=%d",
                    per_inst, current_p)
                decisions[vid] = ScalingDecision(
                    vid, metrics.vertex_name, arg_name,
                    current_p, current_p,
                    "per_inst too low (%.2f)" % per_inst)
                target_output_rate[vid] = (
                    target_input * metrics.selectivity)
                continue

            margin_input = (
                target_input * self.config.target_rate_ratio)
            raw_p = math.ceil(margin_input / per_inst)
            target_p = max(self.config.min_parallelism,
                           min(raw_p, self.config.max_parallelism))
            target_p = self._apply_amplitude_limit(
                current_p, target_p)

            sel_out = target_input * metrics.selectivity
            target_output_rate[vid] = sel_out

            reason = (
                "in=%.1f per_inst=%.1f raw_p=%d "
                "busy=%.3f(comp=%.3f)" % (
                    margin_input, per_inst, raw_p,
                    metrics.busy_ratio, comp_busy))
            decisions[vid] = ScalingDecision(
                vid, metrics.vertex_name, arg_name,
                current_p, target_p, reason)

            self.logger.info(
                "  per_inst=%.1f/s  margin_in=%.1f/s  raw_p=%d"
                "  -> P: %d -> %d  target_out=%.1f/s",
                per_inst, margin_input, raw_p,
                current_p, target_p, sel_out)

        self._cascade_protection(topology, decisions)
        self._log_summary(decisions)
        return decisions

    def _log_summary(
        self, decisions: Dict[str, ScalingDecision]
    ):
        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("Decision Summary (P10):")
        self.logger.info("-" * 60)
        for vid, d in decisions.items():
            tag = ""
            if d.action == "SCALE_UP":
                tag = " [UP]"
            elif d.action == "SCALE_DOWN":
                tag = " [DOWN]"
            self.logger.info(
                "  %-45s %d -> %d  %s%s  (%s)",
                d.vertex_name, d.current_parallelism,
                d.target_parallelism, d.action, tag, d.reason)
        self.logger.info("=" * 60)


# ============================================================================
# 6. 决策过滤器
# ============================================================================

class DecisionFilter:

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("DS2.Filter")
        self._history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=config.activation_window))
        self._busy_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=config.activation_window))
        self.high_busy_threshold = 0.9
        self.fast_track_window = 2

    def reset(self):
        self._history.clear()
        self._busy_history.clear()
        self.logger.info("Decision filter reset")

    def filter(
        self,
        decisions: Dict[str, ScalingDecision],
        topology: JobTopology = None,
    ) -> Dict[str, ScalingDecision]:
        approved: Dict[str, ScalingDecision] = {}

        for vid, decision in decisions.items():
            self._history[vid].append(decision.target_parallelism)

            if topology and vid in topology.vertices:
                busy = topology.vertices[vid].busy_ratio
                self._busy_history[vid].append(busy)

            diff = abs(
                decision.target_parallelism
                - decision.current_parallelism)
            if diff <= self.config.change_threshold:
                continue

            history = list(self._history[vid])
            current_p = decision.current_parallelism

            if self._is_high_busy(vid):
                if len(history) >= self.fast_track_window:
                    recent = history[-self.fast_track_window:]
                    if self._direction_consistent(
                            recent, current_p):
                        final_target = self._compute_median(recent)
                        if final_target != current_p:
                            decision.target_parallelism = final_target
                            approved[vid] = decision
                            self.logger.info(
                                "    [FAST-APPROVED] %s: "
                                "%d -> %d (high-busy, window=%s)",
                                decision.vertex_name,
                                current_p, final_target, recent)
                            continue
                continue

            if len(history) < self.config.activation_window:
                self.logger.debug(
                    "  %s: activation window %d/%d",
                    decision.vertex_name,
                    len(history),
                    self.config.activation_window)
                continue

            recent = history[-self.config.activation_window:]

            if not self._direction_consistent(recent, current_p):
                self.logger.debug(
                    "  %s: direction inconsistent "
                    "window=%s vs current=%d",
                    decision.vertex_name, recent, current_p)
                continue

            final_target = self._compute_median(recent)

            if final_target != current_p:
                decision.target_parallelism = final_target
                approved[vid] = decision
                self.logger.info(
                    "    [APPROVED] %s: %d -> %d (window=%s)",
                    decision.vertex_name,
                    current_p, final_target, recent)

        if approved:
            has_up = any(
                d.action == "SCALE_UP"
                for d in approved.values())
            has_down = any(
                d.action == "SCALE_DOWN"
                for d in approved.values())

            if has_up and has_down:
                down_names = [
                    d.vertex_name for d in approved.values()
                    if d.action == "SCALE_DOWN"
                ]
                self.logger.warning(
                    "    [CONFLICT] UP+DOWN in same batch! "
                    "Dropping SCALE_DOWN for: %s",
                    ", ".join(down_names))

                drop_vids = [
                    vid for vid, d in approved.items()
                    if d.action == "SCALE_DOWN"
                ]
                for vid_drop in drop_vids:
                    if vid_drop in self._history:
                        self._history[vid_drop].clear()

                approved = {
                    vid: d for vid, d in approved.items()
                    if d.action != "SCALE_DOWN"
                }

        if not approved:
            self.logger.info("    No decisions approved this round")

        return approved

    def _direction_consistent(
        self, window: List[int], current_p: int
    ) -> bool:
        directions = set()
        for p in window:
            if p > current_p:
                directions.add("UP")
            elif p < current_p:
                directions.add("DOWN")
        if "UP" in directions and "DOWN" in directions:
            return False
        return True

    def _is_high_busy(self, vid: str) -> bool:
        history = self._busy_history.get(vid)
        if not history or len(history) < self.fast_track_window:
            return False
        recent = list(history)[-self.fast_track_window:]
        avg_busy = sum(recent) / len(recent)
        return avg_busy >= self.high_busy_threshold

    @staticmethod
    def _compute_median(values: List[int]) -> int:
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        if n % 2 == 1:
            return sorted_vals[n // 2]
        return math.ceil(
            (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2)


# ============================================================================
# 7. 扩缩容执行器
# ============================================================================

class ScalingExecutor:

    def __init__(self, client: FlinkClient, config: Config,
                 consumer_group_suffix: str = ""):
        self.client = client
        self.config = config
        self.consumer_group_suffix = consumer_group_suffix
        self.logger = logging.getLogger("DS2.Executor")
        self._scaling_count = 0

    def execute(
        self,
        job_id: str,
        approved: Dict[str, ScalingDecision],
        all_decisions: Dict[str, ScalingDecision],
    ) -> bool:
        if not approved:
            return False

        actual_changes = {
            vid: d for vid, d in approved.items()
            if d.target_parallelism != d.current_parallelism
        }
        if not actual_changes:
            self.logger.info(
                "[PAUSE] All approved targets == current, "
                "skip meaningless restart")
            return False

        approved = actual_changes
        self._scaling_count += 1

        self.logger.info("=" * 60)
        self.logger.info(
            "EXECUTING SCALING #%d", self._scaling_count)
        self.logger.info("=" * 60)
        for vid, d in approved.items():
            self.logger.info(
                "  %s: %d -> %d",
                d.vertex_name,
                d.current_parallelism,
                d.target_parallelism)

        jar_id = self.client.find_jar_id()
        if not jar_id:
            self.logger.error("JAR not found, abort")
            return False
        self.logger.info("JAR id: %s", jar_id)

        program_args = self._build_program_args(
            all_decisions, approved)

        all_targets = []
        for vid, d in all_decisions.items():
            if vid in approved:
                all_targets.append(
                    approved[vid].target_parallelism)
            else:
                all_targets.append(d.current_parallelism)
        global_max_p = max(all_targets) if all_targets else 4

        savepoint_path = None
        if self.config.use_savepoint:
            savepoint_path = self.client.trigger_savepoint(job_id)
            if not savepoint_path:
                self.logger.warning(
                    "Savepoint failed, using stateless restart")

        if not self.client.cancel_job(job_id):
            self.logger.error("Cancel failed, abort")
            return False

        self.logger.info("Waiting 5s for resource release ...")
        time.sleep(5)

        new_job_id = self.client.submit_job(
            jar_id=jar_id,
            parallelism=global_max_p,
            savepoint_path=savepoint_path,
            program_args=program_args,
        )

        if not new_job_id:
            self.logger.error(
                "[FAIL] Job resubmission failed")
            return False

        running_id = self.client.wait_job_running(
            timeout=self.config.wait_running_timeout)
        if not running_id:
            self.logger.error(
                "[FAIL] New job did not reach RUNNING, "
                "may need manual intervention")
            return False

        topology = self.client.get_job_topology(running_id)
        if topology:
            self.logger.info(
                "[OK] Scaling #%d done! New JobID=%s",
                self._scaling_count, running_id)
            for vid_new, m in topology.vertices.items():
                self.logger.info(
                    "    [%s] P=%d  %s",
                    vid_new[:8], m.parallelism, m.vertex_name)
        else:
            self.logger.info(
                "[OK] Scaling #%d done! New JobID=%s",
                self._scaling_count, running_id)

        return True

    def get_last_direction(
        self, approved: Dict[str, ScalingDecision]
    ) -> str:
        dirs = set()
        for d in approved.values():
            if d.target_parallelism > d.current_parallelism:
                dirs.add("UP")
            elif d.target_parallelism < d.current_parallelism:
                dirs.add("DOWN")
        if "UP" in dirs and "DOWN" in dirs:
            return "MIXED"
        if "UP" in dirs:
            return "UP"
        if "DOWN" in dirs:
            return "DOWN"
        return "NONE"

    def _build_program_args(
        self,
        all_decisions: Dict[str, ScalingDecision],
        approved: Dict[str, ScalingDecision],
    ) -> str:
        arg_values: Dict[str, int] = {}

        for vid, d in all_decisions.items():
            if not d.arg_name:
                continue
            p = (approved[vid].target_parallelism
                 if vid in approved
                 else d.current_parallelism)
            if d.arg_name in arg_values:
                arg_values[d.arg_name] = max(
                    arg_values[d.arg_name], p)
            else:
                arg_values[d.arg_name] = p

        for key, op_cfg in self.config.operator_mapping.items():
            if op_cfg.arg_name not in arg_values:
                arg_values[op_cfg.arg_name] = (
                    op_cfg.default_parallelism)

        parts = []

        if self.consumer_group_suffix:
            parts.append(
                "--consumer-group-suffix %s"
                % self.consumer_group_suffix)
            parts.append("--consume-from-latest true")

        for name, value in sorted(arg_values.items()):
            parts.append("--%s %d" % (name, value))

        result = " ".join(parts)
        self.logger.info("programArgs: %s", result)
        return result


# ============================================================================
# 8. 决策日志记录器（CSV）
# ============================================================================

class DecisionLogger:

    def __init__(self, log_dir: str = "ds2_logs"):
        self.logger = logging.getLogger("DS2.CsvLogger")
        os.makedirs(log_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.csv_path = os.path.join(
            log_dir, "ds2_decisions_%s.csv" % ts)
        self._header_written = False
        self.logger.info("CSV log: %s", self.csv_path)

    def log_round(
        self,
        round_num: int,
        topology: JobTopology,
        decisions: Dict[str, ScalingDecision],
        approved: Dict[str, ScalingDecision],
        executed: bool,
    ):
        rows = []
        now = datetime.now().isoformat()

        for vid in topology.topological_sort():
            m = topology.vertices[vid]
            d = decisions.get(vid)

            rows.append({
                "timestamp": now,
                "round": round_num,
                "vertex_name": m.vertex_name,
                "current_parallelism": m.parallelism,
                "target_parallelism": (
                    d.target_parallelism if d else m.parallelism),
                "action": d.action if d else "UNKNOWN",
                "approved": vid in approved,
                "executed": executed,
                "in_rate": round(m.records_in_per_second, 2),
                "out_rate": round(m.records_out_per_second, 2),
                "busy_ratio": round(m.busy_ratio, 4),
                "true_processing_rate": round(
                    m.true_processing_rate, 2),
                "selectivity": round(m.selectivity, 4),
                "reason": d.reason if d else "",
            })

        if not rows:
            return

        try:
            need_header = not self._header_written
            with open(self.csv_path, 'a', newline='',
                      encoding='utf-8') as f:
                writer = csv.DictWriter(
                    f, fieldnames=rows[0].keys())
                if need_header:
                    writer.writeheader()
                    self._header_written = True
                writer.writerows(rows)
        except Exception as e:
            self.logger.warning("CSV write failed: %s", e)


# ============================================================================
# 9. 主控制器 ★ P10: 高负载保护
# ============================================================================

class DS2Controller:

    def __init__(self, config: Config,
                 consumer_group_suffix: str = ""):
        self.config = config
        self.consumer_group_suffix = consumer_group_suffix
        self.logger = logging.getLogger("DS2.Controller")

        self.client = FlinkClient(config)
        self.collector = MetricsCollector(self.client, config)
        self.model = DS2Model(config)
        self.decision_filter = DecisionFilter(config)
        self.executor = ScalingExecutor(
            self.client, config, consumer_group_suffix)
        self.csv_logger = DecisionLogger()

        self._running = True
        self._round = 0
        self._last_scaling_time: float = time.time()

        # P8: 方向锁
        self._last_scale_direction: Optional[str] = None
        self._last_scale_direction_time: float = 0.0

        # P9: 累计连续扩容计数
        self._consecutive_up_count: int = 0

        self.logger.info(
            "Initial warm-up %ds, first decision after %ds",
            config.warm_up_time, config.warm_up_time)

    def _effective_warm_up(self) -> int:
        """P9: 计算有效的 warm-up 时间 (含累计惩罚)"""
        base = self.config.warm_up_time
        limit = self.config.consecutive_scale_limit
        penalty = self.config.consecutive_scale_penalty

        if self._consecutive_up_count >= limit:
            extra = penalty * (self._consecutive_up_count - limit + 1)
            total = base + extra
            self.logger.info(
                "  [CONSEC-COOLDOWN] consecutive_up=%d >= limit=%d, "
                "warm_up: %d + %d = %ds",
                self._consecutive_up_count, limit,
                base, extra, total)
            return total
        return base

    def is_warming_up(self) -> bool:
        if self._last_scaling_time <= 0:
            return False
        elapsed = time.time() - self._last_scaling_time
        effective = self._effective_warm_up()
        if elapsed < effective:
            self.logger.info(
                "Warm-up: %ds / %ds (consecutive_up=%d)",
                int(elapsed), effective,
                self._consecutive_up_count)
            return True
        return False

    def _is_recovering(self, topology: JobTopology) -> bool:
        recovery_count = 0
        valid_count = 0

        for vid, m in topology.vertices.items():
            if not m.has_valid_metrics:
                continue
            valid_count += 1

            if m.busy_ratio >= self.config.recovery_busy_threshold:
                total_tp = (m.records_in_per_second
                            + m.records_out_per_second)
                if total_tp < self.config.recovery_throughput_min:
                    recovery_count += 1
                    self.logger.warning(
                        "  Recovery signal: %s busy=%.3f "
                        "in=%.0f/s out=%.0f/s",
                        m.vertex_name, m.busy_ratio,
                        m.records_in_per_second,
                        m.records_out_per_second)

        if (valid_count > 0
                and recovery_count >= max(valid_count // 2, 2)):
            self.logger.warning(
                "  [PAUSE] Detected restart recovery: "
                "%d/%d operators", recovery_count, valid_count)
            return True

        return False

    def _get_source_throughput(self, topology: JobTopology) -> float:
        """★ P10: 获取当前 Source 总吞吐"""
        for vid, m in topology.vertices.items():
            if not m.has_valid_metrics:
                continue
            # Source 没有上游
            upstream = topology.get_upstream(vid)
            if not upstream:
                return m.records_out_per_second
        return 0.0

    def _get_source_parallelism(self, topology: JobTopology) -> int:
        """★ P10: 获取当前 Source 并行度"""
        for vid, m in topology.vertices.items():
            upstream = topology.get_upstream(vid)
            if not upstream:
                return m.parallelism
        return 1

    def _is_high_load(self, topology: JobTopology) -> bool:
        """
        ★ P10: 高负载保护判定
        当 Source 吞吐 >= capacity * source_p * protection_ratio 时
        认为系统处于高负载, 禁止缩容
        """
        source_throughput = self._get_source_throughput(topology)
        source_p = self._get_source_parallelism(topology)
        capacity = self.config.source_capacity_per_instance
        ratio = self.config.high_load_protection_ratio

        threshold = capacity * source_p * ratio

        is_high = source_throughput >= threshold

        self.logger.info(
            "  [HIGH-LOAD-CHECK] source_out=%.1f r/s, "
            "threshold=%.1f (cap=%.0f * P=%d * ratio=%.2f) -> %s",
            source_throughput, threshold,
            capacity, source_p, ratio,
            "HIGH_LOAD" if is_high else "normal")

        return is_high

    def _apply_high_load_protection(
        self, approved: Dict[str, ScalingDecision],
        topology: JobTopology
    ) -> Dict[str, ScalingDecision]:
        """
        ★ P10: 如果处于高负载, 移除所有 SCALE_DOWN 决策
        理由: 高负载下缩容需要重启, 重启代价 > 节省的资源
        """
        if not self._is_high_load(topology):
            return approved

        down_decisions = {
            vid: d for vid, d in approved.items()
            if d.direction == "DOWN"
        }

        if not down_decisions:
            return approved

        blocked_names = [d.vertex_name for d in down_decisions.values()]
        self.logger.warning(
            "  [HIGH-LOAD-PROTECT] Blocking SCALE_DOWN for: %s "
            "(system under high load, restart not worth it)",
            ", ".join(blocked_names))

        # 清除这些算子的 filter history, 防止下一轮立刻再通过
        for vid in down_decisions:
            if vid in self.decision_filter._history:
                self.decision_filter._history[vid].clear()

        approved = {
            vid: d for vid, d in approved.items()
            if d.direction != "DOWN"
        }

        return approved

    def _apply_direction_lock(
        self, approved: Dict[str, ScalingDecision]
    ) -> Dict[str, ScalingDecision]:
        if not self._last_scale_direction:
            return approved

        elapsed = time.time() - self._last_scale_direction_time
        lock_period = self.config.direction_lock_seconds

        if elapsed >= lock_period:
            if self._last_scale_direction:
                self.logger.info(
                    "  [DIR-LOCK] Lock expired "
                    "(direction=%s, elapsed=%.0fs >= %ds)",
                    self._last_scale_direction, elapsed, lock_period)
                self._last_scale_direction = None
            return approved

        blocked_direction = ("DOWN"
                             if self._last_scale_direction == "UP"
                             else "UP")

        blocked = {
            vid: d for vid, d in approved.items()
            if d.direction == blocked_direction
        }

        if blocked:
            blocked_names = [d.vertex_name for d in blocked.values()]
            self.logger.warning(
                "  [DIR-LOCK] Active! Last=%s %.0fs ago "
                "(lock=%ds). Blocking %s for: %s",
                self._last_scale_direction, elapsed,
                lock_period, blocked_direction,
                ", ".join(blocked_names))

            for vid in blocked:
                if vid in self.decision_filter._history:
                    self.decision_filter._history[vid].clear()

            approved = {
                vid: d for vid, d in approved.items()
                if d.direction != blocked_direction
            }

        return approved

    def run(self):
        self.logger.info("=" * 60)
        self.logger.info("DS2 Controller Starting (P10)")
        self.logger.info(
            "  Flink REST     : %s", self.config.flink_rest_url)
        self.logger.info(
            "  Job Name       : %s", self.config.flink_job_name)
        self.logger.info(
            "  CG Suffix      : %s",
            self.consumer_group_suffix or "(none)")
        self.logger.info(
            "  Interval       : %ds", self.config.policy_interval)
        self.logger.info(
            "  Warm-up        : %ds", self.config.warm_up_time)
        self.logger.info(
            "  Act. Window    : %d", self.config.activation_window)
        self.logger.info(
            "  Rate Ratio     : %.1f",
            self.config.target_rate_ratio)
        self.logger.info(
            "  Parallelism    : [%d, %d]",
            self.config.min_parallelism,
            self.config.max_parallelism)
        self.logger.info(
            "  Source Capacity : %.0f r/s/inst",
            self.config.source_capacity_per_instance)
        self.logger.info(
            "  Source Max P    : %d",
            self.config.source_max_parallelism)
        self.logger.info(
            "  Max Scale      : %.1fx",
            self.config.max_scale_factor)
        self.logger.info(
            "  Use Savepoint  : %s", self.config.use_savepoint)
        self.logger.info(
            "  Wait RUNNING   : %ds",
            self.config.wait_running_timeout)
        self.logger.info(
            "  Direction Lock : %ds",
            self.config.direction_lock_seconds)
        self.logger.info(
            "  Post-restart Skip: %d rounds",
            self.config.post_restart_skip_rounds)
        self.logger.info(
            "  Consec. Limit  : %d",
            self.config.consecutive_scale_limit)
        self.logger.info(
            "  Consec. Penalty: %ds",
            self.config.consecutive_scale_penalty)
        # ★ P10
        self.logger.info(
            "  High-Load Prot.: %.0f%%",
            self.config.high_load_protection_ratio * 100)
        self.logger.info("=" * 60)

        if not self.client.check_connection():
            self.logger.error("Cannot connect to Flink, exit")
            return

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        while self._running:
            try:
                self._run_one_round()
            except KeyboardInterrupt:
                self.logger.info("KeyboardInterrupt, stopping")
                break
            except Exception as e:
                self.logger.error(
                    "Round error: %s", e, exc_info=True)

            for _ in range(self.config.policy_interval):
                if not self._running:
                    break
                time.sleep(1)

        self.logger.info(
            "DS2 Controller stopped "
            "(total %d scaling operations)",
            self.executor._scaling_count)

    def _run_one_round(self):
        self._round += 1
        self.logger.info("")
        self.logger.info("#" * 60)
        self.logger.info("# Round %d", self._round)
        self.logger.info("#" * 60)

        job_id = self.client.get_running_job_id()
        if not job_id:
            self.logger.info("No running job, waiting ...")
            return

        if self.is_warming_up():
            self.logger.info("Warming up, skip")
            return

        topology = self.client.get_job_topology(job_id)
        if not topology:
            self.logger.warning("Failed to get topology")
            return

        self.logger.info(
            "Job: %s (%s)", topology.job_name, job_id)
        self.logger.info(
            "Vertices: %d, Edges: %d",
            len(topology.vertices), len(topology.edges))
        for vid, m in topology.vertices.items():
            self.logger.info(
                "  %s (P=%d, arg=%s)",
                m.vertex_name, m.parallelism, m.arg_name)

        has_data = self.collector.collect(topology)
        if not has_data or not self.collector.has_enough_samples():
            self.logger.info(
                "Need at least 2 samples for diff, skip")
            return

        if self._is_recovering(topology):
            self.logger.info(
                "  Skipping this round (restart recovery)")
            self.collector.reset()
            return

        all_decisions = self.model.compute(topology)

        approved = self.decision_filter.filter(
            all_decisions, topology)

        # P8: 方向锁
        if approved:
            approved = self._apply_direction_lock(approved)

        # ★ P10: 高负载保护 (在方向锁之后再过滤一次)
        if approved:
            approved = self._apply_high_load_protection(
                approved, topology)

        executed = False
        if approved:
            self.logger.info(
                "%d change(s) approved, executing ...",
                len(approved))

            batch_direction = self.executor.get_last_direction(
                approved)

            executed = self.executor.execute(
                job_id, approved, all_decisions)
            if executed:
                self._last_scaling_time = time.time()
                self.collector.reset()
                self.decision_filter.reset()

                # P8: 方向锁
                self._last_scale_direction = batch_direction
                self._last_scale_direction_time = time.time()

                # P9: 累计连续扩容计数
                if batch_direction == "UP":
                    self._consecutive_up_count += 1
                    self.logger.info(
                        "  [CONSEC] UP count -> %d "
                        "(limit=%d, penalty=%ds/each)",
                        self._consecutive_up_count,
                        self.config.consecutive_scale_limit,
                        self.config.consecutive_scale_penalty)
                else:
                    if self._consecutive_up_count > 0:
                        self.logger.info(
                            "  [CONSEC] Reset UP count "
                            "(was %d, direction=%s)",
                            self._consecutive_up_count,
                            batch_direction)
                    self._consecutive_up_count = 0

                eff_warm = self._effective_warm_up()
                self.logger.info(
                    "Scaling done (direction=%s), "
                    "effective warm-up %ds + direction lock %ds",
                    batch_direction, eff_warm,
                    self.config.direction_lock_seconds)
        else:
            self.logger.info("No approved changes")

        self.csv_logger.log_round(
            self._round, topology,
            all_decisions, approved, executed)

    def _handle_signal(self, signum, frame):
        self.logger.info(
            "Signal %d received, stopping ...", signum)
        self._running = False


# ============================================================================
# 10. 工具函数
# ============================================================================

def setup_logging(level: str = "INFO"):
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(name)-15s] %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(numeric_level)
    console.setFormatter(formatter)

    os.makedirs("ds2_logs", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_handler = logging.FileHandler(
        "ds2_logs/ds2_%s.log" % ts, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(file_handler)


def run_dry(config: Config):
    logger = logging.getLogger("DS2.DryRun")
    client = FlinkClient(config)

    if not client.check_connection():
        logger.error("Cannot connect to Flink")
        return

    job_id = client.get_running_job_id()
    if not job_id:
        logger.error("No running job found")
        return

    topology = client.get_job_topology(job_id)
    if not topology:
        logger.error("Failed to get topology")
        return

    logger.info("Job: %s (%s)", topology.job_name, job_id)
    logger.info("Vertices (%d):", len(topology.vertices))
    for vid, m in topology.vertices.items():
        logger.info(
            "  %s  P=%d  arg=%s",
            m.vertex_name, m.parallelism, m.arg_name)

    logger.info("Edges (%d):", len(topology.edges))
    for src, dst in topology.edges:
        sn = topology.vertices.get(src)
        dn = topology.vertices.get(dst)
        logger.info(
            "  %s -> %s",
            sn.vertex_name if sn else src,
            dn.vertex_name if dn else dst)

    logger.info("")
    logger.info("Topological order:")
    for i, vid in enumerate(topology.topological_sort()):
        logger.info(
            "  %d. %s",
            i + 1, topology.vertices[vid].vertex_name)

    collector = MetricsCollector(client, config)
    logger.info("")
    logger.info("--- Sample 1 ---")
    collector.collect(topology)

    logger.info(
        "Waiting %ds for sample 2 ...",
        config.policy_interval)
    time.sleep(config.policy_interval)

    topology = client.get_job_topology(job_id)
    if not topology:
        logger.error("Failed to get topology on 2nd sample")
        return

    logger.info("--- Sample 2 ---")
    has_data = collector.collect(topology)
    if not has_data:
        logger.error("No valid metrics after 2 samples")
        return

    logger.info("")
    logger.info("=" * 80)
    logger.info(
        "%-25s %8s %8s %6s %10s %6s %3s",
        "Operator", "In/s", "Out/s", "Busy",
        "TrueRate", "Sel", "P")
    logger.info("-" * 80)
    for vid in topology.topological_sort():
        m = topology.vertices[vid]
        if m.has_valid_metrics:
            logger.info(
                "%-25s %8.1f %8.1f %6.3f %10.1f %6.3f %3d",
                m.vertex_name,
                m.records_in_per_second,
                m.records_out_per_second,
                m.busy_ratio,
                m.true_processing_rate,
                m.selectivity,
                m.parallelism)
        else:
            logger.info("%-25s %8s", m.vertex_name, "(no data)")
    logger.info("=" * 80)

    model = DS2Model(config)
    decisions = model.compute(topology)

    executor = ScalingExecutor(client, config, "")
    args_preview = executor._build_program_args(
        decisions, decisions)
    logger.info("")
    logger.info("programArgs preview: %s", args_preview)
    logger.info("")
    logger.info("Dry-run complete. No changes applied.")


def main():
    parser = argparse.ArgumentParser(
        description="DS2 Auto-Scaling Controller (P10)")
    parser.add_argument(
        "-c", "--config",
        default="ds2_config.yaml",
        help="Config file path (default: ds2_config.yaml)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute decisions only, do not execute scaling")
    parser.add_argument(
        "--consumer-group-suffix",
        default="",
        help="Consumer Group suffix (passed by experiment_v3.py)")
    args = parser.parse_args()

    config = Config.from_yaml(args.config)
    config.validate()

    if not config.operator_mapping:
        print("[WARN] No operator_mapping, using built-in defaults")
        config.operator_mapping = {
            "source": OperatorConfig(
                "RocketMQ-Source", "p-source", 2),
            "watermark": OperatorConfig(
                "Watermark-Assigner", "p-source", 2),
            "filter": OperatorConfig(
                "Hardware-Filter", "p-filter", 4),
            "signal": OperatorConfig(
                "Signal-Extraction", "p-signal", 4),
            "feature": OperatorConfig(
                "Feature-Extraction", "p-feature", 4),
            "grid": OperatorConfig(
                "Grid-Aggregation", "p-grid", 4),
            "sink_event": OperatorConfig(
                "EventFeature-Sink", "p-sink", 2),
            "sink_grid": OperatorConfig(
                "GridSummary-Sink", "p-sink", 2),
        }

    setup_logging(config.log_level)

    logger = logging.getLogger("DS2.Main")
    logger.info("DS2 Configuration (P10):")
    logger.info(
        "  flink_rest_url     = %s", config.flink_rest_url)
    logger.info(
        "  flink_job_name     = %s", config.flink_job_name)
    logger.info(
        "  consumer_suffix    = %s",
        args.consumer_group_suffix or "(none)")
    logger.info(
        "  policy_interval    = %ds", config.policy_interval)
    logger.info(
        "  warm_up_time       = %ds", config.warm_up_time)
    logger.info(
        "  activation_window  = %d", config.activation_window)
    logger.info(
        "  target_rate_ratio  = %.1f", config.target_rate_ratio)
    logger.info(
        "  parallelism range  = [%d, %d]",
        config.min_parallelism, config.max_parallelism)
    logger.info(
        "  source_capacity    = %.0f r/s/inst",
        config.source_capacity_per_instance)
    logger.info(
        "  source_max_p       = %d",
        config.source_max_parallelism)
    logger.info(
        "  max_scale_factor   = %.1fx",
        config.max_scale_factor)
    logger.info(
        "  use_savepoint      = %s", config.use_savepoint)
    logger.info(
        "  wait_running_timeout = %ds",
        config.wait_running_timeout)
    logger.info(
        "  direction_lock     = %ds",
        config.direction_lock_seconds)
    logger.info(
        "  post_restart_skip  = %d rounds",
        config.post_restart_skip_rounds)
    logger.info(
        "  consec_scale_limit   = %d",
        config.consecutive_scale_limit)
    logger.info(
        "  consec_scale_penalty = %ds",
        config.consecutive_scale_penalty)
    # ★ P10
    logger.info(
        "  high_load_prot_ratio = %.2f",
        config.high_load_protection_ratio)
    logger.info(
        "  target_jar_name    = %s", config.target_jar_name)
    logger.info(
        "  operator_mapping   = %d operators:",
        len(config.operator_mapping))
    for key, op in config.operator_mapping.items():
        logger.info(
            "    %-12s keyword='%s'  arg='%s'  default_p=%d",
            key, op.keyword, op.arg_name,
            op.default_parallelism)

    if args.dry_run:
        logger.info("MODE: dry-run")
        run_dry(config)
    else:
        logger.info("MODE: live")
        controller = DS2Controller(
            config,
            consumer_group_suffix=args.consumer_group_suffix)
        controller.run()


if __name__ == "__main__":
    main()
