#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DS2 自动扩缩容控制器（算子级并行度版）—— 稳定性修复版
=========================================================
基于 OSDI '18 论文 "Three Steps is All You Need" 实现
适配 Apache Flink 1.17 Standalone 集群

修复记录:
  P1: 全局并行度 -> 算子级并行度（通过 programArgs 下发）
  P2: 作业名统一为 Seismic-Stream-Job
  P3: idle 算子处理 + 级联膨胀修复
  P4: 稳定性修复（本版本）
      - 初始预热防止过早首次伸缩
      - 重启恢复期检测，跳过 busy=1.0 假象
      - 空操作拦截，不做无意义重启
      - 单次扩缩幅度限制（最多翻倍/减半）
      - 显著性检查，仅 idle 变化不触发重启
      - 反级联放大，cap target_input 增长
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
    """单个算子的配置"""
    keyword: str
    arg_name: str
    default_parallelism: int = 4


@dataclass
class Config:
    """DS2 全局配置"""

    # Flink 集群
    flink_rest_url: str = "http://localhost:8081"
    flink_job_name: str = "Seismic-Stream-Job"
    flink_savepoint_dir: str = "/tmp/flink-savepoints"

    # DS2 核心参数
    policy_interval: int = 15
    warm_up_time: int = 30
    activation_window: int = 3
    target_rate_ratio: float = 1.5
    min_busy_ratio: float = 0.05
    idle_busy_threshold: float = 0.1
    max_parallelism: int = 16
    min_parallelism: int = 1
    change_threshold: int = 0
    target_jar_name: str = "flink-rocketmq-demo-1.0-SNAPSHOT"

    # P4 新增：稳定性参数
    max_scale_factor: float = 2.0         # 单次最多翻倍/减半
    recovery_busy_threshold: float = 0.95 # busy 高于此值视为可能在恢复中
    recovery_throughput_min: float = 100.0 # 恢复期吞吐低于此值才判定
    min_significant_busy: float = 0.30    # 仅 busy 低于此值的算子变化不触发重启
    initial_grace_period: int = 60        # 启动后等待多少秒才允许首次伸缩

    # 算子映射
    operator_mapping: Dict[str, OperatorConfig] = field(default_factory=dict)

    # 日志
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
                config.flink_rest_url = flink.get('rest_url', config.flink_rest_url)
                config.flink_job_name = flink.get('job_name', config.flink_job_name)
                config.flink_savepoint_dir = flink.get('savepoint_dir', config.flink_savepoint_dir)

            if 'ds2' in data:
                ds2 = data['ds2']
                config.policy_interval     = ds2.get('policy_interval',     config.policy_interval)
                config.warm_up_time        = ds2.get('warm_up_time',        config.warm_up_time)
                config.activation_window   = ds2.get('activation_window',   config.activation_window)
                config.target_rate_ratio   = ds2.get('target_rate_ratio',   config.target_rate_ratio)
                config.min_busy_ratio      = ds2.get('min_busy_ratio',      config.min_busy_ratio)
                config.idle_busy_threshold = ds2.get('idle_busy_threshold', config.idle_busy_threshold)
                config.max_parallelism     = ds2.get('max_parallelism',     config.max_parallelism)
                config.min_parallelism     = ds2.get('min_parallelism',     config.min_parallelism)
                config.change_threshold    = ds2.get('change_threshold',    config.change_threshold)
                config.target_jar_name     = ds2.get('target_jar_name',     config.target_jar_name)

                # P4 新增参数
                config.max_scale_factor         = ds2.get('max_scale_factor',         config.max_scale_factor)
                config.recovery_busy_threshold  = ds2.get('recovery_busy_threshold',  config.recovery_busy_threshold)
                config.recovery_throughput_min  = ds2.get('recovery_throughput_min',   config.recovery_throughput_min)
                config.min_significant_busy     = ds2.get('min_significant_busy',     config.min_significant_busy)
                config.initial_grace_period     = ds2.get('initial_grace_period',     config.initial_grace_period)

                # cooldown_seconds 是 warm_up_time 的别名
                if 'cooldown_seconds' in ds2:
                    config.warm_up_time = ds2['cooldown_seconds']

            if 'operator_mapping' in data:
                for key, val in data['operator_mapping'].items():
                    config.operator_mapping[key] = OperatorConfig(
                        keyword=val.get('keyword', key),
                        arg_name=val.get('arg_name', f'p-{key}'),
                        default_parallelism=val.get('default_parallelism', 4)
                    )

            if 'logging' in data:
                config.log_level = data['logging'].get('level', config.log_level)

            print(f"[INFO] Config loaded from {path}")
            print(f"[INFO] Operator mappings: {list(config.operator_mapping.keys())}")

        except Exception as e:
            print(f"[WARN] Failed to load config: {e}, using defaults")

        return config

    def validate(self) -> bool:
        assert self.policy_interval >= 5,            "policy_interval must be >= 5"
        assert self.warm_up_time >= 0,               "warm_up_time must be >= 0"
        assert self.activation_window >= 1,          "activation_window must be >= 1"
        assert 1.0 <= self.target_rate_ratio <= 2.0, "target_rate_ratio must be in [1.0, 2.0]"
        assert self.max_parallelism >= self.min_parallelism, "max >= min parallelism"
        assert self.max_scale_factor >= 1.0,         "max_scale_factor must be >= 1.0"
        return True

    def find_operator_config(self, vertex_name: str) -> Optional[OperatorConfig]:
        for key, op_config in self.operator_mapping.items():
            if op_config.keyword in vertex_name:
                return op_config
        return None


# ============================================================================
# 2. 数据结构
# ============================================================================

@dataclass
class OperatorMetrics:
    """算子运行时指标容器"""
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
    """单个算子的扩缩容决策"""
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


@dataclass
class JobTopology:
    """Flink 作业拓扑图"""
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
            self.logger.debug(f"GET {endpoint} -> {resp.status_code}")
            return None
        except requests.exceptions.Timeout:
            self.logger.warning(f"GET {endpoint} timeout")
            return None
        except requests.exceptions.ConnectionError:
            self.logger.warning(f"GET {endpoint} connection failed")
            return None
        except Exception as e:
            self.logger.debug(f"GET {endpoint} error: {e}")
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
            self.logger.error(f"POST {endpoint} error: {e}")
            return False, None

    def _patch(self, endpoint: str, timeout: int = 30) -> bool:
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self.session.patch(url, timeout=timeout)
            return resp.status_code < 400
        except Exception as e:
            self.logger.error(f"PATCH {endpoint} error: {e}")
            return False

    def check_connection(self) -> bool:
        data = self._get("/overview")
        if data:
            self.logger.info(f"Connected to Flink {data.get('flink-version', 'unknown')}")
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
        self.logger.debug(f"No running job named '{self.config.flink_job_name}'")
        return None

    def get_job_topology(self, job_id: str) -> Optional[JobTopology]:
        data = self._get(f"/jobs/{job_id}")
        if not data:
            return None

        topology = JobTopology(job_id=job_id, job_name=data.get('name', ''))

        for vertex in data.get('vertices', []):
            vid   = vertex['id']
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

    def get_vertex_metrics(self, job_id: str, vertex_id: str) -> Dict[str, List[float]]:
        params = "?get=" + ",".join(self.REQUIRED_METRICS)
        endpoint = f"/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics"
        data = self._get(endpoint + params)
        if not data:
            return {}

        result: Dict[str, List[float]] = {m: [] for m in self.REQUIRED_METRICS}

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

        if isinstance(data, list) and data and 'metrics' in data[0]:
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
                    agg[mid] = safe_float(m.get('sum', m.get('value', 0)))
            for metric in self.REQUIRED_METRICS:
                result[metric].append(agg.get(metric, 0.0))

        return result

    def trigger_savepoint(self, job_id: str) -> Optional[str]:
        self.logger.info("Triggering savepoint ...")
        self.logger.info(f"  target dir: {self.config.flink_savepoint_dir}")

        ok, resp = self._post(
            f"/jobs/{job_id}/savepoints",
            {"target-directory": self.config.flink_savepoint_dir, "cancel-job": False},
        )
        if not ok or not resp:
            self.logger.error(f"Savepoint trigger failed: {resp}")
            return None

        trigger_id = resp.get("request-id")
        if not trigger_id:
            self.logger.error(f"No request-id in response: {resp}")
            return None

        for _ in range(60):
            time.sleep(1)
            status = self._get(f"/jobs/{job_id}/savepoints/{trigger_id}")
            if not status:
                continue
            state = status.get("status", {}).get("id")
            if state == "COMPLETED":
                location = status.get("operation", {}).get("location")
                if not location:
                    cause = status.get("operation", {}).get("failure-cause", {})
                    self.logger.error(f"Savepoint completed but no location: {cause}")
                    return None
                self.logger.info(f"Savepoint done: {location}")
                return location
            elif state == "FAILED":
                cause = status.get("operation", {}).get("failure-cause", {})
                self.logger.error(f"Savepoint failed: {cause}")
                return None

        self.logger.error("Savepoint timeout (60 s)")
        return None

    def cancel_job(self, job_id: str) -> bool:
        self.logger.info(f"Cancelling job {job_id} ...")
        if not self._patch(f"/jobs/{job_id}?mode=cancel"):
            self.logger.error("Cancel request failed")
            return False

        for _ in range(30):
            time.sleep(1)
            data = self._get(f"/jobs/{job_id}")
            if data and data.get("state") in ("CANCELED", "CANCELLED", "FINISHED"):
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
        self.logger.warning(f"JAR not found: {self.config.target_jar_name}")
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
        self.logger.info(f"Submitting job: global_parallelism={parallelism}")
        self.logger.info(f"  programArgs : {program_args}")
        if savepoint_path:
            self.logger.info(f"  savepoint   : {savepoint_path}")

        ok, resp = self._post(f"/jars/{jar_id}/run", body, timeout=60)
        if ok and resp:
            new_jid = resp.get("jobid")
            self.logger.info(f"Job submitted: {new_jid}")
            return new_jid
        self.logger.error(f"Job submission failed: {resp}")
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

    def reset(self):
        self._prev_snapshot.clear()
        self._prev_time = 0.0
        self.logger.info("Metrics collector reset")

    def collect(self, topology: JobTopology) -> bool:
        now = time.time()
        current_snapshot: Dict[str, Dict[str, float]] = {}
        has_prev = bool(self._prev_snapshot) and self._prev_time > 0

        for vid, metrics in topology.vertices.items():
            raw = self.client.get_vertex_metrics(topology.job_id, vid)
            if not raw:
                self.logger.debug(f"No raw metrics for {metrics.vertex_name}")
                metrics.has_valid_metrics = False
                continue

            total_in   = sum(raw.get("numRecordsIn",         [0.0]))
            total_out  = sum(raw.get("numRecordsOut",        [0.0]))
            total_busy = sum(raw.get("accumulateBusyTimeMs", [0.0]))

            current_snapshot[vid] = {
                "numRecordsIn":         total_in,
                "numRecordsOut":        total_out,
                "accumulateBusyTimeMs": total_busy,
            }

            if has_prev and vid in self._prev_snapshot:
                dt = now - self._prev_time
                if dt < 1.0:
                    metrics.has_valid_metrics = False
                    continue

                prev = self._prev_snapshot[vid]
                delta_in   = max(total_in   - prev["numRecordsIn"],         0.0)
                delta_out  = max(total_out  - prev["numRecordsOut"],        0.0)
                delta_busy = max(total_busy - prev["accumulateBusyTimeMs"], 0.0)

                metrics.records_in_per_second  = delta_in  / dt
                metrics.records_out_per_second = delta_out / dt

                p = max(metrics.parallelism, 1)
                metrics.busy_ratio = min(delta_busy / (dt * 1000.0 * p), 1.0)

                if metrics.records_in_per_second > 1.0:
                    metrics.selectivity = (
                        metrics.records_out_per_second / metrics.records_in_per_second
                    )
                else:
                    metrics.selectivity = 1.0

                eff_busy = max(metrics.busy_ratio, self.config.min_busy_ratio)
                metrics.true_processing_rate = metrics.records_in_per_second  / eff_busy
                metrics.true_output_rate     = metrics.records_out_per_second / eff_busy

                metrics.has_valid_metrics = True

                self.logger.debug(
                    f"  {metrics.vertex_name:<25} "
                    f"in={metrics.records_in_per_second:8.1f}/s "
                    f"out={metrics.records_out_per_second:8.1f}/s "
                    f"busy={metrics.busy_ratio:.3f} "
                    f"true={metrics.true_processing_rate:8.1f}/s "
                    f"sel={metrics.selectivity:.3f}"
                )
            else:
                metrics.has_valid_metrics = False

        self._prev_snapshot = current_snapshot
        self._prev_time = now

        valid = sum(1 for v in topology.vertices.values() if v.has_valid_metrics)
        self.logger.info(
            f"Metrics collected: {valid}/{len(topology.vertices)} vertices valid"
        )
        return valid > 0


# ============================================================================
# 5. DS2 决策模型（P4 稳定性修复版）
# ============================================================================

class DS2Model:
    """
    DS2 目标并行度推导模型

    P4 修复：
      - _apply_amplitude_limit(): 单次扩缩幅度限制
      - 反级联放大: cap target_input 增长
    """

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("DS2.Model")
        self.idle_threshold = config.idle_busy_threshold

    # ----------------------------------------------------------------
    # P4 新增: 幅度限制工具方法
    # ----------------------------------------------------------------
    def _apply_amplitude_limit(self, current_p: int, raw_target_p: int) -> int:
        """
        限制单次扩缩幅度，防止并行度暴涨暴跌。
        - 扩容：最多翻 max_scale_factor 倍
        - 缩容：最多缩到 1/max_scale_factor
        """
        max_factor = self.config.max_scale_factor

        if raw_target_p > current_p:
            capped = min(raw_target_p, math.ceil(current_p * max_factor))
            capped = min(capped, self.config.max_parallelism)
            capped = max(capped, self.config.min_parallelism)
            if capped != raw_target_p:
                self.logger.info(
                    f"    幅度限制(扩容): {raw_target_p} -> {capped} "
                    f"(当前={current_p}, 最大 {max_factor}x)"
                )
            return capped

        elif raw_target_p < current_p:
            floored = max(raw_target_p, math.ceil(current_p / max_factor))
            floored = max(floored, self.config.min_parallelism)
            if floored != raw_target_p:
                self.logger.info(
                    f"    幅度限制(缩容): {raw_target_p} -> {floored} "
                    f"(当前={current_p}, 最小 1/{max_factor}x)"
                )
            return floored

        return raw_target_p

    def compute(self, topology: JobTopology) -> Dict[str, ScalingDecision]:
        decisions: Dict[str, ScalingDecision] = {}
        target_output_rate: Dict[str, float] = {}

        self.logger.info("=" * 60)
        self.logger.info("DS2 Model Computing ...")
        self.logger.info("=" * 60)

        for vid in topology.topological_sort():
            metrics = topology.vertices[vid]
            upstream_ids = topology.get_upstream(vid)
            current_p = metrics.parallelism
            arg_name = metrics.arg_name

            self.logger.info(f"\n--- {metrics.vertex_name} (P={current_p}) ---")

            # ── 无有效指标：保持现状 ──
            if not metrics.has_valid_metrics:
                decisions[vid] = ScalingDecision(
                    vid, metrics.vertex_name, arg_name,
                    current_p, current_p, "No valid metrics"
                )
                target_output_rate[vid] = metrics.records_out_per_second
                self.logger.info(f"  -> keep P={current_p} (no metrics)")
                continue

            # ── Source 算子 ──
            if not upstream_ids:
                target_p = self._compute_source_parallelism(metrics)
                decisions[vid] = ScalingDecision(
                    vid, metrics.vertex_name, arg_name,
                    current_p, target_p,
                    f"Source busy={metrics.busy_ratio:.3f}"
                )

                # 传播输出速率（按并行度等比缩放，但受幅度限制约束）
                max_factor = self.config.max_scale_factor
                if current_p > 0 and target_p != current_p:
                    projected = metrics.records_out_per_second * target_p / current_p
                    # P4: 反级联放大 — cap 投影增长
                    max_projected = metrics.records_out_per_second * max_factor
                    target_output_rate[vid] = min(projected, max_projected)
                else:
                    target_output_rate[vid] = metrics.records_out_per_second

                self.logger.info(
                    f"  actual_out={metrics.records_out_per_second:.1f}/s  "
                    f"busy={metrics.busy_ratio:.3f}  "
                    f"target_out={target_output_rate[vid]:.1f}/s  "
                    f"-> P: {current_p} -> {target_p}"
                )
                continue

            # ── 非 Source 算子 ──

            # Step 1: 从上游聚合目标输入速率
            target_input = sum(
                target_output_rate.get(uid, 0.0) for uid in upstream_ids
            )

            # P4: 反级联放大 — cap target_input 增长
            actual_input = metrics.records_in_per_second
            if actual_input > self.config.recovery_throughput_min:
                max_growth = self.config.max_scale_factor * self.config.target_rate_ratio
                max_target_input = actual_input * max_growth
                if target_input > max_target_input:
                    self.logger.info(
                        f"  反级联: target_input {target_input:.0f} -> "
                        f"{max_target_input:.0f} (actual={actual_input:.0f} "
                        f"x {max_growth:.1f})"
                    )
                    target_input = max_target_input

            self.logger.info(f"  target_input={target_input:.1f}/s")

            # Step 2: 判断是否 idle
            if metrics.busy_ratio < self.idle_threshold:
                target_p, reason, out_rate = self._handle_idle_operator(
                    metrics, target_input, current_p
                )
                # P4: 对 idle 算子也应用幅度限制
                target_p = self._apply_amplitude_limit(current_p, target_p)

                decisions[vid] = ScalingDecision(
                    vid, metrics.vertex_name, arg_name,
                    current_p, target_p, reason
                )
                target_output_rate[vid] = out_rate
                self.logger.info(
                    f"  busy={metrics.busy_ratio:.3f} < {self.idle_threshold} "
                    f"-> {reason}  P: {current_p} -> {target_p}  "
                    f"target_out={out_rate:.1f}/s"
                )
                continue

            # Step 3: 计算每实例真实处理能力
            eff_busy = max(metrics.busy_ratio, self.config.min_busy_ratio)
            true_rate = metrics.records_in_per_second / eff_busy
            per_inst_rate = true_rate / max(current_p, 1)

            if per_inst_rate < 1.0:
                self.logger.warning(
                    f"  per_inst_rate={per_inst_rate:.2f} too low, "
                    f"keep P={current_p}"
                )
                decisions[vid] = ScalingDecision(
                    vid, metrics.vertex_name, arg_name,
                    current_p, current_p,
                    f"per_inst_rate too low: {per_inst_rate:.2f}"
                )
                target_output_rate[vid] = target_input * metrics.selectivity
                continue

            # Step 4: 目标并行度
            target_input_margin = target_input * self.config.target_rate_ratio
            raw_p = math.ceil(target_input_margin / per_inst_rate)
            target_p = max(
                self.config.min_parallelism,
                min(raw_p, self.config.max_parallelism)
            )

            # P4: 应用幅度限制
            target_p = self._apply_amplitude_limit(current_p, target_p)

            # 传播实际流量
            target_output_rate[vid] = target_input * metrics.selectivity

            decisions[vid] = ScalingDecision(
                vid, metrics.vertex_name, arg_name,
                current_p, target_p,
                f"in={target_input_margin:.1f} per_inst={per_inst_rate:.1f} "
                f"raw_p={raw_p} busy={metrics.busy_ratio:.3f}"
            )

            self.logger.info(
                f"  per_inst={per_inst_rate:.1f}/s  "
                f"target_in_margin={target_input_margin:.1f}/s  "
                f"raw_p={raw_p}  -> P: {current_p} -> {target_p}  "
                f"target_out={target_output_rate[vid]:.1f}/s"
            )

        self._log_summary(decisions)
        return decisions

    def _compute_source_parallelism(self, metrics: OperatorMetrics) -> int:
        p = metrics.parallelism
        busy = metrics.busy_ratio

        # Source busy 补偿（RocketMQ Source busy 经常为 0）
        if busy < 0.001 and metrics.records_out_per_second > 0:
            capacity_per_instance = 2500.0
            estimated_busy = (metrics.records_out_per_second / max(p, 1)) / capacity_per_instance
            busy = min(estimated_busy, 1.0)
            self.logger.info(
                f"  [Source 补偿] 吞吐反推 busy = {busy:.3f} "
                f"(out_rate={metrics.records_out_per_second:.1f}/s)"
            )

        target_util = 1.0 / self.config.target_rate_ratio  # 0.667

        if busy > target_util:
            raw_p = math.ceil(p * busy / target_util)
            clamped = max(self.config.min_parallelism,
                          min(raw_p, self.config.max_parallelism))
            # P4: 应用幅度限制
            return self._apply_amplitude_limit(p, clamped)

        if busy < target_util * 0.5 and p > self.config.min_parallelism:
            raw_p = math.ceil(p * busy / target_util)
            raw_p = max(raw_p, self.config.min_parallelism)
            clamped = max(raw_p, p - 1)
            # P4: 应用幅度限制
            return self._apply_amplitude_limit(p, clamped)

        return p

    def _handle_idle_operator(
            self,
            metrics: OperatorMetrics,
            target_input: float,
            current_p: int
    ) -> Tuple[int, str, float]:
        out_rate = target_input * metrics.selectivity

        if target_input <= 0.0:
            target_p = self.config.min_parallelism
            return (
                target_p,
                f"No upstream data, set min_p={target_p}",
                out_rate
            )

        eff_busy = max(metrics.busy_ratio, self.config.min_busy_ratio)
        per_inst_rate = (metrics.records_in_per_second / eff_busy) / max(current_p, 1)

        if per_inst_rate < 1.0:
            return (
                current_p,
                f"per_inst_rate={per_inst_rate:.2f} too low, keep P",
                out_rate
            )

        target_input_margin = target_input * self.config.target_rate_ratio

        needed_p = math.ceil(target_input_margin / per_inst_rate)
        if needed_p < current_p:
            needed_p = max(needed_p, math.ceil(current_p / 2))
        target_p = max(
            self.config.min_parallelism,
            min(needed_p, self.config.max_parallelism)
        )
        # 注意: 外层 compute() 会再调用 _apply_amplitude_limit

        if target_p < current_p:
            reason = (
                f"Idle+underloaded: per_inst={per_inst_rate:.1f} "
                f"needed={needed_p} -> SCALE_DOWN"
            )
        elif target_p > current_p:
            reason = (
                f"Idle but future load high: per_inst={per_inst_rate:.1f} "
                f"needed={needed_p} -> SCALE_UP"
            )
        else:
            reason = (
                f"Idle (busy={metrics.busy_ratio:.3f}), capacity OK"
            )

        return target_p, reason, out_rate

    def _check_idle_capacity(self, metrics: OperatorMetrics,
                             target_input_margin: float) -> bool:
        if metrics.busy_ratio < 0.001:
            if metrics.records_in_per_second < 1.0:
                return True
            ratio = target_input_margin / max(metrics.records_in_per_second, 1.0)
            return ratio <= 5.0

        estimated_capacity = metrics.records_in_per_second / metrics.busy_ratio
        return target_input_margin <= estimated_capacity * 0.8

    def _log_summary(self, decisions: Dict[str, ScalingDecision]):
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Decision Summary:")
        self.logger.info("-" * 60)
        for vid, d in decisions.items():
            tag = ""
            if d.action == "SCALE_UP":
                tag = " [UP]"
            elif d.action == "SCALE_DOWN":
                tag = " [DOWN]"
            self.logger.info(
                f"  {d.vertex_name:<45} "
                f"{d.current_parallelism} -> {d.target_parallelism}  "
                f"{d.action}{tag}  ({d.reason})"
            )
        self.logger.info("=" * 60)


# ============================================================================
# 6. 决策过滤器（Activation Window + Change Threshold）—— 修复版
# ============================================================================

class DecisionFilter:
    """
    稳定性过滤器：
      1. 方向一致性：窗口内所有建议方向相同即可放行
      2. 取中位数作为最终目标并行度
      3. 高负载快速通道：busy >= 0.9 只需2轮方向一致
    """

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("DS2.Filter")
        self._history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=config.activation_window)
        )
        self._busy_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=config.activation_window)
        )
        self.high_busy_threshold = 0.9
        self.fast_track_window = 2

    def reset(self):
        self._history.clear()
        self._busy_history.clear()
        self.logger.info("Decision filter reset")

    def filter(self, decisions: Dict[str, ScalingDecision],
               topology: JobTopology = None) -> Dict[str, ScalingDecision]:
        approved: Dict[str, ScalingDecision] = {}

        for vid, decision in decisions.items():
            self._history[vid].append(decision.target_parallelism)

            if topology and vid in topology.vertices:
                busy = topology.vertices[vid].busy_ratio
                self._busy_history[vid].append(busy)

            diff = abs(decision.target_parallelism - decision.current_parallelism)
            if diff <= self.config.change_threshold:
                continue

            history = list(self._history[vid])
            current_p = decision.current_parallelism

            # ---------- 快速通道：高负载算子 ----------
            if self._is_high_busy(vid):
                if len(history) >= self.fast_track_window:
                    recent = history[-self.fast_track_window:]
                    if self._direction_consistent(recent, current_p):
                        final_target = self._compute_median(recent)
                        if final_target != current_p:
                            decision.target_parallelism = final_target
                            approved[vid] = decision
                            self.logger.info(
                                f"  [FAST-APPROVED] {decision.vertex_name}: "
                                f"{current_p} -> {final_target} "
                                f"(high-busy fast track, window={recent})"
                            )
                            continue
                    else:
                        self.logger.debug(
                            f"  {decision.vertex_name}: high-busy but direction "
                            f"inconsistent {recent}, wait"
                        )
                        continue
                else:
                    self.logger.debug(
                        f"  {decision.vertex_name}: high-busy, "
                        f"{len(history)}/{self.fast_track_window} samples, wait"
                    )
                    continue

            # ---------- 常规通道 ----------
            if len(history) < self.config.activation_window:
                self.logger.debug(
                    f"  {decision.vertex_name}: activation window "
                    f"{len(history)}/{self.config.activation_window}, wait"
                )
                continue

            recent = history[-self.config.activation_window:]

            if not self._direction_consistent(recent, current_p):
                self.logger.debug(
                    f"  {decision.vertex_name}: direction inconsistent "
                    f"window={recent} vs current={current_p}, skip"
                )
                continue

            final_target = self._compute_median(recent)

            if final_target != current_p:
                decision.target_parallelism = final_target
                approved[vid] = decision
                self.logger.info(
                    f"  [APPROVED] {decision.vertex_name}: "
                    f"{current_p} -> {final_target} "
                    f"(window={recent}, median={final_target})"
                )

        if not approved:
            self.logger.info("  No decisions approved this round")

        return approved

    def _direction_consistent(self, window: List[int], current_p: int) -> bool:
        directions = []
        for p in window:
            if p > current_p:
                directions.append("UP")
            elif p < current_p:
                directions.append("DOWN")
            else:
                directions.append("SAME")

        unique = set(directions)
        if "UP" in unique and "DOWN" in unique:
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
        else:
            return math.ceil((sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2)


# ============================================================================
# 7. 扩缩容执行器（P4 修复：空操作拦截）
# ============================================================================

class ScalingExecutor:

    def __init__(self, client: FlinkClient, config: Config):
        self.client = client
        self.config = config
        self.logger = logging.getLogger("DS2.Executor")

    def execute(self, job_id: str,
                approved: Dict[str, ScalingDecision],
                all_decisions: Dict[str, ScalingDecision]) -> bool:
        if not approved:
            return False

        # ============================================================
        # P4 修复：空操作拦截 — 过滤掉 target == current 的决策
        # ============================================================
        actual_changes = {
            vid: d for vid, d in approved.items()
            if d.target_parallelism != d.current_parallelism
        }
        if not actual_changes:
            self.logger.info(
                "⏸️ 所有 approved 决策的目标并行度等于当前值，"
                "跳过无意义重启 (空操作拦截)"
            )
            return False

        approved = actual_changes
        # ============================================================

        self.logger.info("=" * 60)
        self.logger.info("EXECUTING SCALING")
        self.logger.info("=" * 60)
        for vid, d in approved.items():
            self.logger.info(
                f"  {d.vertex_name}: {d.current_parallelism} -> {d.target_parallelism}"
            )

        jar_id = self.client.find_jar_id()
        if not jar_id:
            self.logger.error("JAR not found, abort")
            return False
        self.logger.info(f"JAR id: {jar_id}")

        program_args = self._build_program_args(all_decisions, approved)

        all_targets = []
        for vid, d in all_decisions.items():
            if vid in approved:
                all_targets.append(approved[vid].target_parallelism)
            else:
                all_targets.append(d.current_parallelism)
        global_max_p = max(all_targets) if all_targets else 4

        savepoint_path = self.client.trigger_savepoint(job_id)
        if not savepoint_path:
            self.logger.error("Savepoint failed, abort")
            return False

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

        if new_job_id:
            self.logger.info(f"[OK] Scaling complete! New Job: {new_job_id}")
            return True
        else:
            self.logger.error("[FAIL] Job resubmission failed")
            return False

    def _build_program_args(self, all_decisions: Dict[str, ScalingDecision],
                            approved: Dict[str, ScalingDecision]) -> str:
        arg_values: Dict[str, int] = {}

        for vid, d in all_decisions.items():
            if not d.arg_name:
                continue
            p = approved[vid].target_parallelism if vid in approved else d.current_parallelism
            if d.arg_name in arg_values:
                arg_values[d.arg_name] = max(arg_values[d.arg_name], p)
            else:
                arg_values[d.arg_name] = p

        for key, op_cfg in self.config.operator_mapping.items():
            if op_cfg.arg_name not in arg_values:
                arg_values[op_cfg.arg_name] = op_cfg.default_parallelism

        parts = [f"--{name} {value}" for name, value in sorted(arg_values.items())]
        result = " ".join(parts)
        self.logger.info(f"programArgs: {result}")
        return result


# ============================================================================
# 8. 决策日志记录器（CSV）
# ============================================================================

class DecisionLogger:

    def __init__(self, log_dir: str = "ds2_logs"):
        self.logger = logging.getLogger("DS2.CsvLogger")
        os.makedirs(log_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.csv_path = os.path.join(log_dir, f"ds2_decisions_{ts}.csv")
        self._header_written = False
        self.logger.info(f"CSV log: {self.csv_path}")

    def log_round(self, round_num: int, topology: JobTopology,
                  decisions: Dict[str, ScalingDecision],
                  approved: Dict[str, ScalingDecision],
                  executed: bool):
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
                "target_parallelism": d.target_parallelism if d else m.parallelism,
                "action": d.action if d else "UNKNOWN",
                "approved": vid in approved,
                "executed": executed,
                "in_rate": round(m.records_in_per_second, 2),
                "out_rate": round(m.records_out_per_second, 2),
                "busy_ratio": round(m.busy_ratio, 4),
                "true_processing_rate": round(m.true_processing_rate, 2),
                "selectivity": round(m.selectivity, 4),
                "reason": d.reason if d else "",
            })

        if not rows:
            return

        try:
            need_header = not self._header_written
            with open(self.csv_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                if need_header:
                    writer.writeheader()
                    self._header_written = True
                writer.writerows(rows)
        except Exception as e:
            self.logger.warning(f"CSV write failed: {e}")


# ============================================================================
# 9. 主控制器（P4 修复版）
# ============================================================================

class DS2Controller:
    """
    主控制循环。P4 新增：
      - 初始预热期（防止过早首次伸缩）
      - 重启恢复检测（跳过 busy=1.0 + 吞吐≈0 的过渡期轮次）
      - 显著性检查（仅 idle 算子变化时不触发昂贵重启）
    """

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("DS2.Controller")

        self.client = FlinkClient(config)
        self.collector = MetricsCollector(self.client, config)
        self.model = DS2Model(config)
        self.decision_filter = DecisionFilter(config)
        self.executor = ScalingExecutor(self.client, config)
        self.csv_logger = DecisionLogger()

        self._running = True
        self._round = 0

        # ============================================================
        # P4 修复：设初始预热时间，防止启动后立刻伸缩
        # ============================================================
        self._last_scaling_time: float = time.time()
        self.logger.info(
            f"初始预热 {config.warm_up_time}s, "
            f"首次决策将在 {config.warm_up_time}s 后"
        )

    def is_warming_up(self) -> bool:
        if self._last_scaling_time <= 0:
            return False
        elapsed = time.time() - self._last_scaling_time
        if elapsed < self.config.warm_up_time:
            self.logger.info(
                f"Warm-up: {elapsed:.0f}s / {self.config.warm_up_time}s"
            )
            return True
        return False

    # ----------------------------------------------------------------
    # P4 新增：重启恢复状态检测
    # ----------------------------------------------------------------
    def _is_recovering(self, topology: JobTopology) -> bool:
        """
        检测作业是否处于重启恢复过渡期。
        特征：多个算子 busy ≈ 1.0 但吞吐接近 0。
        """
        recovery_count = 0
        valid_count = 0

        for vid, m in topology.vertices.items():
            if not m.has_valid_metrics:
                continue
            valid_count += 1

            if m.busy_ratio >= self.config.recovery_busy_threshold:
                total_throughput = m.records_in_per_second + m.records_out_per_second
                if total_throughput < self.config.recovery_throughput_min:
                    recovery_count += 1
                    self.logger.warning(
                        f"  恢复期信号: {m.vertex_name} "
                        f"busy={m.busy_ratio:.3f} "
                        f"in={m.records_in_per_second:.0f}/s "
                        f"out={m.records_out_per_second:.0f}/s"
                    )

        # 超过一半的有效算子处于恢复状态
        if valid_count > 0 and recovery_count >= max(valid_count // 2, 2):
            self.logger.warning(
                f"  ⏸️ 判定为重启恢复期: {recovery_count}/{valid_count} "
                f"算子 busy≥{self.config.recovery_busy_threshold} "
                f"且吞吐<{self.config.recovery_throughput_min}/s"
            )
            return True

        return False

    # ----------------------------------------------------------------
    # P4 新增：显著性检查
    # ----------------------------------------------------------------
    def _has_significant_change(self, approved: Dict[str, ScalingDecision],
                                topology: JobTopology) -> bool:
        """
        检查 approved 决策中是否有 '显著' 变化：
        至少一个被变更的算子 busy >= min_significant_busy。
        如果全部都是 idle 算子的小调整，则不值得重启。
        """
        for vid, decision in approved.items():
            m = topology.vertices.get(vid)
            if m and m.busy_ratio >= self.config.min_significant_busy:
                return True

            # 变化幅度 >= 2 也视为显著
            if abs(decision.target_parallelism - decision.current_parallelism) >= 2:
                return True

        return False

    def run(self):
        self.logger.info("=" * 60)
        self.logger.info("DS2 Controller Starting (P4 稳定性修复版)")
        self.logger.info(f"  Flink REST     : {self.config.flink_rest_url}")
        self.logger.info(f"  Job Name       : {self.config.flink_job_name}")
        self.logger.info(f"  Interval       : {self.config.policy_interval}s")
        self.logger.info(f"  Warm-up        : {self.config.warm_up_time}s")
        self.logger.info(f"  Act. Window    : {self.config.activation_window}")
        self.logger.info(f"  Rate Ratio     : {self.config.target_rate_ratio}")
        self.logger.info(f"  Parallelism    : [{self.config.min_parallelism}, {self.config.max_parallelism}]")
        self.logger.info(f"  Max Scale      : {self.config.max_scale_factor}x")
        self.logger.info(f"  Recovery Thrsh : busy>={self.config.recovery_busy_threshold}, "
                         f"thru<{self.config.recovery_throughput_min}")
        self.logger.info(f"  Significance   : busy>={self.config.min_significant_busy}")
        self.logger.info(f"  Grace Period   : {self.config.initial_grace_period}s")
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
                self.logger.error(f"Round error: {e}", exc_info=True)

            for _ in range(self.config.policy_interval):
                if not self._running:
                    break
                time.sleep(1)

        self.logger.info("DS2 Controller stopped")

    def _run_one_round(self):
        self._round += 1
        self.logger.info(f"\n{'#' * 60}")
        self.logger.info(f"# Round {self._round}")
        self.logger.info(f"{'#' * 60}")

        # 1. 查找作业
        job_id = self.client.get_running_job_id()
        if not job_id:
            self.logger.info("No running job, waiting ...")
            return

        # 2. 预热检查
        if self.is_warming_up():
            self.logger.info("Warming up, skip")
            return

        # 3. 获取拓扑
        topology = self.client.get_job_topology(job_id)
        if not topology:
            self.logger.warning("Failed to get topology")
            return

        self.logger.info(f"Job: {topology.job_name} ({job_id})")
        self.logger.info(f"Vertices: {len(topology.vertices)}, Edges: {len(topology.edges)}")
        for vid, m in topology.vertices.items():
            self.logger.info(f"  {m.vertex_name} (P={m.parallelism}, arg={m.arg_name})")

        # 4. 采集指标
        has_data = self.collector.collect(topology)
        if not has_data:
            self.logger.info("Need at least 2 samples for diff, skip")
            return

        # ============================================================
        # P4 修复：重启恢复检测
        # ============================================================
        if self._is_recovering(topology):
            self.logger.info("  跳过本轮决策 (重启恢复过渡期)")
            # 重置采集器，让恢复期数据不污染下一轮差分
            self.collector.reset()
            return
        # ============================================================

        # 5. 计算决策
        all_decisions = self.model.compute(topology)

        # 6. 过滤
        approved = self.decision_filter.filter(all_decisions, topology)

        # ============================================================
        # P4 修复：显著性检查
        # ============================================================
        if approved and not self._has_significant_change(approved, topology):
            self.logger.info(
                "  ⏸️ 仅 idle/低负载算子需变更 (busy < %.2f), "
                "延迟执行以避免无效重启",
                self.config.min_significant_busy
            )
            approved = {}
        # ============================================================

        # 7. 执行
        executed = False
        if approved:
            self.logger.info(f"{len(approved)} change(s) approved, executing ...")
            executed = self.executor.execute(job_id, approved, all_decisions)
            if executed:
                self._last_scaling_time = time.time()
                self.collector.reset()
                self.decision_filter.reset()
                self.logger.info(
                    f"Scaling done, warm-up {self.config.warm_up_time}s starts"
                )
        else:
            self.logger.info("No approved changes")

        # 8. 日志
        self.csv_logger.log_round(
            self._round, topology, all_decisions, approved, executed
        )

    def _handle_signal(self, signum, frame):
        self.logger.info(f"Signal {signum} received, stopping ...")
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
        f"ds2_logs/ds2_{ts}.log", encoding="utf-8"
    )
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

    logger.info(f"Job: {topology.job_name} ({job_id})")
    logger.info(f"Vertices ({len(topology.vertices)}):")
    for vid, m in topology.vertices.items():
        logger.info(f"  {m.vertex_name}  P={m.parallelism}  arg={m.arg_name}")

    logger.info(f"Edges ({len(topology.edges)}):")
    for src, dst in topology.edges:
        sn = topology.vertices.get(src, None)
        dn = topology.vertices.get(dst, None)
        logger.info(
            f"  {sn.vertex_name if sn else src} -> {dn.vertex_name if dn else dst}"
        )

    logger.info("\nTopological order:")
    for i, vid in enumerate(topology.topological_sort()):
        logger.info(f"  {i + 1}. {topology.vertices[vid].vertex_name}")

    collector = MetricsCollector(client, config)
    logger.info("\n--- Sample 1 ---")
    collector.collect(topology)

    logger.info(f"Waiting {config.policy_interval}s for sample 2 ...")
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

    logger.info("\n" + "=" * 80)
    logger.info(
        f"{'Operator':<25} {'In/s':>8} {'Out/s':>8} "
        f"{'Busy':>6} {'TrueRate':>10} {'Sel':>6} {'P':>3}"
    )
    logger.info("-" * 80)
    for vid in topology.topological_sort():
        m = topology.vertices[vid]
        if m.has_valid_metrics:
            logger.info(
                f"{m.vertex_name:<25} "
                f"{m.records_in_per_second:>8.1f} "
                f"{m.records_out_per_second:>8.1f} "
                f"{m.busy_ratio:>6.3f} "
                f"{m.true_processing_rate:>10.1f} "
                f"{m.selectivity:>6.3f} "
                f"{m.parallelism:>3}"
            )
        else:
            logger.info(f"{m.vertex_name:<25} {'(no data)':>8}")
    logger.info("=" * 80)

    model = DS2Model(config)
    decisions = model.compute(topology)

    executor = ScalingExecutor(client, config)
    args_preview = executor._build_program_args(decisions, decisions)
    logger.info(f"\nprogramArgs preview: {args_preview}")

    logger.info("\nDry-run complete. No changes applied.")


def main():
    parser = argparse.ArgumentParser(description="DS2 Auto-Scaling Controller")
    parser.add_argument(
        "-c", "--config",
        default="ds2_config.yaml",
        help="Config file path (default: ds2_config.yaml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute decisions only, do not execute scaling",
    )
    args = parser.parse_args()

    config = Config.from_yaml(args.config)
    config.validate()

    if not config.operator_mapping:
        print("[WARN] No operator_mapping in config, using built-in defaults")
        config.operator_mapping = {
            "source": OperatorConfig("RocketMQ-Source", "p-source", 2),
            "filter": OperatorConfig("Hardware-Filter", "p-filter", 4),
            "signal": OperatorConfig("Signal-Extraction", "p-signal", 4),
            "feature": OperatorConfig("Feature-Extraction", "p-feature", 4),
            "grid": OperatorConfig("Grid-Aggregation", "p-grid", 4),
            "sink_event": OperatorConfig("EventFeature-Sink", "p-sink", 2),
            "sink_grid": OperatorConfig("GridSummary-Sink", "p-sink", 2),
        }

    setup_logging(config.log_level)

    logger = logging.getLogger("DS2.Main")
    logger.info("DS2 Configuration:")
    logger.info(f"  flink_rest_url     = {config.flink_rest_url}")
    logger.info(f"  flink_job_name     = {config.flink_job_name}")
    logger.info(f"  policy_interval    = {config.policy_interval}s")
    logger.info(f"  warm_up_time       = {config.warm_up_time}s")
    logger.info(f"  activation_window  = {config.activation_window}")
    logger.info(f"  target_rate_ratio  = {config.target_rate_ratio}")
    logger.info(f"  parallelism range  = [{config.min_parallelism}, {config.max_parallelism}]")
    logger.info(f"  max_scale_factor   = {config.max_scale_factor}x")
    logger.info(f"  target_jar_name    = {config.target_jar_name}")
    logger.info(f"  recovery_threshold = busy>={config.recovery_busy_threshold}, "
                f"thru<{config.recovery_throughput_min}")
    logger.info(f"  significance       = busy>={config.min_significant_busy}")
    logger.info(f"  operator_mapping   = {len(config.operator_mapping)} operators:")
    for key, op in config.operator_mapping.items():
        logger.info(
            f"    {key:<12} keyword='{op.keyword}'  arg='{op.arg_name}'  "
            f"default_p={op.default_parallelism}"
        )

    if args.dry_run:
        logger.info("MODE: dry-run (no scaling will be executed)")
        run_dry(config)
    else:
        logger.info("MODE: live")
        controller = DS2Controller(config)
        controller.run()


if __name__ == "__main__":
    main()
