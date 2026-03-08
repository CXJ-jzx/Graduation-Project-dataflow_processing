#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DS2 自动扩缩容控制器 (完整修正版)
================================

基于 OSDI '18 论文 "Three Steps is All You Need" 实现
适配 Apache Flink 1.17 Standalone 集群

【核心修复说明】
1. 指标采集策略变更：
   - 弃用 Flink 的 `*PerSecond` 瞬时指标（在低负载或特定版本下可能为0）。
   - 改用 `numRecordsIn`、`numRecordsOut`、`accumulateBusyTimeMs` 等累积总量指标。
   - 在控制器端计算 (Current - Last) / TimeDelta，得出精准的速率和繁忙度。

2. API 解析逻辑增强：
   - 兼容 Flink REST API 的两种返回格式（Subtask列表模式 vs 聚合统计模式）。
   - 确保在并行度为 1 时也能正确读取到 sum 值。

作者: DS2 Implementation
"""

import os
import sys
import time
import math
import signal
import logging
import argparse
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from collections import defaultdict, deque
from datetime import datetime

import requests

# YAML 支持（可选）
try:
    import yaml

    HAS_YAML = True
except ImportError:
    HAS_YAML = False


# ============================================================================
# 1. 配置管理 (Config)
# ============================================================================

@dataclass
class Config:
    """DS2 配置类"""

    # Flink 集群配置
    flink_rest_url: str = "http://localhost:8081"
    flink_job_name: str = "Seismic-Cache-Optimized-Job"
    flink_savepoint_dir: str = "/tmp/flink-savepoints"

    # DS2 核心参数（论文建议值）
    policy_interval: int = 15  # 决策间隔（秒）
    warm_up_time: int = 60  # 重启后的预热静默期（秒）
    activation_window: int = 3  # 激活窗口（连续 N 次决策一致才执行）
    target_rate_ratio: float = 1.1  # 目标速率冗余系数 (Safety Margin)
    min_busy_ratio: float = 0.05  # 最小忙碌比 (防止除零)
    max_parallelism: int = 16  # 最大并发限制
    min_parallelism: int = 1  # 最小并发限制
    change_threshold: int = 0  # 忽略微小的并发度变化

    target_jar_name: str = "flink-rocketmq-demo-1.0-SNAPSHOT"

    # 日志
    log_level: str = "INFO"

    @classmethod
    def from_yaml(cls, path: str) -> 'Config':
        """从 YAML 文件加载配置"""
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

            # Flink 配置
            if 'flink' in data:
                flink = data['flink']
                config.flink_rest_url = flink.get('rest_url', config.flink_rest_url)
                config.flink_job_name = flink.get('job_name', config.flink_job_name)
                config.flink_savepoint_dir = flink.get('savepoint_dir', config.flink_savepoint_dir)

            # DS2 参数
            if 'ds2' in data:
                ds2 = data['ds2']
                config.policy_interval = ds2.get('policy_interval', config.policy_interval)
                config.warm_up_time = ds2.get('warm_up_time', config.warm_up_time)
                config.activation_window = ds2.get('activation_window', config.activation_window)
                config.target_rate_ratio = ds2.get('target_rate_ratio', config.target_rate_ratio)
                config.min_busy_ratio = ds2.get('min_busy_ratio', config.min_busy_ratio)
                config.max_parallelism = ds2.get('max_parallelism', config.max_parallelism)
                config.min_parallelism = ds2.get('min_parallelism', config.min_parallelism)
                config.change_threshold = ds2.get('change_threshold', config.change_threshold)
                config.target_jar_name = ds2.get('target_jar_name', config.target_jar_name)

            # 日志
            if 'logging' in data:
                config.log_level = data['logging'].get('level', config.log_level)

            print(f"[INFO] Config loaded from {path}")

        except Exception as e:
            print(f"[WARN] Failed to load config: {e}, using defaults")

        return config

    def validate(self) -> bool:
        """验证配置有效性"""
        assert self.policy_interval >= 5, "policy_interval must be >= 5"
        assert self.warm_up_time >= 0, "warm_up_time must be >= 0"
        assert self.activation_window >= 1, "activation_window must be >= 1"
        assert 1.0 <= self.target_rate_ratio <= 2.0, "target_rate_ratio must be in [1.0, 2.0]"
        assert self.max_parallelism >= self.min_parallelism, "max >= min parallelism"
        return True


# ============================================================================
# 2. 数据结构 (Data Structures)
# ============================================================================

@dataclass
class OperatorMetrics:
    """算子指标容器"""
    vertex_id: str
    vertex_name: str
    parallelism: int

    # ----------------------------------------------------
    # 计算后的实时指标 (Rates)
    # ----------------------------------------------------
    records_in_per_second: float = 0.0
    records_out_per_second: float = 0.0

    # 忙碌比率 (0.0 - 1.0)，表示该算子有多忙
    busy_ratio: float = 0.0

    # ----------------------------------------------------
    # DS2 模型推导指标
    # ----------------------------------------------------
    # 真实处理能力 (True Processing Rate): 假设 busy_ratio = 1.0 时的处理速率
    true_processing_rate: float = 0.0

    # 真实输出能力
    true_output_rate: float = 0.0

    # 选择率 (Selectivity): output / input
    selectivity: float = 1.0

    # 用于检测倾斜 (暂留接口)
    per_subtask_busy: List[float] = field(default_factory=list)


@dataclass
class ScalingDecision:
    """单个算子的扩缩容决策"""
    vertex_id: str
    vertex_name: str
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
    """作业拓扑图"""
    job_id: str
    job_name: str
    vertices: Dict[str, OperatorMetrics] = field(default_factory=dict)
    edges: List[Tuple[str, str]] = field(default_factory=list)  # (source_id, target_id)

    def get_upstream(self, vertex_id: str) -> List[str]:
        """获取直接上游算子 ID"""
        return [e[0] for e in self.edges if e[1] == vertex_id]

    def get_downstream(self, vertex_id: str) -> List[str]:
        """获取直接下游算子 ID"""
        return [e[1] for e in self.edges if e[0] == vertex_id]

    def topological_sort(self) -> List[str]:
        """
        拓扑排序
        DS2 算法要求按照数据流向（Source -> Sink）依次计算，
        因为下游的目标输入依赖于上游的目标输出。
        """
        in_degree = defaultdict(int)
        for _, target in self.edges:
            in_degree[target] += 1

        # 入度为 0 的节点（Source）先入队
        queue = [v for v in self.vertices if in_degree[v] == 0]
        result = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            for downstream in self.get_downstream(node):
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    queue.append(downstream)

        # 如果有环（理论上 Flink DAG 无环），返回默认顺序
        if len(result) != len(self.vertices):
            return list(self.vertices.keys())

        return result


# ============================================================================
# 3. Flink REST API 客户端 (FlinkClient)
# ============================================================================

class FlinkClient:
    """
    负责与 Flink Cluster 交互
    """

    # 🔧 关键修改：请求累积总量指标，而非速率指标
    # numRecordsIn: 累积接收记录数
    # numRecordsOut: 累积发送记录数
    # accumulateBusyTimeMs: 累积忙碌时间 (Flink 1.17 标准)
    REQUIRED_METRICS = [
        "numRecordsIn",
        "numRecordsOut",
        "accumulateBusyTimeMs"
    ]

    def __init__(self, config: Config):
        self.config = config
        self.base_url = config.flink_rest_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.logger = logging.getLogger("DS2.FlinkClient")

    def _get(self, endpoint: str, timeout: int = 10) -> Optional[Dict]:
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self.session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            else:
                self.logger.debug(f"GET {endpoint} returned {resp.status_code}")
                return None
        except Exception as e:
            self.logger.debug(f"GET {endpoint} failed: {e}")
            return None

    def _post(self, endpoint: str, data: Dict = None, timeout: int = 30) -> Tuple[bool, Optional[Dict]]:
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self.session.post(url, json=data or {}, timeout=timeout)
            return resp.status_code < 400, resp.json() if resp.text else {}
        except Exception as e:
            self.logger.error(f"POST {endpoint} failed: {e}")
            return False, None

    def _patch(self, endpoint: str, timeout: int = 30) -> bool:
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self.session.patch(url, timeout=timeout)
            return resp.status_code < 400
        except Exception as e:
            self.logger.error(f"PATCH {endpoint} failed: {e}")
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
            if (job.get('name') == self.config.flink_job_name and
                    job.get('state') == 'RUNNING'):
                return job['jid']
        return None

    def get_job_topology(self, job_id: str) -> Optional[JobTopology]:
        data = self._get(f"/jobs/{job_id}")
        if not data:
            return None

        topology = JobTopology(
            job_id=job_id,
            job_name=data.get('name', '')
        )

        # 解析 Vertex (算子)
        for vertex in data.get('vertices', []):
            vid = vertex['id']
            topology.vertices[vid] = OperatorMetrics(
                vertex_id=vid,
                vertex_name=vertex['name'],
                parallelism=vertex['parallelism']
            )

        # 解析 Edges (边)
        plan = data.get('plan', {})
        for node in plan.get('nodes', []):
            node_id = node['id']
            for inp in node.get('inputs', []):
                source_id = inp['id']
                topology.edges.append((source_id, node_id))

        return topology

    def get_vertex_metrics(self, job_id: str, vertex_id: str) -> Dict[str, List[float]]:
        """
        获取算子的指标数据
        🔧 关键修改：适配 Flink REST API 的两种返回格式（List vs Aggregated）
        """
        endpoint = f"/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics"
        params = f"?get={','.join(self.REQUIRED_METRICS)}"

        data = self._get(endpoint + params)
        if not data:
            return {}

        result = {m: [] for m in self.REQUIRED_METRICS}

        # 格式 A: 并行度 > 1 时，通常返回 Subtask 列表
        # [{"subtask": 0, "metrics": [{"id": "...", "value": "..."}]}, ...]
        if isinstance(data, list) and len(data) > 0 and 'metrics' in data[0]:
            for subtask in data:
                metrics_map = {}
                for m in subtask.get('metrics', []):
                    try:
                        metrics_map[m['id']] = float(m.get('value', 0))
                    except (ValueError, TypeError):
                        pass

                for metric in self.REQUIRED_METRICS:
                    result[metric].append(metrics_map.get(metric, 0.0))

        # 格式 B: 并行度 = 1 或 API 聚合模式，返回聚合统计值
        # [{"id": "numRecordsIn", "min":..., "max":..., "sum":..., "avg":...}, ...]
        elif isinstance(data, list):
            metrics_map = {}
            for m in data:
                metric_id = m.get('id')
                if metric_id in self.REQUIRED_METRICS:
                    try:
                        # 优先读取 'sum' (总和)，如果没有则尝试读取 'value'
                        if 'sum' in m:
                            metrics_map[metric_id] = float(m['sum'])
                        else:
                            metrics_map[metric_id] = float(m.get('value', 0))
                    except (ValueError, TypeError):
                        metrics_map[metric_id] = 0.0

            # 这种情况下，我们将所有 Subtask 视为一个整体 (列表长度为1)
            for metric in self.REQUIRED_METRICS:
                result[metric].append(metrics_map.get(metric, 0.0))

        return result
    '''
    def trigger_savepoint(self, job_id: str) -> Optional[str]:
        self.logger.info("Triggering savepoint...")
        success, resp = self._post(
            f"/jobs/{job_id}/savepoints",
            {"target-directory": self.config.flink_savepoint_dir, "cancel-job": False}
        )
        if not success or not resp:
            return None

        trigger_id = resp.get("request-id")
        if not trigger_id: return None

        # 等待 Savepoint 完成
        for _ in range(60):
            time.sleep(1)
            status = self._get(f"/jobs/{job_id}/savepoints/{trigger_id}")
            if not status: continue

            state = status.get("status", {}).get("id")
            if state == "COMPLETED":
                location = status.get("operation", {}).get("location")
                self.logger.info(f"Savepoint completed: {location}")
                return location
            elif state == "FAILED":
                return None

        return None
    '''


    def trigger_savepoint(self, job_id: str) -> Optional[str]:
        self.logger.info("Triggering savepoint...")
        # 打印一下我们要往哪里写，确认配置读取没问题
        self.logger.info(f"Target Dir: {self.config.flink_savepoint_dir}")

        success, resp = self._post(
            f"/jobs/{job_id}/savepoints",
            {"target-directory": self.config.flink_savepoint_dir, "cancel-job": False}
        )
        if not success or not resp:
            self.logger.error(f"Trigger request failed: {resp}")
            return None

        trigger_id = resp.get("request-id")
        if not trigger_id: return None

        # 等待 Savepoint 完成
        for _ in range(60):
            time.sleep(1)
            status = self._get(f"/jobs/{job_id}/savepoints/{trigger_id}")
            if not status: continue

            # --- [新增] 打印原始响应，用于调试 ---
            self.logger.debug(f"Savepoint Status Response: {status}")
            # -----------------------------------

            state = status.get("status", {}).get("id")

            if state == "COMPLETED":
                location = status.get("operation", {}).get("location")
                if location is None:
                    # 如果状态完成但没路径，这是异常情况，打印警告
                    self.logger.warning(f"Savepoint reports COMPLETED but location is None! Full response: {status}")
                else:
                    self.logger.info(f"Savepoint completed: {location}")
                return location

            elif state == "FAILED":
                # 打印失败原因
                cause = status.get("operation", {}).get("failure-cause", "Unknown reason")
                self.logger.error(f"Savepoint FAILED: {cause}")
                return None

        return None

    def cancel_job(self, job_id: str) -> bool:
        self.logger.info(f"Cancelling job {job_id}...")
        return self._patch(f"/jobs/{job_id}?mode=cancel")

    # [新增方法] 查找集群中已上传的 JAR ID
    def get_jar_id(self, name_keyword: str) -> Optional[str]:
        data = self._get("/jars")
        if not data:
            return None

        # 按上传时间倒序找最新的
        files = data.get('files', [])
        files.sort(key=lambda x: x['uploaded'], reverse=True)

        for jar in files:
            if name_keyword in jar.get('name', ''):
                return jar['id']
        return None

    # [新增方法] 提交作业运行
    def run_job(self, jar_id: str, parallelism: int, savepoint_path: str, program_args: str = "") -> bool:
        endpoint = f"/jars/{jar_id}/run"
        payload = {
            "parallelism": parallelism,
            "savepointPath": savepoint_path,
            "allowNonRestoredState": False,
            "programArgs": program_args
        }
        self.logger.info(f"Submitting job with p={parallelism}, sp={savepoint_path}")
        success, resp = self._post(endpoint, payload)
        if success:
            job_id = resp.get("jobid")
            self.logger.info(f"Job submitted successfully! New Job ID: {job_id}")
            return True
        else:
            self.logger.error(f"Failed to submit job: {resp}")
            return False

    # [辅助方法] 等待作业完全停止
    def wait_for_job_cancel(self, job_id: str, timeout=30) -> bool:
        for _ in range(timeout):
            status = self._get(f"/jobs/{job_id}")
            # 如果查不到，或者状态是 CANCELED/FAILED/FINISHED，说明停了
            if not status or status.get('state') in ['CANCELED', 'FAILED', 'FINISHED']:
                return True
            time.sleep(1)
        return False

# ============================================================================
# 4. 指标采集与计算 (MetricsCollector)
# ============================================================================

class MetricsCollector:
    """
    负责采集原始指标，并计算 DS2 所需的速率和繁忙度。

    【核心逻辑】
    使用 "差分法"：记录上一次采集的累积值和时间戳。
    Rate = (Current_Total - Last_Total) / (Current_Time - Last_Time)
    """

    def __init__(self, config: Config, flink_client: FlinkClient):
        self.config = config
        self.client = flink_client
        self.logger = logging.getLogger("DS2.Collector")

        # 缓存: {vertex_id: (timestamp, total_in, total_out, total_busy_ms)}
        self.last_metrics = {}

    def collect_all(self, topology: JobTopology) -> JobTopology:
        current_time = time.time()

        for vid, vertex in topology.vertices.items():
            # 1. 调用 Client 获取原始数据 (列表形式)
            raw = self.client.get_vertex_metrics(topology.job_id, vid)
            if not raw:
                continue

            # 2. 聚合当前时刻的总量
            curr_total_in = sum(raw.get('numRecordsIn', [0]))
            curr_total_out = sum(raw.get('numRecordsOut', [0]))

            # 兼容 accumulateBusyTimeMs (推荐) 和 busyTimeMs (旧版)
            busy_key = 'accumulateBusyTimeMs' if 'accumulateBusyTimeMs' in raw else 'busyTimeMs'
            curr_total_busy = sum(raw.get(busy_key, [0]))

            # 获取 subtask 数量，用于计算平均 busy
            subtask_count = max(1, len(raw.get(busy_key, [1])))
            curr_avg_busy = curr_total_busy / subtask_count

            # 3. 计算差分速率
            in_rate = 0.0
            out_rate = 0.0
            busy_ratio = self.config.min_busy_ratio

            if vid in self.last_metrics:
                last_ts, last_in, last_out, last_busy = self.last_metrics[vid]
                duration = current_time - last_ts

                if duration > 0:
                    # 计算 TPS (Events/sec)
                    in_rate = (curr_total_in - last_in) / duration
                    out_rate = (curr_total_out - last_out) / duration

                    # 计算繁忙度 (0.0 - 1.0)
                    # 逻辑：在 duration 秒内，平均每个 subtask 忙了 delta_busy 毫秒
                    busy_delta = curr_avg_busy - last_busy

                    # 归一化: ms / (s * 1000)
                    calculated_busy = busy_delta / (duration * 1000.0)

                    # 钳位，防止指标抖动导致异常值
                    busy_ratio = max(0.0, min(1.0, calculated_busy))

                    # 应用最小繁忙度保底 (防止除零)
                    busy_ratio = max(busy_ratio, self.config.min_busy_ratio)

            # 4. 更新缓存
            self.last_metrics[vid] = (current_time, curr_total_in, curr_total_out, curr_avg_busy)

            # 5. 如果是第一次采集（没有历史数据），无法计算速率，跳过此次 DS2 计算
            # 但为了日志显示正常，先把基础值设为 0
            if in_rate == 0 and out_rate == 0 and busy_ratio == self.config.min_busy_ratio:
                vertex.records_in_per_second = 0
                vertex.records_out_per_second = 0
                vertex.busy_ratio = self.config.min_busy_ratio
                continue

            # 6. ========== DS2 模型核心指标推导 ==========

            # 真实处理能力 = 当前吞吐 / 忙碌度
            # "如果算子 100% 忙碌，它能处理多少数据？"
            true_processing_rate = in_rate / busy_ratio
            true_output_rate = out_rate / busy_ratio

            # 选择率 (过滤率/膨胀率)
            selectivity = out_rate / in_rate if in_rate > 0 else 1.0

            # 7. 回填到 OperatorMetrics 对象
            vertex.records_in_per_second = in_rate
            vertex.records_out_per_second = out_rate
            vertex.busy_time_ms_per_second = busy_ratio * 1000.0
            vertex.busy_ratio = busy_ratio
            vertex.true_processing_rate = true_processing_rate
            vertex.true_output_rate = true_output_rate
            vertex.selectivity = selectivity

            # 记录用于检测倾斜的数据 (此处简化为平均值)
            vertex.per_subtask_busy = [busy_ratio * 1000.0] * subtask_count

        return topology

    def detect_skew(self, vertex: OperatorMetrics, threshold: float = 0.3) -> bool:
        """数据倾斜检测"""
        if len(vertex.per_subtask_busy) < 2:
            return False
        avg = sum(vertex.per_subtask_busy) / len(vertex.per_subtask_busy)
        if avg == 0:
            return False
        max_deviation = max(abs(b - avg) / avg for b in vertex.per_subtask_busy)
        return max_deviation > threshold


# ============================================================================
# 5. DS2 算法模型 (Model)
# ============================================================================

class DS2Model:
    """
    DS2 扩缩容决策核心逻辑
    """

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("DS2.Model")
        # 决策历史窗口，用于平滑抖动
        self.decision_history: deque = deque(maxlen=config.activation_window)

    def compute_plan(self, topology: JobTopology) -> Dict[str, ScalingDecision]:
        """
        计算每个算子的目标并发度
        """
        decisions = {}
        # 记录传递给下游的目标流量
        target_output_map: Dict[str, float] = {}

        # 必须按拓扑序遍历 (Source -> Sink)
        sorted_vertices = topology.topological_sort()

        for vid in sorted_vertices:
            metrics = topology.vertices[vid]
            current_p = metrics.parallelism
            upstream = topology.get_upstream(vid)

            # --- CASE 1: Source 算子 ---
            if not upstream:
                # Source 算子通常由外部系统（如 Kafka Lag）决定，DS2 简化为保持现状
                # 或者如果有 Backpressure，也可以尝试扩容
                decisions[vid] = ScalingDecision(
                    vid, metrics.vertex_name, current_p, current_p, "Source operator"
                )
                target_output_map[vid] = metrics.records_out_per_second
                continue

            # --- CASE 2: 中间/Sink 算子 ---

            # Step 1: 计算目标输入速率 = 所有上游目标输出之和
            target_input = sum(
                target_output_map.get(u, topology.vertices[u].records_out_per_second)
                for u in upstream
            )

            # Step 2: 计算单实例真实处理能力
            true_rate_per_inst = metrics.true_processing_rate / current_p if current_p > 0 else 1.0

            # Step 3: 计算目标并发度
            if true_rate_per_inst > 0:
                # 公式: (目标流量 / 单机能力) * 安全系数
                raw_parallelism = (target_input / true_rate_per_inst) * self.config.target_rate_ratio
                target_p = math.ceil(raw_parallelism)
            else:
                target_p = current_p

            # Step 4: 应用边界约束
            target_p = max(self.config.min_parallelism, target_p)
            target_p = min(self.config.max_parallelism, target_p)

            # Step 5: 忽略微小抖动
            if abs(target_p - current_p) <= self.config.change_threshold:
                target_p = current_p

            # 生成决策对象
            reason = self._make_reason(metrics, target_input, true_rate_per_inst, target_p)
            decisions[vid] = ScalingDecision(
                vertex_id=vid,
                vertex_name=metrics.vertex_name,
                current_parallelism=current_p,
                target_parallelism=target_p,
                reason=reason
            )

            # 计算传递给下游的流量 = 输入 * 选择率
            target_output_map[vid] = target_input * metrics.selectivity

            self.logger.debug(
                f"{metrics.vertex_name}: busy={metrics.busy_ratio:.1%}, "
                f"target_in={target_input:.0f}, cap_per_node={true_rate_per_inst:.0f}, "
                f"p: {current_p} -> {target_p}"
            )

        return decisions

    def _make_reason(self, m: OperatorMetrics, target_in: float, cap: float, target_p: int) -> str:
        if target_p > m.parallelism:
            return f"SCALE UP: busy={m.busy_ratio:.0%}, load={target_in:.0f}/s"
        if target_p < m.parallelism:
            return f"SCALE DOWN: busy={m.busy_ratio:.0%}"
        return "OPTIMAL"

    def should_apply(self, decisions: Dict[str, ScalingDecision]) -> Tuple[bool, str]:
        """Activation Window 机制: 连续 N 次决策一致才执行"""

        # 1. 如果没有变化，直接返回
        if not any(d.needs_change for d in decisions.values()):
            self.decision_history.clear()
            return False, "No scaling needed"

        # 2. 记录当前决策快照
        current_snapshot = {vid: d.target_parallelism for vid, d in decisions.items()}
        self.decision_history.append(current_snapshot)

        # 3. 检查窗口是否填满
        if len(self.decision_history) < self.config.activation_window:
            remaining = self.config.activation_window - len(self.decision_history)
            return False, f"Stabilizing... ({len(self.decision_history)}/{self.config.activation_window})"

        # 4. 检查决策一致性
        first = self.decision_history[0]
        is_stable = all(snap == first for snap in self.decision_history)

        if is_stable:
            return True, "Decision stable, APPLYING scaling"
        else:
            return False, "Decisions fluctuating, waiting..."

    def reset_history(self):
        self.decision_history.clear()


# ============================================================================
# 6. 执行器 (Executor)
# ============================================================================
class ScalingExecutor:
    """负责执行扩缩容操作 (自动化版)"""

    def __init__(self, config: Config, client: FlinkClient):
        self.config = config
        self.client = client
        self.logger = logging.getLogger("DS2.Executor")
        self.last_scale_time = 0

    def is_warming_up(self) -> bool:
        return time.time() - self.last_scale_time < self.config.warm_up_time

    def get_warmup_remaining(self) -> float:
        return max(0, self.config.warm_up_time - (time.time() - self.last_scale_time))

    def execute(self, job_id: str, decisions: Dict[str, ScalingDecision], dry_run: bool) -> bool:
        changes = [d for d in decisions.values() if d.needs_change]

        # 1. 计算新的全局并发度
        # Flink 提交时通常设置一个全局并发度，这里取所有算子决策的最大值
        new_max_parallelism = max(d.target_parallelism for d in decisions.values())

        self.logger.info("=" * 60)
        self.logger.info(f" >>> EXECUTING SCALING PLAN (Target Global P={new_max_parallelism}) <<<")
        for c in changes:
            arrow = "↑" if c.target_parallelism > c.current_parallelism else "↓"
            self.logger.info(f"  {c.vertex_name}: {c.current_parallelism} -> {c.target_parallelism} {arrow}")
        self.logger.info("=" * 60)

        if dry_run:
            self.logger.info("[Dry Run] Skipping actual execution.")
            return True

        # 2. 寻找 JAR 包 ID
        # 必须确保你的 JAR 已经上传到 Flink Web UI 的 "Submitted Jars" 中
        jar_id = self.client.get_jar_id(self.config.target_jar_name)
        if not jar_id:
            self.logger.error(
                f"Cannot find uploaded JAR matching '{self.config.target_jar_name}'. Please upload it via Flink Web UI first.")
            return False

        # 3. 触发 Savepoint
        self.logger.info("1. Triggering Savepoint...")
        savepoint_path = self.client.trigger_savepoint(job_id)
        if not savepoint_path:
            self.logger.error("Failed to trigger savepoint. Aborting.")
            return False

        # 4. 停止当前作业
        self.logger.info("2. Cancelling Job...")
        if not self.client.cancel_job(job_id):
            self.logger.error("Failed to cancel job.")
            return False

        # 等待作业彻底停止
        if not self.client.wait_for_job_cancel(job_id):
            self.logger.error("Job did not stop in time.")
            return False

        # 5. 提交新作业
        self.logger.info("3. Resubmitting Job...")

        # 如果你的 Flink 程序支持通过 args 设置具体算子并发度，可以在这里构造 program_args
        # 例如: program_args = "--p-source 2 --p-process 4"
        # 这里演示最基础的：设置全局并发度
        success = self.client.run_job(
            jar_id=jar_id,
            parallelism=new_max_parallelism,
            savepoint_path=savepoint_path
        )

        if success:
            self.last_scale_time = time.time()
            self.logger.info(">>> SCALING COMPLETED SUCCESSFULLY <<<")
            return True
        else:
            self.logger.error("Failed to resubmit job! Please check Flink Logs.")
            return False


# ============================================================================
# 7. 主控制器循环 (Main Controller)
# ============================================================================

class DS2Controller:
    def __init__(self, config_path, dry_run, verbose):
        self.config = Config.from_yaml(config_path)
        self.config.validate()
        self.dry_run = dry_run

        level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(level=level, format='%(asctime)s | %(name)-15s | %(levelname)-5s | %(message)s',
                            datefmt='%H:%M:%S')
        self.logger = logging.getLogger("DS2.Controller")

        self.client = FlinkClient(self.config)
        self.collector = MetricsCollector(self.config, self.client)
        self.model = DS2Model(self.config)
        self.executor = ScalingExecutor(self.config, self.client)

        self.running = True
        signal.signal(signal.SIGINT, lambda s, f: setattr(self, 'running', False))

    def run(self):
        self.logger.info(f"DS2 Controller Started (DryRun={self.dry_run})")

        if not self.client.check_connection():
            self.logger.error("Cannot connect to Flink Cluster.")
            return

        while self.running:
            try:
                loop_start = time.time()

                # 1. 查找作业
                job_id = self.client.get_running_job_id()

                if not job_id:
                    self.logger.warning(f"Job '{self.config.flink_job_name}' not running.")

                elif self.executor.is_warming_up():
                    rem = self.executor.get_warmup_remaining()
                    self.logger.info(f"Cooling down... {rem:.0f}s")

                else:
                    # 2. 获取拓扑
                    topo = self.client.get_job_topology(job_id)
                    if topo:
                        # 3. 采集指标 (关键步骤)
                        topo = self.collector.collect_all(topo)
                        self._print_status(topo)

                        # 4. 计算决策
                        decisions = self.model.compute_plan(topo)

                        # 5. 稳定性检查
                        should_apply, reason = self.model.should_apply(decisions)
                        self.logger.info(f"Decision: {reason}")

                        # 6. 执行扩缩容
                        if should_apply:
                            if self.executor.execute(job_id, decisions, self.dry_run):
                                self.model.reset_history()
                                # 如果非 Dry Run，作业已停止，控制器可以退出或等待重启
                                if not self.dry_run:
                                    self.logger.info("Scaling triggered. Waiting for manual restart...")
                                    # 实际生产中这里可能会调用 subprocess 启动新作业

                # 等待下一周期
                elapsed = time.time() - loop_start
                sleep_time = max(1, self.config.policy_interval - elapsed)
                time.sleep(sleep_time)

            except Exception as e:
                self.logger.error(f"Loop Error: {e}", exc_info=True)
                time.sleep(5)

        self.logger.info("Controller stopped.")

    def _print_status(self, topo: JobTopology):
        self.logger.info("-" * 60)
        self.logger.info("Current Status:")
        for vid in topo.topological_sort():
            m = topo.vertices[vid]

            status_icon = "🟢"
            if m.busy_ratio > 0.8:
                status_icon = "🔴"
            elif m.busy_ratio > 0.5:
                status_icon = "🟡"

            self.logger.info(
                f"[{m.vertex_name}] p={m.parallelism} | "
                f"in={m.records_in_per_second:,.0f}/s | "
                f"busy={m.busy_ratio:.1%} {status_icon}"
            )

            if self.logger.level <= logging.DEBUG:
                self.logger.debug(f"   Debug: true_cap={m.true_processing_rate:,.0f}, selectivity={m.selectivity:.2f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", default="ds2_config.yaml")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    DS2Controller(args.config, args.dry_run, args.verbose).run()


    """
    python ds2_controller.py --dry-run -v
    """