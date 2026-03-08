#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DS2 自动扩缩容控制器
====================

基于 OSDI '18 论文 "Three Steps is All You Need" 实现
适配 Apache Flink 1.17 Standalone 集群

功能:
- 通过 REST API 采集 Flink 算子指标
- 基于 DS2 模型计算最优并发度
- 支持 Dry Run 模式（只观察不执行）
- 支持远程 Flink 集群

使用方法:
    python ds2_controller.py                     # 正常运行
    python ds2_controller.py --dry-run           # 只打印决策
    python ds2_controller.py -c my_config.yaml   # 使用自定义配置

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
# 配置管理
# ============================================================================

@dataclass
class Config:
    """DS2 配置类"""

    # Flink 集群配置
    flink_rest_url: str = "http://localhost:8081"
    flink_job_name: str = "DS2-Demo-Job"
    flink_savepoint_dir: str = "/tmp/flink-savepoints"

    # DS2 核心参数（论文建议值）
    policy_interval: int = 15  # 决策间隔（秒）
    warm_up_time: int = 30  # 预热时间（秒）
    activation_window: int = 3  # 激活窗口
    target_rate_ratio: float = 1.1  # 目标速率比
    min_busy_ratio: float = 0.05  # 最小忙碌比
    max_parallelism: int = 16  # 最大并发
    min_parallelism: int = 1  # 最小并发
    change_threshold: int = 1  # 变化阈值

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

            # 日志
            if 'logging' in data:
                config.log_level = data['logging'].get('level', config.log_level)

            print(f"[INFO] Config loaded from {path}")

        except Exception as e:
            print(f"[WARN] Failed to load config: {e}, using defaults")

        return config

    def validate(self) -> bool:
        """验证配置"""
        assert self.policy_interval >= 5, "policy_interval must be >= 5"
        assert self.warm_up_time >= 0, "warm_up_time must be >= 0"
        assert self.activation_window >= 1, "activation_window must be >= 1"
        assert 1.0 <= self.target_rate_ratio <= 2.0, "target_rate_ratio must be in [1.0, 2.0]"
        assert self.max_parallelism >= self.min_parallelism, "max >= min parallelism"
        return True


# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class OperatorMetrics:
    """算子指标"""
    vertex_id: str
    vertex_name: str
    parallelism: int

    # 原始指标（从 Flink 采集）
    records_in_per_second: float = 0.0
    records_out_per_second: float = 0.0
    busy_time_ms_per_second: float = 0.0

    # 计算指标（DS2 模型）
    busy_ratio: float = 0.0  # 忙碌比率 (0-1)
    true_processing_rate: float = 0.0  # 真实处理能力
    true_output_rate: float = 0.0  # 真实输出能力
    selectivity: float = 1.0  # 选择率 = output / input

    # 每个实例的指标（用于检测倾斜）
    per_subtask_busy: List[float] = field(default_factory=list)


@dataclass
class ScalingDecision:
    """扩缩容决策"""
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
    """作业拓扑"""
    job_id: str
    job_name: str
    vertices: Dict[str, OperatorMetrics] = field(default_factory=dict)
    edges: List[Tuple[str, str]] = field(default_factory=list)  # (source, target)

    def get_sources(self) -> List[str]:
        """获取 Source 算子"""
        targets = {e[1] for e in self.edges}
        return [v for v in self.vertices if v not in targets]

    def get_sinks(self) -> List[str]:
        """获取 Sink 算子"""
        sources = {e[0] for e in self.edges}
        return [v for v in self.vertices if v not in sources]

    def get_upstream(self, vertex_id: str) -> List[str]:
        """获取上游算子"""
        return [e[0] for e in self.edges if e[1] == vertex_id]

    def get_downstream(self, vertex_id: str) -> List[str]:
        """获取下游算子"""
        return [e[1] for e in self.edges if e[0] == vertex_id]

    def topological_sort(self) -> List[str]:
        """拓扑排序（从 Source 到 Sink）"""
        in_degree = defaultdict(int)
        for _, target in self.edges:
            in_degree[target] += 1

        # 入度为 0 的节点先处理
        queue = [v for v in self.vertices if in_degree[v] == 0]
        result = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            for downstream in self.get_downstream(node):
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    queue.append(downstream)

        # 如果拓扑排序不完整（有环），返回原始顺序
        return result if len(result) == len(self.vertices) else list(self.vertices.keys())


# ============================================================================
# Flink REST API 客户端
# ============================================================================

class FlinkClient:
    """Flink REST API 客户端"""

    # DS2 需要的核心指标
    REQUIRED_METRICS = [
        "numRecordsInPerSecond",
        "numRecordsOutPerSecond",
        "busyTimeMsPerSecond",
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
        """GET 请求"""
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self.session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            else:
                self.logger.debug(f"GET {endpoint} returned {resp.status_code}")
                return None
        except requests.exceptions.ConnectionError:
            self.logger.error(f"Cannot connect to Flink at {self.base_url}")
            return None
        except Exception as e:
            self.logger.debug(f"GET {endpoint} failed: {e}")
            return None

    def _post(self, endpoint: str, data: Dict = None, timeout: int = 30) -> Tuple[bool, Optional[Dict]]:
        """POST 请求"""
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self.session.post(url, json=data or {}, timeout=timeout)
            success = resp.status_code < 400
            result = resp.json() if resp.text else {}
            return success, result
        except Exception as e:
            self.logger.error(f"POST {endpoint} failed: {e}")
            return False, None

    def _patch(self, endpoint: str, timeout: int = 30) -> bool:
        """PATCH 请求"""
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self.session.patch(url, timeout=timeout)
            return resp.status_code < 400
        except Exception as e:
            self.logger.error(f"PATCH {endpoint} failed: {e}")
            return False

    def check_connection(self) -> bool:
        """检查与 Flink 的连接"""
        data = self._get("/overview")
        if data:
            self.logger.info(f"Connected to Flink {data.get('flink-version', 'unknown')}")
            return True
        return False

    def get_running_job_id(self) -> Optional[str]:
        """获取目标作业的 Job ID"""
        data = self._get("/jobs/overview")
        if not data:
            return None

        for job in data.get('jobs', []):
            if (job.get('name') == self.config.flink_job_name and
                    job.get('state') == 'RUNNING'):
                return job['jid']

        return None

    def get_job_topology(self, job_id: str) -> Optional[JobTopology]:
        """获取作业拓扑结构"""
        data = self._get(f"/jobs/{job_id}")
        if not data:
            return None

        topology = JobTopology(
            job_id=job_id,
            job_name=data.get('name', '')
        )

        # 解析顶点
        for vertex in data.get('vertices', []):
            vid = vertex['id']
            topology.vertices[vid] = OperatorMetrics(
                vertex_id=vid,
                vertex_name=vertex['name'],
                parallelism=vertex['parallelism']
            )

        # 解析边（从 plan 中）
        plan = data.get('plan', {})
        for node in plan.get('nodes', []):
            node_id = node['id']
            for inp in node.get('inputs', []):
                source_id = inp['id']
                topology.edges.append((source_id, node_id))

        self.logger.debug(f"Topology: {len(topology.vertices)} vertices, {len(topology.edges)} edges")
        return topology

    def get_vertex_metrics(self, job_id: str, vertex_id: str) -> Dict[str, List[float]]:
        """获取单个算子所有 subtask 的指标"""
        endpoint = f"/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics"
        params = f"?get={','.join(self.REQUIRED_METRICS)}"

        data = self._get(endpoint + params)
        if not data:
            return {}

        result = {m: [] for m in self.REQUIRED_METRICS}

        for subtask in data:
            metrics_map = {}
            for m in subtask.get('metrics', []):
                try:
                    metrics_map[m['id']] = float(m.get('value', 0))
                except (ValueError, TypeError):
                    metrics_map[m['id']] = 0.0

            for metric in self.REQUIRED_METRICS:
                result[metric].append(metrics_map.get(metric, 0.0))

        return result

    def trigger_savepoint(self, job_id: str) -> Optional[str]:
        """触发 Savepoint 并等待完成"""
        self.logger.info("Triggering savepoint...")

        success, resp = self._post(
            f"/jobs/{job_id}/savepoints",
            {"target-directory": self.config.flink_savepoint_dir, "cancel-job": False}
        )

        if not success or not resp:
            self.logger.error("Failed to trigger savepoint")
            return None

        trigger_id = resp.get("request-id")
        if not trigger_id:
            self.logger.error("No request-id in savepoint response")
            return None

        # 轮询等待完成（最多 120 秒）
        for i in range(120):
            time.sleep(1)
            status = self._get(f"/jobs/{job_id}/savepoints/{trigger_id}")

            if not status:
                continue

            state = status.get("status", {}).get("id")

            if state == "COMPLETED":
                location = status.get("operation", {}).get("location")
                self.logger.info(f"Savepoint completed: {location}")
                return location
            elif state == "FAILED":
                failure = status.get("operation", {}).get("failure-cause", {})
                self.logger.error(f"Savepoint failed: {failure.get('class', 'unknown')}")
                return None

            if i % 10 == 0:
                self.logger.debug(f"Waiting for savepoint... ({i}s)")

        self.logger.error("Savepoint timeout")
        return None

    def cancel_job(self, job_id: str) -> bool:
        """取消作业"""
        self.logger.info(f"Cancelling job {job_id}...")
        success = self._patch(f"/jobs/{job_id}?mode=cancel")
        if success:
            time.sleep(3)  # 等待作业停止
        return success


# ============================================================================
# 指标采集器
# ============================================================================

class MetricsCollector:
    """
    指标采集器
    负责从 Flink 采集原始指标并计算 DS2 模型所需的真实速率
    """

    def __init__(self, config: Config, flink_client: FlinkClient):
        self.config = config
        self.client = flink_client
        self.logger = logging.getLogger("DS2.Collector")

    def collect_all(self, topology: JobTopology) -> JobTopology:
        """
        采集所有算子的指标并计算真实速率

        核心公式 (论文 Eq. 1-2):
        - True Processing Rate = Observed Rate / Busy Ratio
        - Busy Ratio = busyTimeMsPerSecond / 1000
        """
        for vid, vertex in topology.vertices.items():
            raw = self.client.get_vertex_metrics(topology.job_id, vid)
            if not raw:
                continue

            # 获取原始指标
            in_rates = raw.get('numRecordsInPerSecond', [0])
            out_rates = raw.get('numRecordsOutPerSecond', [0])
            busy_times = raw.get('busyTimeMsPerSecond', [0])

            # 聚合：输入输出取总和，忙碌时间取平均
            total_in = sum(in_rates)
            total_out = sum(out_rates)
            avg_busy_ms = sum(busy_times) / len(busy_times) if busy_times else 0

            # 计算忙碌比率 (0.0 - 1.0)
            # busyTimeMsPerSecond 表示每秒中有多少毫秒在忙碌（最大 1000）
            busy_ratio = avg_busy_ms / 1000.0
            busy_ratio = max(busy_ratio, self.config.min_busy_ratio)  # 防止除零

            # ========== DS2 核心计算 ==========
            # 真实处理速率 = 观测速率 / 忙碌比率
            # 这表示如果算子 100% 忙碌时能达到的处理速率
            true_processing_rate = total_in / busy_ratio
            true_output_rate = total_out / busy_ratio

            # 选择率 = 输出 / 输入
            selectivity = total_out / total_in if total_in > 0 else 1.0
            # ===================================

            # 更新指标
            vertex.records_in_per_second = total_in
            vertex.records_out_per_second = total_out
            vertex.busy_time_ms_per_second = avg_busy_ms
            vertex.busy_ratio = busy_ratio
            vertex.true_processing_rate = true_processing_rate
            vertex.true_output_rate = true_output_rate
            vertex.selectivity = selectivity
            vertex.per_subtask_busy = [b / 1000.0 for b in busy_times]

        return topology

    def detect_skew(self, vertex: OperatorMetrics, threshold: float = 0.3) -> bool:
        """
        检测数据倾斜
        如果某个 subtask 的负载明显高于平均值，返回 True
        """
        if len(vertex.per_subtask_busy) < 2:
            return False

        avg = sum(vertex.per_subtask_busy) / len(vertex.per_subtask_busy)
        if avg == 0:
            return False

        max_deviation = max(abs(b - avg) / avg for b in vertex.per_subtask_busy)
        return max_deviation > threshold


# ============================================================================
# DS2 扩缩容模型
# ============================================================================

class DS2Model:
    """
    DS2 扩缩容模型

    实现论文核心算法:
    1. 按拓扑序遍历所有算子
    2. 计算每个算子的目标输入速率（= 上游目标输出之和）
    3. 计算最优并发度: π* = ⌈target_rate / true_rate_per_instance⌉ × ratio
    """

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("DS2.Model")

        # 决策历史（用于 Activation Window）
        self.decision_history: deque = deque(maxlen=config.activation_window)

    def compute_plan(self, topology: JobTopology) -> Dict[str, ScalingDecision]:
        """
        计算扩缩容计划

        论文公式 (7):
        π*_i = ⌈(Σ A_ji × o_j[λ_o^*]) / (o_i[λ_p] / p_i)⌉

        简化理解:
        π*_i = ⌈target_input_rate / true_rate_per_instance⌉ × safety_ratio
        """
        decisions = {}

        # 按拓扑序处理（确保先处理上游）
        sorted_vertices = topology.topological_sort()

        # 存储每个算子的"目标输出速率"（传递给下游）
        target_output: Dict[str, float] = {}

        for vid in sorted_vertices:
            metrics = topology.vertices[vid]
            current_p = metrics.parallelism

            # 判断是否为 Source
            upstream = topology.get_upstream(vid)
            is_source = len(upstream) == 0

            if is_source:
                # Source 算子：保持当前并发度
                # 可扩展：根据 Kafka Lag 等外部指标调整
                decisions[vid] = ScalingDecision(
                    vertex_id=vid,
                    vertex_name=metrics.vertex_name,
                    current_parallelism=current_p,
                    target_parallelism=current_p,
                    reason="Source operator (maintain)"
                )
                # Source 的目标输出 = 当前实际输出
                target_output[vid] = metrics.records_out_per_second
                continue

            # ========== DS2 核心算法 ==========

            # Step 1: 计算目标输入速率
            # = 所有上游算子的目标输出之和
            target_input = sum(
                target_output.get(u, topology.vertices[u].records_out_per_second)
                for u in upstream
            )

            # Step 2: 计算单实例的真实处理能力
            # = 整个算子的真实处理能力 / 当前并发度
            true_rate_per_instance = metrics.true_processing_rate / current_p if current_p > 0 else 1.0

            # Step 3: 计算最优并发度
            if true_rate_per_instance > 0:
                # 基础公式: target_input / true_rate_per_instance
                # 乘以 safety_ratio 预留冗余
                raw_parallelism = (target_input / true_rate_per_instance) * self.config.target_rate_ratio
                target_p = math.ceil(raw_parallelism)
            else:
                target_p = current_p

            # Step 4: 应用约束
            target_p = max(self.config.min_parallelism, target_p)
            target_p = min(self.config.max_parallelism, target_p)

            # Step 5: 忽略微小变化
            if abs(target_p - current_p) <= self.config.change_threshold:
                target_p = current_p

            # ===================================

            # 生成决策
            reason = self._make_reason(metrics, target_input, true_rate_per_instance, target_p)
            decisions[vid] = ScalingDecision(
                vertex_id=vid,
                vertex_name=metrics.vertex_name,
                current_parallelism=current_p,
                target_parallelism=target_p,
                reason=reason
            )

            # 计算该算子的目标输出（传递给下游）
            # 目标输出 = 目标输入 × 选择率
            target_output[vid] = target_input * metrics.selectivity

            self.logger.debug(
                f"{metrics.vertex_name}: "
                f"busy={metrics.busy_ratio:.1%}, "
                f"true_cap={metrics.true_processing_rate:.0f}/s, "
                f"target_in={target_input:.0f}/s, "
                f"per_inst={true_rate_per_instance:.0f}/s, "
                f"p: {current_p} -> {target_p}"
            )

        return decisions

    def _make_reason(self, metrics: OperatorMetrics, target_in: float,
                     rate_per_inst: float, target_p: int) -> str:
        """生成决策原因说明"""
        current_p = metrics.parallelism

        if target_p > current_p:
            return (f"SCALE UP: busy={metrics.busy_ratio:.0%}, "
                    f"target_in={target_in:.0f}/s, capacity={rate_per_inst:.0f}/s/inst")
        elif target_p < current_p:
            return (f"SCALE DOWN: over-provisioned, busy={metrics.busy_ratio:.0%}")
        else:
            return "OPTIMAL: no change needed"

    def should_apply(self, decisions: Dict[str, ScalingDecision]) -> Tuple[bool, str]:
        """
        判断是否应该执行扩缩容

        使用 Activation Window 机制:
        - 只有连续 N 次决策完全一致才执行
        - 避免因短期波动导致频繁扩缩容
        """
        # 检查是否有变化
        needs_change = any(d.needs_change for d in decisions.values())
        if not needs_change:
            self.decision_history.clear()
            return False, "No scaling needed"

        # 转换为可比较的格式
        current = {vid: d.target_parallelism for vid, d in decisions.items()}
        self.decision_history.append(current)

        # 检查是否达到激活窗口
        if len(self.decision_history) < self.config.activation_window:
            remaining = self.config.activation_window - len(self.decision_history)
            return False, f"Waiting for stable decision ({remaining} more)"

        # 检查所有历史决策是否一致
        first = self.decision_history[0]
        all_same = all(d == first for d in self.decision_history)

        if all_same:
            return True, "Decision stable, ready to apply"
        else:
            return False, "Decisions not stable (fluctuating)"

    def reset_history(self):
        """重置决策历史（扩缩容后调用）"""
        self.decision_history.clear()


# ============================================================================
# 扩缩容执行器
# ============================================================================

class ScalingExecutor:
    """
    扩缩容执行器

    执行流程:
    1. 触发 Savepoint
    2. 取消当前作业
    3. 提示用户使用新并发度重启作业

    注意：由于控制器运行在宿主机，无法直接在集群上执行 flink run，
    因此只能输出重启命令供用户手动执行或集成到自动化脚本中
    """

    def __init__(self, config: Config, flink_client: FlinkClient):
        self.config = config
        self.client = flink_client
        self.logger = logging.getLogger("DS2.Executor")

        self.last_scale_time: float = 0
        self.last_savepoint: Optional[str] = None

    def is_warming_up(self) -> bool:
        """检查是否在预热期"""
        if self.last_scale_time == 0:
            return False
        elapsed = time.time() - self.last_scale_time
        return elapsed < self.config.warm_up_time

    def get_warmup_remaining(self) -> float:
        """获取剩余预热时间"""
        if self.last_scale_time == 0:
            return 0
        elapsed = time.time() - self.last_scale_time
        return max(0, self.config.warm_up_time - elapsed)

    def execute(self, job_id: str, decisions: Dict[str, ScalingDecision],
                dry_run: bool = False) -> bool:
        """
        执行扩缩容

        Args:
            job_id: 当前作业 ID
            decisions: 扩缩容决策
            dry_run: 如果为 True，只打印不执行

        Returns:
            是否成功
        """
        # 筛选需要变化的算子
        changes = [(d.vertex_name, d.current_parallelism, d.target_parallelism, d.reason)
                   for d in decisions.values() if d.needs_change]

        if not changes:
            return True

        # 打印扩缩容计划
        self.logger.info("=" * 70)
        self.logger.info("SCALING PLAN")
        self.logger.info("=" * 70)

        for name, cur_p, new_p, reason in changes:
            direction = "↑" if new_p > cur_p else "↓"
            self.logger.info(f"  {name}: {cur_p} -> {new_p} {direction}")
            self.logger.info(f"    Reason: {reason}")

        self.logger.info("-" * 70)

        if dry_run:
            self.logger.info("[DRY RUN] Would apply above changes")
            self.logger.info("=" * 70)
            return True

        # Step 1: 触发 Savepoint
        savepoint = self.client.trigger_savepoint(job_id)
        if not savepoint:
            self.logger.error("Failed to create savepoint, aborting scaling")
            return False

        self.last_savepoint = savepoint

        # Step 2: 取消作业
        if not self.client.cancel_job(job_id):
            self.logger.error("Failed to cancel job")
            return False

        # Step 3: 输出重启命令
        max_parallelism = max(d.target_parallelism for d in decisions.values())

        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("SCALING ACTION REQUIRED")
        self.logger.info("=" * 70)
        self.logger.info("")
        self.logger.info("Job has been stopped. To restart with new parallelism:")
        self.logger.info("")
        self.logger.info("  1. SSH to your Flink JobManager")
        self.logger.info("")
        self.logger.info("  2. Run the following command:")
        self.logger.info("")
        self.logger.info(f"     flink run -d \\")
        self.logger.info(f"       -s {savepoint} \\")
        self.logger.info(f"       -p {max_parallelism} \\")
        self.logger.info(f"       /path/to/your/job.jar")
        self.logger.info("")
        self.logger.info("  3. Or use operator-specific parallelism:")
        self.logger.info("")
        for d in decisions.values():
            if d.needs_change:
                self.logger.info(f"     {d.vertex_name}: {d.target_parallelism}")
        self.logger.info("")
        self.logger.info("=" * 70)

        # 记录扩缩容时间
        self.last_scale_time = time.time()

        return True


# ============================================================================
# 主控制器
# ============================================================================

class DS2Controller:
    """
    DS2 主控制器

    实现完整的自动扩缩容控制循环:
    1. 查找目标作业
    2. 采集指标
    3. 计算扩缩容计划
    4. 判断是否执行（Activation Window）
    5. 执行扩缩容
    6. 进入预热期
    """

    def __init__(self, config_path: str, dry_run: bool = False, verbose: bool = False):
        # 加载配置
        self.config = Config.from_yaml(config_path)
        self.config.validate()

        self.dry_run = dry_run
        self.running = True

        # 设置日志
        log_level = logging.DEBUG if verbose else getattr(logging, self.config.log_level.upper())
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s | %(name)-20s | %(levelname)-5s | %(message)s',
            datefmt='%H:%M:%S'
        )
        self.logger = logging.getLogger("DS2.Controller")

        # 初始化组件
        self.client = FlinkClient(self.config)
        self.collector = MetricsCollector(self.config, self.client)
        self.model = DS2Model(self.config)
        self.executor = ScalingExecutor(self.config, self.client)

        # 信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """处理退出信号"""
        self.logger.info("Received shutdown signal")
        self.running = False

    def run(self):
        """主控制循环"""
        self._print_banner()

        # 检查连接
        if not self.client.check_connection():
            self.logger.error(f"Cannot connect to Flink at {self.config.flink_rest_url}")
            self.logger.error("Please check:")
            self.logger.error("  1. Flink cluster is running")
            self.logger.error("  2. REST API is accessible from this machine")
            self.logger.error("  3. Firewall allows port 8081")
            return

        while self.running:
            try:
                loop_start = time.time()

                # Step 1: 查找作业
                job_id = self.client.get_running_job_id()
                if not job_id:
                    self.logger.warning(f"Job '{self.config.flink_job_name}' not found or not running")
                    self._wait_interval(loop_start)
                    continue

                # Step 2: 检查预热期
                if self.executor.is_warming_up():
                    remaining = self.executor.get_warmup_remaining()
                    self.logger.info(f"Warming up... {remaining:.0f}s remaining")
                    self._wait_interval(loop_start)
                    continue

                # Step 3: 获取拓扑
                topology = self.client.get_job_topology(job_id)
                if not topology:
                    self._wait_interval(loop_start)
                    continue

                # Step 4: 采集指标
                topology = self.collector.collect_all(topology)

                # Step 5: 打印当前状态
                self._print_status(topology)

                # Step 6: 计算扩缩容计划
                decisions = self.model.compute_plan(topology)

                # Step 7: 判断是否执行
                should_apply, reason = self.model.should_apply(decisions)
                self.logger.info(f"Decision: {reason}")

                # Step 8: 执行扩缩容
                if should_apply:
                    success = self.executor.execute(job_id, decisions, dry_run=self.dry_run)
                    if success:
                        self.model.reset_history()

                # 等待下一周期
                self._wait_interval(loop_start)

            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.exception(f"Error in control loop: {e}")
                self._wait_interval(time.time())

        self.logger.info("DS2 Controller stopped")

    def _print_banner(self):
        """打印启动信息"""
        self.logger.info("=" * 70)
        self.logger.info("DS2 Auto-Scaling Controller for Apache Flink")
        self.logger.info("Based on OSDI '18: 'Three Steps is All You Need'")
        self.logger.info("=" * 70)
        self.logger.info(f"Flink REST URL    : {self.config.flink_rest_url}")
        self.logger.info(f"Target Job        : {self.config.flink_job_name}")
        self.logger.info(f"Policy Interval   : {self.config.policy_interval}s")
        self.logger.info(f"Activation Window : {self.config.activation_window}")
        self.logger.info(f"Target Rate Ratio : {self.config.target_rate_ratio}")
        self.logger.info(f"Dry Run           : {self.dry_run}")
        self.logger.info("=" * 70)

    def _print_status(self, topology: JobTopology):
        """打印当前算子状态"""
        self.logger.info("-" * 70)
        self.logger.info("Current Operator Status:")


        for vid in topology.topological_sort():
            m = topology.vertices[vid]

            # 忙碌程度指示器
            if m.busy_ratio > 0.8:
                status = "🔴 HIGH"
            elif m.busy_ratio > 0.5:
                status = "🟡 MED"
            else:
                status = "🟢 LOW"

            self.logger.info(
                f"  [{m.vertex_name}] "
                f"p={m.parallelism}, "
                f"in={m.records_in_per_second:,.0f}/s, "
                f"out={m.records_out_per_second:,.0f}/s, "
                f"busy={m.busy_ratio:.0%} {status}, "
                f"capacity={m.true_processing_rate:,.0f}/s"
            )

            # 检测倾斜
            if self.collector.detect_skew(m):
                self.logger.warning(f"    ⚠️  Data skew detected!")
            # 增加原始指标日志，方便排查 Flink 是否返回了有效数据
            self.logger.debug(f"    Raw Metrics: in={m.records_in_per_second}, busy_ms={m.busy_time_ms_per_second}")

    def _wait_interval(self, loop_start: float):
        """等待到下一个决策周期"""
        elapsed = time.time() - loop_start
        sleep_time = max(1, self.config.policy_interval - elapsed)
        time.sleep(sleep_time)


# ============================================================================
# 命令行入口
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="DS2 Auto-Scaling Controller for Apache Flink 1.17",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ds2_controller.py                      # Run with default config
  python ds2_controller.py --dry-run            # Only print decisions
  python ds2_controller.py -c my_config.yaml    # Use custom config
  python ds2_controller.py -v                   # Verbose output
  python ds2_controller.py --dry-run -v

http://192.168.56.151:8081/jobs/<JOB_ID>/vertices/<VERTEX_ID>/subtasks/metrics?get=numRecordsIn,numRecordsInPerSecond,busyTimeMsPerSecond

Configuration:
  Create a ds2_config.yaml file with your Flink cluster settings.
  See the example config for available options.
        """
    )

    parser.add_argument(
        "-c", "--config",
        default="ds2_config.yaml",
        help="Path to configuration file (default: ds2_config.yaml)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print scaling decisions without executing"
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging"
    )

    args = parser.parse_args()

    controller = DS2Controller(
        config_path=args.config,
        dry_run=args.dry_run,
        verbose=args.verbose
    )
    controller.run()


if __name__ == "__main__":
    main()