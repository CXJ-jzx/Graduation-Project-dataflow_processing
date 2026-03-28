"""
Flink REST API 指标采集模块 (修正版)

修正内容:
1. 使用 subtasks/metrics 端点获取实时速率(非累积计数器/总时长)
2. 正确解析返回格式 {min, max, avg, sum}，处理 NaN
3. collect_window 中计算 peak_input_rate 和 input_cv
4. 使用已验证可用的 Sink 自定义延迟指标

数据流向:
  Flink REST API → collect_once() → 16维 raw dict
                 → collect_window() → List[dict] (含窗口级统计)
                 → (由 FeatureEngineer.aggregate_median 聚合)
"""

import math
import time
import requests
import numpy as np
from typing import Dict, List, Optional
import yaml
import logging

logger = logging.getLogger("ml_tuner.observation_collector")


class ObservationCollector:
    """Flink REST API 指标采集器"""

    def __init__(self, config_path: str = None, config_dict: dict = None):
        if config_dict is not None:
            cfg = config_dict
        elif config_path is not None:
            with open(config_path, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f)
        else:
            raise ValueError("必须提供 config_path 或 config_dict")

        self.rest_url = cfg['flink']['rest_url'].rstrip('/')
        self.job_name = cfg['flink']['job_name']

        # 算子关键字（用于 vertex 发现）
        self.operator_names = cfg['metrics']['operator_names']

        # 自定义 Gauge 指标的算子前缀
        self.gauge_prefixes = cfg['metrics']['gauge_prefixes']

        # 采集窗口配置
        coll_cfg = cfg['metrics']['collection']
        self.window_seconds = int(coll_cfg['window_seconds'])
        self.interval_seconds = int(coll_cfg['interval_seconds'])
        self.sample_count = int(coll_cfg['sample_count'])

        # 缓存
        self._job_id: Optional[str] = None
        self._vertex_map: Dict[str, str] = {}
        self._source_vertex_id: Optional[str] = None

        # HTTP Session
        self._session = requests.Session()
        self._session.headers.update({'Accept': 'application/json'})
        self._timeout = 10

        logger.info("ObservationCollector 初始化完成: rest_url=%s, job=%s",
                     self.rest_url, self.job_name)

    # ==========================================
    # 连接与发现
    # ==========================================

    def discover(self) -> bool:
        try:
            resp = self._get("/jobs/overview")
            jobs = resp.get('jobs', [])

            self._job_id = None
            for job in jobs:
                if job['name'] == self.job_name and job['state'] == 'RUNNING':
                    self._job_id = job['jid']
                    break

            if not self._job_id:
                logger.error("未找到运行中的作业: %s", self.job_name)
                return False

            logger.info("发现作业: jid=%s", self._job_id)

            resp = self._get(f"/jobs/{self._job_id}")
            vertices = resp.get('vertices', [])

            self._vertex_map = {}
            for op_key, op_keyword in self.operator_names.items():
                for vertex in vertices:
                    if op_keyword in vertex['name']:
                        self._vertex_map[op_key] = vertex['id']
                        logger.info("  Vertex 映射: %s → %s (%s)",
                                    op_key, vertex['id'][:12], vertex['name'])
                        break
                else:
                    logger.warning("  未找到算子: %s (关键字: %s)", op_key, op_keyword)

            self._source_vertex_id = self._vertex_map.get('source')

            found = len(self._vertex_map)
            total = len(self.operator_names)
            logger.info("Vertex 发现完成: %d/%d", found, total)
            return found > 0

        except Exception as e:
            logger.error("发现失败: %s", e)
            return False

    def get_job_id(self) -> Optional[str]:
        return self._job_id

    def is_job_running(self) -> bool:
        if not self._job_id:
            return False
        try:
            resp = self._get(f"/jobs/{self._job_id}")
            return resp.get('state') == 'RUNNING'
        except Exception:
            return False

    # ==========================================
    # 窗口采集
    # ==========================================

    def collect_window(self) -> List[Dict[str, float]]:
        """
        窗口式多次采样

        ★ 修正: 在窗口级别计算 peak_input_rate 和 input_cv
        """
        if not self._job_id:
            if not self.discover():
                logger.error("collect_window: 作业发现失败")
                return []

        samples = []
        logger.info("开始窗口采集: %d 次, 间隔 %ds",
                     self.sample_count, self.interval_seconds)

        for i in range(self.sample_count):
            try:
                sample = self.collect_once()
                if sample:
                    samples.append(sample)
                    logger.debug("采样 %d/%d 完成: input_rate=%.1f, latency_p99=%.1f",
                                 i + 1, self.sample_count,
                                 sample.get('avg_input_rate', 0),
                                 sample.get('e2e_latency_p99_ms', sample.get('latency_p99', 0)))
                else:
                    logger.warning("采样 %d/%d 返回空", i + 1, self.sample_count)
            except Exception as e:
                logger.error("采样 %d/%d 失败: %s", i + 1, self.sample_count, e)

            if i < self.sample_count - 1:
                time.sleep(self.interval_seconds)

        # ━━━ ★ 窗口级统计量（修正 peak_input_rate 和 input_cv）━━━
        if samples:
            input_rates = [s.get('avg_input_rate', 0.0) for s in samples]

            # peak = 窗口内真实最大值（而非 1.2 倍估算）
            peak = float(max(input_rates)) if input_rates else 0.0

            # CV = 窗口内输入速率的变异系数
            if len(input_rates) > 1:
                mean_rate = float(np.mean(input_rates))
                std_rate = float(np.std(input_rates))
                cv = std_rate / mean_rate if mean_rate > 0 else 0.0
            else:
                cv = 0.0

            # 写回每条样本（这样 aggregate_median 取中位数时值一致）
            for s in samples:
                s['peak_input_rate'] = peak
                s['input_cv'] = cv

        logger.info("窗口采集完成: %d/%d 次成功", len(samples), self.sample_count)
        return samples

    # ==========================================
    # 单次采集
    # ==========================================

    def collect_once(self) -> Optional[Dict[str, float]]:
        """
        单次采集全部 16 维环境特征

        ★ 修正: 全部使用 subtasks/metrics 端点获取实时指标
        """
        if not self._job_id:
            if not self.discover():
                return None

        try:
            features = {}

            # 1. 吞吐量（Source 实时输出速率）
            self._collect_throughput(features)

            # 2. 繁忙率和背压（各算子实时指标）
            self._collect_busy_and_backpressure(features)

            # 3. 总并行度
            self._collect_parallelism(features)

            # 4. L2/L3 缓存指标（自定义 Gauge，需算子名前缀）
            self._collect_custom_gauges(features)

            # 5. 端到端延迟 P99（Sink 自定义指标）
            self._collect_latency(features)

            # 6. Checkpoint 统计
            self._collect_checkpoint_stats(features)

            # 7. 堆内存使用率
            self._collect_heap_usage(features)

            # peak_input_rate 和 input_cv 由 collect_window 覆写
            features.setdefault('peak_input_rate', features.get('avg_input_rate', 0.0))
            features.setdefault('input_cv', 0.0)

            return features

        except Exception as e:
            logger.error("单次采集失败: %s", e)
            return None

    # ==========================================
    # 分项采集（全部使用 subtasks/metrics 端点）
    # ==========================================

    def _collect_throughput(self, features: Dict[str, float]):
        """
        ★ 修正: 使用 numRecordsOutPerSecond 实时指标，而非累积计数/总时长

        Source 的 numRecordsOutPerSecond.sum = 全部 subtask 的总输出速率
        """
        source_vid = self._vertex_map.get('source')
        if not source_vid:
            features['avg_input_rate'] = 0.0
            features['active_node_count'] = 0
            features['avg_backlog'] = 0.0
            return

        data = self._query_subtask_metrics(
            source_vid, ['numRecordsOutPerSecond']
        )

        rate_data = data.get('numRecordsOutPerSecond', {})
        # sum = 所有 subtask 的总吞吐量
        total_rate = rate_data.get('sum', 0.0)

        features['avg_input_rate'] = total_rate
        # 活跃节点数：用 Source 输出速率近似（每节点每秒约1条）
        features['active_node_count'] = min(total_rate, 2000.0)
        features['avg_backlog'] = 0.0

        logger.debug("吞吐量: avg_input_rate=%.1f records/s", total_rate)

    def _collect_busy_and_backpressure(self, features: Dict[str, float]):
        """
        ★ 修正: 使用 busyTimeMsPerSecond / idleTimeMsPerSecond 实时指标

        背压估算: backpressured_time ≈ 1000 - busy_time - idle_time
        (Flink 中 busy + idle + backpressured ≈ 1000 ms/s)
        """
        busy_avgs = []     # 各算子的 avg busy ratio
        busy_maxes = []    # 各算子的 max busy ratio（最热 subtask）
        bp_ratios = []     # 各算子的背压比例

        for op_key, vid in self._vertex_map.items():
            # Source 的 busyTimeMsPerSecond 是 NaN，跳过
            if op_key == 'source':
                continue

            data = self._query_subtask_metrics(
                vid, ['busyTimeMsPerSecond', 'idleTimeMsPerSecond']
            )

            busy = data.get('busyTimeMsPerSecond', {})
            idle = data.get('idleTimeMsPerSecond', {})

            busy_avg = busy.get('avg', 0.0)     # 各 subtask 平均
            busy_max = busy.get('max', 0.0)     # 最热 subtask
            idle_avg = idle.get('avg', 0.0)

            busy_avgs.append(busy_avg / 1000.0)
            busy_maxes.append(busy_max / 1000.0)

            # 背压 = 剩余时间（非 busy 也非 idle 的部分）
            bp_ms = max(0.0, 1000.0 - busy_avg - idle_avg)
            bp_ratios.append(bp_ms / 1000.0)

        features['avg_busy_ratio'] = float(np.mean(busy_avgs)) if busy_avgs else 0.0
        features['max_busy_ratio'] = float(max(busy_maxes)) if busy_maxes else 0.0
        features['backpressure_ratio'] = float(max(bp_ratios)) if bp_ratios else 0.0

        logger.debug("繁忙率: avg=%.4f, max=%.4f, 背压=%.4f",
                     features['avg_busy_ratio'], features['max_busy_ratio'],
                     features['backpressure_ratio'])

    def _collect_parallelism(self, features: Dict[str, float]):
        """从 Job 详情获取总并行度"""
        try:
            resp = self._get(f"/jobs/{self._job_id}")
            total_p = sum(v.get('parallelism', 1) for v in resp.get('vertices', []))
            features['total_parallelism'] = float(total_p)
        except Exception as e:
            logger.error("采集并行度失败: %s", e)
            features['total_parallelism'] = 0.0

    def _collect_custom_gauges(self, features: Dict[str, float]):
        """
        采集自定义 Gauge 指标: L2/L3 缓存命中率和占用率

        ★ 关键: 指标名必须带算子名前缀
        已验证可用:
          Feature-Extraction.l2_hit_rate  (subtasks/metrics, avg)
          Grid-Aggregation.l3_hit_rate    (subtasks/metrics, avg)
        """
        # --- L2 指标（Feature-Extraction 算子）---
        feature_vid = self._vertex_map.get('feature')
        if feature_vid:
            prefix = self.gauge_prefixes.get('feature', 'Feature-Extraction')
            hit_name = f"{prefix}.l2_hit_rate"
            occ_name = f"{prefix}.l2_occupancy"

            data = self._query_subtask_metrics(feature_vid, [hit_name, occ_name])
            features['l2_hit_rate'] = data.get(hit_name, {}).get('avg', 0.0)
            features['l2_occupancy'] = data.get(occ_name, {}).get('avg', 0.0)
        else:
            features['l2_hit_rate'] = 0.0
            features['l2_occupancy'] = 0.0

        # --- L3 指标（Grid-Aggregation 算子）---
        grid_vid = self._vertex_map.get('grid')
        if grid_vid:
            prefix = self.gauge_prefixes.get('grid', 'Grid-Aggregation')
            hit_name = f"{prefix}.l3_hit_rate"
            occ_name = f"{prefix}.l3_occupancy"

            data = self._query_subtask_metrics(grid_vid, [hit_name, occ_name])
            features['l3_hit_rate'] = data.get(hit_name, {}).get('avg', 0.0)
            features['l3_occupancy'] = data.get(occ_name, {}).get('avg', 0.0)
        else:
            features['l3_hit_rate'] = 0.0
            features['l3_occupancy'] = 0.0

        logger.debug("缓存: L2 hit=%.4f occ=%.4f, L3 hit=%.4f occ=%.4f",
                     features['l2_hit_rate'], features['l2_occupancy'],
                     features['l3_hit_rate'], features['l3_occupancy'])

    def _collect_latency(self, features: Dict[str, float]):
        """
        优先采集端到端延迟:
          Sink__EventFeature-Sink.e2e_latency_p99_ms  (ms)
        回退:
          Sink__EventFeature-Sink.latency_p99         (μs, 转 ms)
        """
        event_sink_vid = self._vertex_map.get('event_sink')
        if not event_sink_vid:
            features['e2e_latency_p99_ms'] = 0.0
            features['latency_p99'] = 0.0
            logger.warning("event_sink vertex 未发现，无法采集延迟")
            return

        prefix = self.gauge_prefixes.get('event_sink', 'Sink__EventFeature-Sink')
        metric_e2e = f"{prefix}.e2e_latency_p99_ms"
        metric_sink = f"{prefix}.latency_p99"

        data = self._query_subtask_metrics(event_sink_vid, [metric_e2e, metric_sink])

        e2e = data.get(metric_e2e, {}).get('max', 0.0)
        sink_us = data.get(metric_sink, {}).get('max', 0.0)

        # 统一输出:
        # - e2e_latency_p99_ms: 真正端到端（ms）
        # - latency_p99: 兼容旧代码（统一为 ms）
        if e2e > 0:
            features['e2e_latency_p99_ms'] = e2e
            features['latency_p99'] = e2e  # 兼容旧目标函数字段
            logger.debug("延迟: e2e_p99=%.1f ms", e2e)
        else:
            # 回退: sink latency 是微秒，换算成毫秒
            sink_ms = sink_us / 1000.0
            features['e2e_latency_p99_ms'] = sink_ms
            features['latency_p99'] = sink_ms
            logger.warning("e2e_latency_p99_ms 不可用，回退到 sink latency=%.3f ms", sink_ms)

    def _collect_checkpoint_stats(self, features: Dict[str, float]):
        """采集 Checkpoint 统计信息"""
        try:
            resp = self._get(f"/jobs/{self._job_id}/checkpoints")
            summary = resp.get('summary', {})
            e2e_dur = summary.get('end_to_end_duration', {})
            features['checkpoint_avg_dur'] = float(e2e_dur.get('avg', 0))
            logger.debug("Checkpoint avg_dur: %.1f ms", features['checkpoint_avg_dur'])
        except Exception as e:
            logger.error("采集 Checkpoint 统计失败: %s", e)
            features.setdefault('checkpoint_avg_dur', 0.0)

    def _collect_heap_usage(self, features: Dict[str, float]):
        """采集 TaskManager 堆内存使用率"""
        try:
            resp = self._get("/taskmanagers")
            taskmanagers = resp.get('taskmanagers', [])

            if not taskmanagers:
                features['heap_usage_ratio'] = 0.0
                return

            heap_ratios = []
            for tm in taskmanagers:
                tmid = tm['id']
                try:
                    metrics_resp = self._get(
                        f"/taskmanagers/{tmid}/metrics"
                        f"?get=Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Max"
                    )
                    heap_used = 0
                    heap_max = 0
                    for item in metrics_resp:
                        if item['id'] == 'Status.JVM.Memory.Heap.Used':
                            heap_used = self._safe(item.get('value', 0))
                        elif item['id'] == 'Status.JVM.Memory.Heap.Max':
                            heap_max = self._safe(item.get('value', 0))

                    if heap_max > 0:
                        heap_ratios.append(heap_used / heap_max)
                except Exception as e:
                    logger.debug("TM %s 堆内存查询失败: %s", tmid[:12], e)

            features['heap_usage_ratio'] = float(np.mean(heap_ratios)) if heap_ratios else 0.0
            logger.debug("Heap usage: %.4f (avg of %d TMs)",
                         features['heap_usage_ratio'], len(heap_ratios))

        except Exception as e:
            logger.error("采集堆内存失败: %s", e)
            features.setdefault('heap_usage_ratio', 0.0)

    # ==========================================
    # 底层查询工具
    # ==========================================

    def _query_subtask_metrics(self, vertex_id: str,
                                metric_names: List[str]) -> Dict[str, Dict[str, float]]:
        """
        查询 subtasks/metrics 端点（已验证可用的端点）

        Args:
            vertex_id: Flink Vertex ID
            metric_names: 指标名列表

        Returns:
            {metric_name: {"min": x, "max": x, "avg": x, "sum": x}}
        """
        result = {}
        try:
            names_str = ",".join(metric_names)
            resp = self._get(
                f"/jobs/{self._job_id}/vertices/{vertex_id}"
                f"/subtasks/metrics?get={names_str}"
            )

            for item in resp:
                mid = item['id']
                result[mid] = {
                    "min": self._safe(item.get("min", 0)),
                    "max": self._safe(item.get("max", 0)),
                    "avg": self._safe(item.get("avg", 0)),
                    "sum": self._safe(item.get("sum", 0)),
                }

        except Exception as e:
            logger.error("查询 subtask metrics 失败: vertex=%s, error=%s",
                         vertex_id[:12], e)

        return result

    @staticmethod
    def _safe(val, default=0.0) -> float:
        """
        安全转换为 float

        处理: None, "NaN", 非数字字符串, math.nan
        """
        if val is None:
            return default
        if isinstance(val, str):
            if val.lower() == 'nan':
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

    def _get(self, path: str):
        """GET 请求"""
        if path.startswith("http"):
            url = path
        else:
            url = f"{self.rest_url}{path}"
        resp = self._session.get(url, timeout=self._timeout)
        resp.raise_for_status()
        return resp.json()

    # ==========================================
    # 清理
    # ==========================================

    def reset_cache(self):
        self._job_id = None
        self._vertex_map = {}
        self._source_vertex_id = None
        logger.info("采集器缓存已清除")

    # ==========================================
    # 稳定性检测（savepoint 恢复后等待指标初始化）
    # ==========================================

    def wait_until_stable(self,
                          bp_threshold: float = 0.50,
                          stable_required: int = 3,
                          check_interval: int = 15,
                          timeout: int = 300,
                          stop_flag=None) -> bool:
        """
        等待系统指标稳定后再采集。

        Flink 从 savepoint 恢复后，busyTimeMsPerSecond / idleTimeMsPerSecond
        等 Task 级指标需要时间初始化。初始化完成前两者均为 0，导致计算出的
        backpressure_ratio = 1.0（虚假背压）。

        本方法轮询背压指标，连续 stable_required 次低于阈值后返回 True。

        Args:
            bp_threshold: 背压阈值（低于此值认为稳定）
            stable_required: 需要连续多少次低于阈值
            check_interval: 检查间隔（秒）
            timeout: 最长等待（秒）
            stop_flag: 可选的停止标志（threading.Event 或 callable）

        Returns:
            True=已稳定，False=超时
        """
        if not self._job_id:
            if not self.discover():
                logger.warning("wait_until_stable: 作业发现失败")
                return False

        logger.info("等待系统稳定 (bp < %.0f%%, 连续 %d 次, 超时 %ds)...",
                     bp_threshold * 100, stable_required, timeout)

        start = time.time()
        consecutive_ok = 0
        last_bp = 1.0

        while time.time() - start < timeout:
            # 检查停止标志
            if stop_flag is not None:
                if callable(stop_flag) and stop_flag():
                    return False
                if hasattr(stop_flag, 'is_set') and stop_flag.is_set():
                    return False

            try:
                bp = self._quick_check_backpressure()
                last_bp = bp

                if bp < bp_threshold:
                    consecutive_ok += 1
                    if consecutive_ok >= stable_required:
                        elapsed = time.time() - start
                        logger.info("✅ 系统已稳定: bp=%.2f, 耗时 %.0fs, "
                                    "连续 %d 次达标",
                                    bp, elapsed, consecutive_ok)
                        return True
                    logger.debug("稳定性检查: bp=%.2f ✓ (%d/%d)",
                                 bp, consecutive_ok, stable_required)
                else:
                    if consecutive_ok > 0:
                        logger.debug("稳定性检查: bp=%.2f ✗, 重置计数", bp)
                    consecutive_ok = 0

            except Exception as e:
                logger.warning("稳定性检查异常: %s", e)
                consecutive_ok = 0

            time.sleep(check_interval)

        elapsed = time.time() - start
        logger.warning("⚠️ 稳定性等待超时 (%ds), 最后 bp=%.2f",
                       int(elapsed), last_bp)
        return False

    def _quick_check_backpressure(self) -> float:
        """
        快速检查当前最大背压值

        Returns:
            float: 0.0~1.0 的背压比例
        """
        bp_ratios = []

        for op_key, vid in self._vertex_map.items():
            if op_key == 'source':
                continue

            try:
                data = self._query_subtask_metrics(
                    vid, ['busyTimeMsPerSecond', 'idleTimeMsPerSecond']
                )

                busy = data.get('busyTimeMsPerSecond', {})
                idle = data.get('idleTimeMsPerSecond', {})

                busy_avg = busy.get('avg', 0.0)
                idle_avg = idle.get('avg', 0.0)

                bp_ms = max(0.0, 1000.0 - busy_avg - idle_avg)
                bp_ratios.append(bp_ms / 1000.0)
            except Exception:
                bp_ratios.append(1.0)  # 查询失败视为不稳定

        return float(max(bp_ratios)) if bp_ratios else 1.0

    def close(self):
        self._session.close()
        logger.info("ObservationCollector 已关闭")

    def __repr__(self) -> str:
        return (f"ObservationCollector(rest={self.rest_url}, "
                f"job_id={self._job_id}, "
                f"vertices={len(self._vertex_map)})")
