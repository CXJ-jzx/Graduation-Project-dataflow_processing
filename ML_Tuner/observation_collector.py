#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import logging
import re
import requests
import yaml
import numpy as np
from typing import Optional

logger = logging.getLogger("ml_tuner.observation_collector")


class ObservationCollector:
    VERTEX_NAME_PATTERNS = {
        'source':     re.compile(r'Source.*RocketMQ', re.IGNORECASE),
        'filter':     re.compile(r'Hardware.*Filter', re.IGNORECASE),
        'signal':     re.compile(r'Signal.*Extraction', re.IGNORECASE),
        'feature':    re.compile(r'Feature.*Extraction', re.IGNORECASE),
        'grid':       re.compile(r'Grid.*Aggregation', re.IGNORECASE),
        'grid_sink':  re.compile(r'Sink.*GridSummary', re.IGNORECASE),
        'event_sink': re.compile(r'Sink.*EventFeature', re.IGNORECASE),
    }

    VERIFIED_METRIC_NAMES = {
        'source': {
            'numRecordsInPerSecond':  'Source__RocketMQ-Source.numRecordsInPerSecond',
            'numRecordsOutPerSecond': 'Source__RocketMQ-Source.numRecordsOutPerSecond',
            'numRecordsIn':           'Source__RocketMQ-Source.numRecordsIn',
            'numRecordsOut':          'Source__RocketMQ-Source.numRecordsOut',
        },
        'filter': {
            'numRecordsInPerSecond':  'Hardware-Filter.numRecordsInPerSecond',
            'numRecordsOutPerSecond': 'Hardware-Filter.numRecordsOutPerSecond',
        },
        'signal': {
            'numRecordsInPerSecond':  'Signal-Extraction.numRecordsInPerSecond',
            'numRecordsOutPerSecond': 'Signal-Extraction.numRecordsOutPerSecond',
        },
        'feature': {
            'numRecordsInPerSecond':  'Feature-Extraction.numRecordsInPerSecond',
            'numRecordsOutPerSecond': 'Feature-Extraction.numRecordsOutPerSecond',
            'l2_hit_rate':            'Feature-Extraction.l2_hit_rate',
            'l2_occupancy':           'Feature-Extraction.l2_occupancy',
        },
        'grid': {
            'numRecordsInPerSecond':  'Grid-Aggregation.numRecordsInPerSecond',
            'numRecordsOutPerSecond': 'Grid-Aggregation.numRecordsOutPerSecond',
            'l3_hit_rate':            'Grid-Aggregation.l3_hit_rate',
            'l3_occupancy':           'Grid-Aggregation.l3_occupancy',
        },
        'grid_sink': {
            'numRecordsInPerSecond':  'Sink__GridSummary-Sink.numRecordsInPerSecond',
            'numRecordsOutPerSecond': 'Sink__GridSummary-Sink.numRecordsOutPerSecond',
        },
        'event_sink': {
            'numRecordsInPerSecond':  'Sink__EventFeature-Sink.numRecordsInPerSecond',
            'numRecordsOutPerSecond': 'Sink__EventFeature-Sink.numRecordsOutPerSecond',
            'e2e_latency_p99_ms':     'Sink__EventFeature-Sink.e2e_latency_p99_ms',
            'latency_p99':            'Sink__EventFeature-Sink.latency_p99',
        },
    }

    def __init__(self, config_dict=None, config_path="ml_tuner_config.yaml"):
        if config_dict is None:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_dict = yaml.safe_load(f)

        flink_cfg = config_dict.get("flink", {})
        self._rest_url = flink_cfg.get("rest_url", "http://localhost:8081").rstrip("/")
        self._job_name = flink_cfg.get("job_name", "")

        mc = config_dict.get("metrics", {}).get("collection", {})
        self.window_size = int(mc.get("sample_count", 10))
        self.interval_seconds = int(mc.get("interval_seconds", 30))

        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})
        self._timeout = 10

        self._job_id = None
        self._vertex_map = {}
        self._vertex_names = {}
        self._vertex_parallelism = {}

        logger.info("ObservationCollector 初始化: rest=%s, job=%s, window=%d, interval=%ds",
                    self._rest_url, self._job_name,
                    self.window_size, self.interval_seconds)

    def discover(self) -> bool:
        try:
            resp = self._session.get(f"{self._rest_url}/jobs/overview", timeout=self._timeout)
            if not resp.ok:
                return False

            jobs = resp.json().get("jobs", [])
            running = [j for j in jobs if j.get("state") == "RUNNING"]
            if not running:
                return False

            if self._job_name:
                matched = [j for j in running if self._job_name.lower() in j.get("name", "").lower()]
                job = matched[0] if matched else running[0]
            else:
                job = running[0]

            self._job_id = job["jid"]
            logger.info("发现作业: jid=%s, name=%s", self._job_id, job.get("name", ""))

            resp2 = self._session.get(f"{self._rest_url}/jobs/{self._job_id}", timeout=self._timeout)
            if not resp2.ok:
                return False

            vertices = resp2.json().get("vertices", [])
            self._vertex_map.clear()
            self._vertex_names.clear()
            self._vertex_parallelism.clear()

            for v in vertices:
                vid = v["id"]
                vname = v.get("name", "")
                vpar = v.get("parallelism", 1)
                for op_key, pattern in self.VERTEX_NAME_PATTERNS.items():
                    if pattern.search(vname):
                        self._vertex_map[op_key] = vid
                        self._vertex_names[op_key] = vname
                        self._vertex_parallelism[op_key] = vpar
                        logger.info("  %s → %s (par=%d) [%s]",
                                    op_key, vid[:12], vpar, vname)
                        break

            logger.info("Vertex 发现: %d/%d 匹配",
                        len(self._vertex_map), len(self.VERTEX_NAME_PATTERNS))
            return len(self._vertex_map) >= 5
        except Exception as e:
            logger.error("作业发现失败: %s", e)
            return False

    def reset_cache(self):
        self._job_id = None
        self._vertex_map.clear()
        self._vertex_names.clear()
        self._vertex_parallelism.clear()
        logger.info("采集器缓存已清除")

    def _resolve_metric(self, op_key: str, generic_name: str) -> str:
        verified = self.VERIFIED_METRIC_NAMES.get(op_key, {})
        return verified.get(generic_name, generic_name)

    def collect_once(self) -> dict:
        if not self._job_id and not self.discover():
            return {}
        try:
            return self._collect_one_snapshot()
        except Exception as e:
            logger.warning("collect_once 失败: %s", e)
            return {}

    def collect_window(self) -> list:
        if not self._job_id and not self.discover():
            logger.error("collect_window: discover 失败")
            return []

        logger.info("开始窗口采集: %d 次, 间隔 %ds", self.window_size, self.interval_seconds)

        samples = []
        for i in range(self.window_size):
            try:
                s = self._collect_one_snapshot()
                if s:
                    samples.append(s)
            except Exception as e:
                logger.warning("第 %d 次采集异常: %s", i + 1, e)

            if i < self.window_size - 1:
                time.sleep(self.interval_seconds)

        if samples:
            input_rates = [s.get('avg_input_rate', 0.0) for s in samples if s.get('avg_input_rate', 0.0) > 0]
            peak_rate = float(max(input_rates)) if input_rates else 0.0
            if len(input_rates) >= 2:
                mean_r = float(np.mean(input_rates))
                cv = float(np.std(input_rates) / mean_r) if mean_r > 0 else 0.0
            else:
                cv = 0.0
            for s in samples:
                s['peak_input_rate'] = peak_rate
                s['input_cv'] = cv

        logger.info("窗口采集完成: %d/%d 次成功", len(samples), self.window_size)
        return samples

    def _collect_one_snapshot(self) -> dict:
        snapshot = {}

        # throughput
        source_vid = self._vertex_map.get('source')
        if source_vid:
            out_name = self._resolve_metric('source', 'numRecordsOutPerSecond')
            in_name = self._resolve_metric('source', 'numRecordsInPerSecond')
            data = self._query_subtask_metrics(source_vid, [in_name, out_name])
            in_rate = data.get(in_name, {}).get('sum', 0.0)
            out_rate = data.get(out_name, {}).get('sum', 0.0)
            snapshot['avg_input_rate'] = max(float(in_rate), float(out_rate))
        else:
            snapshot['avg_input_rate'] = 0.0

        # backpressure
        bp_ratios = []
        for op_key, vid in self._vertex_map.items():
            if op_key == 'source':
                continue
            bp = self._get_vertex_backpressure(vid)
            if bp is not None:
                bp_ratios.append(bp)
        snapshot['backpressure_ratio'] = float(max(bp_ratios)) if bp_ratios else 0.0

        # latency
        event_sink_vid = self._vertex_map.get('event_sink')
        snapshot['e2e_latency_p99_ms'] = self._get_e2e_latency(event_sink_vid) if event_sink_vid else 0.0
        snapshot['latency_p99'] = snapshot['e2e_latency_p99_ms']

        # checkpoint
        ckpt = self._query_checkpoints()
        snapshot['checkpoint_avg_dur'] = ckpt.get('avg_duration', 0.0)
        snapshot['checkpoint_max_dur'] = ckpt.get('max_duration', 0.0)
        snapshot['checkpoint_count'] = ckpt.get('completed', 0)
        snapshot['checkpoint_failed'] = ckpt.get('failed', 0)
        snapshot['checkpoint_size'] = ckpt.get('avg_size', 0.0)

        # busy
        busy_ratios = []
        for op_key, vid in self._vertex_map.items():
            if op_key == 'source':
                continue
            data = self._query_subtask_metrics(vid, ['busyTimeMsPerSecond'])
            busy_ms = data.get('busyTimeMsPerSecond', {}).get('avg', 0.0)
            if busy_ms is not None and float(busy_ms) > 0:
                busy_ratios.append(float(busy_ms) / 1000.0)
        snapshot['avg_busy_ratio'] = float(np.mean(busy_ratios)) if busy_ratios else 0.0
        snapshot['max_busy_ratio'] = float(max(busy_ratios)) if busy_ratios else 0.0

        # parallelism / heap / backlog
        snapshot['total_parallelism'] = float(sum(self._vertex_parallelism.values()))
        snapshot['heap_usage_ratio'] = self._get_heap_usage()
        snapshot['avg_backlog'] = self._estimate_avg_backlog()

        # L2/L3
        snapshot['l2_hit_rate'], snapshot['l2_occupancy'] = self._collect_l2()
        snapshot['l3_hit_rate'], snapshot['l3_occupancy'] = self._collect_l3()

        snapshot['active_node_count'] = 0.0
        snapshot['timestamp'] = time.time()
        return snapshot

    def _collect_l2(self):
        feature_vid = self._vertex_map.get('feature')
        if not feature_vid:
            return 0.0, 0.0
        l2_hit_name = self._resolve_metric('feature', 'l2_hit_rate')
        l2_occ_name = self._resolve_metric('feature', 'l2_occupancy')
        data = self._query_subtask_metrics(feature_vid, [l2_hit_name, l2_occ_name])
        return self._extract_gauge_value(data, l2_hit_name), self._extract_gauge_value(data, l2_occ_name)

    def _collect_l3(self):
        grid_vid = self._vertex_map.get('grid')
        if not grid_vid:
            return 0.0, 0.0
        l3_hit_name = self._resolve_metric('grid', 'l3_hit_rate')
        l3_occ_name = self._resolve_metric('grid', 'l3_occupancy')
        data = self._query_subtask_metrics(grid_vid, [l3_hit_name, l3_occ_name])
        return self._extract_gauge_value(data, l3_hit_name), self._extract_gauge_value(data, l3_occ_name)

    @staticmethod
    def _extract_gauge_value(data: dict, metric_name: str) -> float:
        entry = data.get(metric_name, {})
        for agg in ('avg', 'max', 'sum'):
            v = entry.get(agg)
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    pass
        return 0.0

    def _estimate_avg_backlog(self) -> float:
        vals = []
        for op_key in ['filter', 'signal', 'feature', 'grid']:
            vid = self._vertex_map.get(op_key)
            if not vid:
                continue
            data = self._query_subtask_metrics(vid, ['buffers.inputQueueLength'])
            inq = data.get('buffers.inputQueueLength', {}).get('avg', 0.0)
            if inq is not None and float(inq) > 0:
                vals.append(float(inq))
        return float(np.mean(vals)) if vals else 0.0

    def _get_e2e_latency(self, vertex_id: str) -> float:
        if not vertex_id:
            return 0.0
        e2e_name = 'Sink__EventFeature-Sink.e2e_latency_p99_ms'
        sink_name = 'Sink__EventFeature-Sink.latency_p99'
        data = self._query_subtask_metrics(vertex_id, [e2e_name, sink_name])
        e2e_val = self._extract_gauge_value(data, e2e_name)
        if e2e_val > 0:
            return e2e_val
        sink_val = self._extract_gauge_value(data, sink_name)
        if sink_val > 0:
            return sink_val / 1000.0
        return 0.0

    def _get_vertex_backpressure(self, vertex_id: str, max_retries: int = 3) -> Optional[float]:
        url = f"{self._rest_url}/jobs/{self._job_id}/vertices/{vertex_id}/backpressure"
        for attempt in range(max_retries):
            try:
                resp = self._session.get(url, timeout=self._timeout)
                if not resp.ok:
                    continue
                data = resp.json()
                status = data.get("status", "")
                if status == "ok":
                    subtasks = data.get("subtasks", [])
                    if subtasks:
                        ratios = [st.get("ratio", 0.0) for st in subtasks]
                        return float(max(ratios))
                    level = data.get("backpressureLevel", "ok")
                    return {"ok": 0.05, "low": 0.30, "high": 0.80}.get(level, 0.5)
                if status == "deprecated":
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    level = data.get("backpressureLevel", "ok")
                    return {"ok": 0.05, "low": 0.30, "high": 0.80}.get(level, 0.5)
            except Exception:
                pass
        return None

    def _get_heap_usage(self) -> float:
        try:
            resp = self._session.get(f"{self._rest_url}/taskmanagers", timeout=self._timeout)
            if not resp.ok:
                return 0.0
            tms = resp.json().get("taskmanagers", [])
            ratios = []
            for tm in tms:
                tm_id = tm.get("id", "")
                if not tm_id:
                    continue
                m_url = (f"{self._rest_url}/taskmanagers/{tm_id}/metrics"
                         f"?get=Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Max")
                m_resp = self._session.get(m_url, timeout=self._timeout)
                if not m_resp.ok:
                    continue
                metrics = {item['id']: float(item['value']) for item in m_resp.json()}
                used = metrics.get('Status.JVM.Memory.Heap.Used', 0)
                total = metrics.get('Status.JVM.Memory.Heap.Max', 0)
                if total > 0:
                    ratios.append(used / total)
            return float(np.mean(ratios)) if ratios else 0.0
        except Exception:
            return 0.0

    def _query_subtask_metrics(self, vertex_id: str, metric_names: list) -> dict:
        url = f"{self._rest_url}/jobs/{self._job_id}/vertices/{vertex_id}/subtasks/metrics"
        params = {"get": ",".join(metric_names)}
        try:
            resp = self._session.get(url, params=params, timeout=self._timeout)
            if not resp.ok:
                return {}
            data = resp.json()
            result = {}
            for item in data:
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

    def _query_checkpoints(self) -> dict:
        result = {
            'completed': 0, 'failed': 0,
            'avg_duration': 0.0, 'max_duration': 0.0,
            'avg_size': 0.0,
        }
        try:
            url = f"{self._rest_url}/jobs/{self._job_id}/checkpoints"
            resp = self._session.get(url, timeout=self._timeout)
            if not resp.ok:
                return result
            data = resp.json()
            counts = data.get("counts", {})
            result['completed'] = counts.get("completed", 0)
            result['failed'] = counts.get("failed", 0)
            summary = data.get("summary", {})
            e2e = summary.get("end_to_end_duration", {})
            size = summary.get("state_size", {})
            result['avg_duration'] = e2e.get("avg", 0.0)
            result['max_duration'] = e2e.get("max", 0.0)
            result['avg_size'] = size.get("avg", 0.0)
        except Exception:
            pass
        return result

    def wait_until_stable(self, bp_threshold: float = 0.50,
                          stable_required: int = 3,
                          check_interval: int = 15,
                          timeout: int = 300,
                          stop_flag=None) -> bool:
        if not self._job_id and not self.discover():
            return False

        logger.info("等待系统稳定 (bp < %.0f%%, 连续 %d 次, 超时 %ds)...",
                    bp_threshold * 100, stable_required, timeout)
        start = time.time()
        consecutive_ok = 0
        while time.time() - start < timeout:
            if stop_flag is not None:
                if callable(stop_flag) and stop_flag():
                    return False
                if hasattr(stop_flag, 'is_set') and stop_flag.is_set():
                    return False

            bp = self._quick_check_backpressure()
            if bp < bp_threshold:
                consecutive_ok += 1
                if consecutive_ok >= stable_required:
                    logger.info("✅ 系统已稳定: bp=%.2f, 耗时 %.0fs",
                                bp, time.time() - start)
                    return True
            else:
                consecutive_ok = 0

            time.sleep(check_interval)

        logger.warning("⚠️ 稳定性等待超时")
        return False

    def wait_until_data_flowing(self,
                                min_rate: float = 10.0,
                                stable_required: int = 2,
                                check_interval: int = 15,
                                timeout: int = 180,
                                stop_flag=None) -> bool:
        """
        双检测：
        1) realtime: numRecordsOutPerSecond
        2) accumulated fallback: write-records 差值
        """
        if not self._job_id and not self.discover():
            return False

        source_vid = self._vertex_map.get('source')
        if not source_vid:
            return True

        out_name = self._resolve_metric('source', 'numRecordsOutPerSecond')
        in_name = self._resolve_metric('source', 'numRecordsInPerSecond')

        logger.info("等待数据流就绪 (rate > %.0f, 连续 %d 次, 超时 %ds)...",
                    min_rate, stable_required, timeout)

        start = time.time()
        consecutive_ok = 0
        prev_acc = None
        prev_ts = None

        while time.time() - start < timeout:
            if stop_flag is not None:
                if callable(stop_flag) and stop_flag():
                    return False
                if hasattr(stop_flag, 'is_set') and stop_flag.is_set():
                    return False

            realtime_rate = 0.0
            acc_rate = 0.0

            try:
                data = self._query_subtask_metrics(source_vid, [in_name, out_name])
                realtime_rate = max(float(data.get(in_name, {}).get('sum', 0.0)),
                                    float(data.get(out_name, {}).get('sum', 0.0)))
            except Exception:
                pass

            try:
                acc = self._get_accumulated_records(source_vid)
                now = time.time()
                if acc is not None and prev_acc is not None and prev_ts is not None:
                    dt = now - prev_ts
                    if dt > 0:
                        acc_rate = (acc - prev_acc) / dt
                prev_acc = acc
                prev_ts = now
            except Exception:
                pass

            rate = max(realtime_rate, acc_rate)

            if rate >= min_rate:
                consecutive_ok += 1
                if consecutive_ok >= stable_required:
                    method = "realtime" if realtime_rate >= min_rate else "accumulated"
                    logger.info("✅ 数据流已就绪: rate=%.0f records/s, 耗时 %.0fs (%s)",
                                rate, time.time() - start, method)
                    return True
            else:
                consecutive_ok = 0

            time.sleep(check_interval)

        logger.warning("⚠️ 数据流就绪超时 (%.0fs), realtime/accumulated 均未达标",
                       time.time() - start)
        return False

    def _get_accumulated_records(self, vertex_id: str) -> Optional[int]:
        try:
            url = f"{self._rest_url}/jobs/{self._job_id}/vertices/{vertex_id}"
            resp = self._session.get(url, timeout=self._timeout)
            if resp.status_code != 200:
                return None
            data = resp.json()

            total = 0
            subtasks = data.get('subtasks', [])
            if subtasks:
                for st in subtasks:
                    metrics = st.get('metrics', {})
                    wr = metrics.get('write-records', metrics.get('read-records', 0))
                    total += int(wr)
                return total

            metrics = data.get('metrics', {})
            wr = metrics.get('write-records', metrics.get('read-records', 0))
            return int(wr) if wr else None
        except Exception:
            return None

    def _quick_check_backpressure(self) -> float:
        vals = []
        for op_key, vid in self._vertex_map.items():
            if op_key == 'source':
                continue
            bp = self._get_vertex_backpressure(vid, max_retries=2)
            if bp is not None:
                vals.append(bp)
        return float(max(vals)) if vals else 0.0

    def close(self):
        self._session.close()
        logger.info("ObservationCollector 已关闭")
