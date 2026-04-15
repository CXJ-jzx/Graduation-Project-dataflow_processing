#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ds2_experiment.py
DS2 弹性调度效果测试 —— 自动化实验主控（全算子监控版）

用法:
  python ds2_experiment.py E1          # 只跑 E1
  python ds2_experiment.py E1 C1       # 跑 E1 和 C1
  python ds2_experiment.py all         # 跑全部 5 组
"""

import os
import sys
import csv
import json
import time
import uuid
import shutil
import signal
import logging
import re
import subprocess
import threading
from datetime import datetime
from typing import Optional, Dict, List, Tuple

import yaml
import requests
import numpy as np
import paramiko

# ================================================================
# 日志
# ================================================================
LOG_FORMAT = "[%(asctime)s] %(levelname)-5s %(name)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("ds2_experiment")

# ================================================================
# 算子匹配
# ================================================================
VERTEX_PATTERNS = {
    'source':     re.compile(r'Source.*RocketMQ', re.IGNORECASE),
    'filter':     re.compile(r'Hardware.*Filter', re.IGNORECASE),
    'signal':     re.compile(r'Signal.*Extraction', re.IGNORECASE),
    'feature':    re.compile(r'Feature.*Extraction', re.IGNORECASE),
    'grid':       re.compile(r'Grid.*Aggregation', re.IGNORECASE),
    'grid_sink':  re.compile(r'Sink.*GridSummary', re.IGNORECASE),
    'event_sink': re.compile(r'Sink.*EventFeature', re.IGNORECASE),
}
OPERATOR_KEYS = ['source', 'filter', 'signal', 'feature', 'grid', 'grid_sink', 'event_sink']

# 全算子颜色映射（图表复用）
OP_COLORS = {
    'source':     '#1f77b4',  # 蓝
    'filter':     '#d62728',  # 红
    'signal':     '#ff7f0e',  # 橙
    'feature':    '#2ca02c',  # 绿
    'grid':       '#9467bd',  # 紫
    'grid_sink':  '#8c564b',  # 棕
    'event_sink': '#e377c2',  # 粉
}


class DS2Experiment:
    """单次实验的完整生命周期管理"""

    def __init__(self, global_cfg: dict, exp_name: str, exp_cfg: dict):
        self.global_cfg = global_cfg
        self.exp_name = exp_name
        self.exp_cfg = exp_cfg

        # Flink
        flink_cfg = global_cfg['flink']
        self.rest_url = flink_cfg['rest_url'].rstrip('/')
        self.jar_path = flink_cfg.get('jar_path')
        self.main_class = flink_cfg.get('main_class')

        # SSH
        ssh_cfg = global_cfg.get('ssh', {})
        self.ssh_host = ssh_cfg.get('host', '192.168.56.151')
        self.ssh_port = ssh_cfg.get('port', 22)
        self.ssh_user = ssh_cfg.get('user', 'root')
        self.ssh_password = ssh_cfg.get('password', '')

        # RocketMQ
        rmq_cfg = global_cfg.get('rocketmq', {})
        self.rmq_home = rmq_cfg.get('home', '/usr/local/rocketmq')
        self.rmq_cluster = rmq_cfg.get('cluster', 'RaftCluster')
        self.rmq_namesrv = rmq_cfg.get('namesrv_addr', '192.168.56.151:9876')
        self.consumer_group_prefix = rmq_cfg.get('consumer_group_prefix',
                                                  'seismic-flink-consumer-group')
        self.deregister_wait = int(rmq_cfg.get('deregister_wait_seconds', 15))

        # Producer
        prod_cfg = global_cfg.get('producer', {})
        self.producer_class = prod_cfg.get('main_class')
        self.producer_jar = os.path.abspath(prod_cfg.get('local_jar', ''))
        self.producer_work_dir = os.path.abspath(prod_cfg.get('work_dir', '..'))
        self.producer_namesrv = prod_cfg.get('namesrv_addr', self.rmq_namesrv)

        # DS2 Controller
        ds2_ctrl = global_cfg.get('ds2_controller', {})
        self.ds2_script = ds2_ctrl.get('script_path', 'ds2_controller.py')
        self.ds2_config = ds2_ctrl.get('config_path', 'ds2_config.yaml')

        # 实验参数
        self.ds2_enabled = exp_cfg.get('ds2_enabled', False)
        self.duration = exp_cfg.get('duration_seconds', 600)
        self.warmup_before_ds2 = exp_cfg.get('warmup_before_ds2', 30)
        self.drain_seconds = exp_cfg.get('drain_seconds', 60)
        self.parallelism = exp_cfg.get('parallelism', {})
        self.load_profile = exp_cfg.get('load_profile', [])
        self.collect_interval = global_cfg.get('metrics', {}).get('collect_interval', 10)

        # 运行时状态
        self._session = requests.Session()
        self._session.headers.update({'Accept': 'application/json'})
        self._timeout = 15
        self._jar_id: Optional[str] = None
        self._job_id: Optional[str] = None
        self._vertex_map: Dict[str, str] = {}
        self._vertex_parallelism: Dict[str, int] = {}
        self._consumer_group: Optional[str] = None
        self._producer_process: Optional[subprocess.Popen] = None
        self._producer_log_handle = None
        self._ds2_process: Optional[subprocess.Popen] = None
        self._ds2_log_handle = None
        self._running = True
        self._metrics_data: List[dict] = []
        self._scaling_events: List[dict] = []

        # RunID
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.run_id = f"{exp_name}_run01_{ts}"
        self.run_dir = os.path.join("experiments", self.run_id)

        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.warning("收到中断信号，准备清理...")
        self._running = False

    # ================================================================
    # 主入口
    # ================================================================
    def run(self) -> bool:
        """执行完整实验"""
        try:
            logger.info("=" * 70)
            logger.info("  实验 [%s] %s 开始", self.exp_name,
                        self.exp_cfg.get('name', ''))
            logger.info("  RunID: %s", self.run_id)
            logger.info("  DS2: %s", "开启" if self.ds2_enabled else "关闭")
            logger.info("  初始并行度: %s", self.parallelism)
            logger.info("  负载剖面: %s", [(p['name'], p['rate'], p['duration'])
                                          for p in self.load_profile])
            logger.info("  总时长: %ds + %ds排空", self.duration, self.drain_seconds)
            logger.info("=" * 70)

            # Step 0: 创建目录 + 清场
            self._setup_directories()
            self._cleanup_environment()

            # Step 1: 上传 JAR
            if not self._ensure_jar_uploaded():
                raise RuntimeError("JAR 上传失败")

            # Step 2: 提交 Flink 作业
            if not self._submit_flink_job():
                raise RuntimeError("Flink 作业提交失败")

            if not self._discover_vertices():
                raise RuntimeError("算子发现失败")

            # Step 3: 启动 Producer
            initial_rate = self.load_profile[0]['rate'] if self.load_profile else 5000
            if not self._start_producer(initial_rate):
                raise RuntimeError("Producer 启动失败")

            # Step 4: 预热
            logger.info("预热 %ds (等待数据流稳定)...", self.warmup_before_ds2)
            self._wait_seconds(self.warmup_before_ds2)

            # Step 5: 启动 DS2（如果需要）
            if self.ds2_enabled:
                self._start_ds2_controller()
                logger.info("DS2 Controller 已启动, 等待 15s 让它完成首轮采集...")
                self._wait_seconds(15)

            # Step 6: 进入主采集循环
            self._run_experiment_loop()

            # Step 7: 排空
            logger.info("停止 Producer, 排空 %ds...", self.drain_seconds)
            self._stop_producer()
            self._wait_seconds(self.drain_seconds)

            # Step 8: 保存结果
            self._save_results()
            self._generate_plots()

            logger.info("✅ 实验 [%s] 完成: %s", self.exp_name, self.run_dir)
            return True

        except Exception as e:
            logger.error("❌ 实验 [%s] 失败: %s", self.exp_name, e, exc_info=True)
            return False

        finally:
            self._full_cleanup()

    # ================================================================
    # Step 0: 目录与清场
    # ================================================================
    def _setup_directories(self):
        """创建实验目录结构"""
        for sub in ['meta', 'logs', 'metrics', 'results', 'figs']:
            os.makedirs(os.path.join(self.run_dir, sub), exist_ok=True)

        # 配置快照
        for f in ['ds2_config.yaml', 'ds2_controller.py',
                   'ds2_calibration_config.yaml', 'ds2_experiment_config.yaml']:
            if os.path.exists(f):
                shutil.copy2(f, os.path.join(self.run_dir, 'meta', f))

        # 保存实验元信息
        meta = {
            'run_id': self.run_id,
            'exp_name': self.exp_name,
            'exp_config': self.exp_cfg,
            'start_time': datetime.now().isoformat(),
            'parallelism': self.parallelism,
            'ds2_enabled': self.ds2_enabled,
        }
        with open(os.path.join(self.run_dir, 'meta', 'run_info.json'), 'w',
                  encoding='utf-8') as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)

        logger.info("实验目录已创建: %s", self.run_dir)

    def _cleanup_environment(self):
        """清场: 停旧进程, 取消旧作业"""
        logger.info("清场: 停止所有旧 Flink 作业...")
        try:
            jobs = self._get("/jobs")
            for j in jobs.get("jobs", []):
                if j["status"] == "RUNNING":
                    logger.info("  取消作业: %s", j["id"])
                    self._session.patch(f"{self.rest_url}/jobs/{j['id']}",
                                        timeout=self._timeout)
                    time.sleep(5)
        except Exception as e:
            logger.warning("清场异常(非致命): %s", e)

        # 确认无 RUNNING 作业
        time.sleep(5)
        try:
            jobs = self._get("/jobs")
            running = [j for j in jobs.get("jobs", []) if j["status"] == "RUNNING"]
            if running:
                logger.warning("仍有 %d 个 RUNNING 作业, 等待...", len(running))
                time.sleep(10)
        except Exception:
            pass

        logger.info("清场完成")

    # ================================================================
    # Step 2: 提交 Flink 作业
    # ================================================================
    def _submit_flink_job(self) -> bool:
        suffix = f"_exp_{uuid.uuid4().hex[:8]}"
        self._consumer_group = f"{self.consumer_group_prefix}{suffix}"

        parts = []
        for p_name, p_val in self.parallelism.items():
            parts.append(f"--{p_name} {p_val}")
        parts.append(f"--consumer-group-suffix {suffix}")
        parts.append("--consume-from-latest true")

        program_args = " ".join(parts)
        logger.info("提交 Flink 作业: %s", program_args)

        run_body = {
            "entryClass": self.main_class,
            "programArgs": program_args,
            "allowNonRestoredState": True,
        }
        try:
            resp = self._session.post(
                f"{self.rest_url}/jars/{self._jar_id}/run",
                json=run_body, timeout=120
            )
            if resp.status_code == 200:
                self._job_id = resp.json().get("jobid")
                logger.info("✅ 作业提交成功: %s", self._job_id)
            else:
                logger.error("❌ 提交失败 %d: %s", resp.status_code, resp.text[:300])
                return False
        except Exception as e:
            logger.error("提交异常: %s", e)
            return False

        # 等待 RUNNING
        for i in range(30):
            time.sleep(3)
            try:
                state = self._get(f"/jobs/{self._job_id}").get("state", "UNKNOWN")
                if state == "RUNNING":
                    logger.info("✅ 作业已 RUNNING")
                    return True
                if state in ["FAILED", "CANCELED", "CANCELLED"]:
                    logger.error("作业启动失败: %s", state)
                    return False
            except Exception:
                pass
        logger.error("等待 RUNNING 超时")
        return False

    def _discover_vertices(self) -> bool:
        try:
            detail = self._get(f"/jobs/{self._job_id}")
            self._vertex_map.clear()
            self._vertex_parallelism.clear()
            for v in detail.get("vertices", []):
                vid = v["id"]
                vname = v.get("name", "")
                vpar = v.get("parallelism", 1)
                for op_key, pattern in VERTEX_PATTERNS.items():
                    if pattern.search(vname):
                        self._vertex_map[op_key] = vid
                        self._vertex_parallelism[op_key] = vpar
                        break
            logger.info("算子发现: %d/%d  映射: %s",
                        len(self._vertex_map), len(VERTEX_PATTERNS),
                        {k: self._vertex_parallelism.get(k, '?')
                         for k in OPERATOR_KEYS if k in self._vertex_map})
            return len(self._vertex_map) >= 5
        except Exception as e:
            logger.error("算子发现失败: %s", e)
            return False

    # ================================================================
    # Step 3: Producer 管理
    # ================================================================
    def _start_producer(self, rate: int) -> bool:
        self._stop_producer()

        log_path = os.path.join(self.run_dir, 'logs', 'producer.log')
        cmd = [
            'java', '-cp', self.producer_jar,
            self.producer_class,
            str(rate),
            self.producer_namesrv,
        ]
        logger.info("启动 Producer: rate=%d r/s, cmd=%s", rate, ' '.join(cmd))

        try:
            self._producer_log_handle = open(log_path, 'w', encoding='utf-8')
            self._producer_process = subprocess.Popen(
                cmd, cwd=self.producer_work_dir,
                stdout=self._producer_log_handle, stderr=subprocess.STDOUT,
            )
            time.sleep(3)
            if self._producer_process.poll() is not None:
                logger.error("Producer 启动后立即退出: %d",
                             self._producer_process.returncode)
                return False
            logger.info("✅ Producer 已启动: PID=%d", self._producer_process.pid)
            return True
        except Exception as e:
            logger.error("Producer 启动失败: %s", e)
            return False

    def _stop_producer(self):
        if self._producer_process and self._producer_process.poll() is None:
            logger.info("停止 Producer: PID=%d", self._producer_process.pid)
            try:
                self._producer_process.terminate()
                self._producer_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._producer_process.kill()
                self._producer_process.wait(timeout=5)
            except Exception as e:
                logger.warning("停止 Producer 异常: %s", e)
        self._producer_process = None
        if self._producer_log_handle:
            try:
                self._producer_log_handle.close()
            except Exception:
                pass
            self._producer_log_handle = None

    def _switch_producer_rate(self, new_rate: int):
        """切换 Producer 速率（停旧启新）"""
        logger.info("切换 Producer 速率: → %d r/s", new_rate)
        self._stop_producer()
        time.sleep(2)

        log_path = os.path.join(self.run_dir, 'logs',
                                f'producer_{new_rate}.log')
        cmd = [
            'java', '-cp', self.producer_jar,
            self.producer_class,
            str(new_rate),
            self.producer_namesrv,
        ]
        try:
            self._producer_log_handle = open(log_path, 'w', encoding='utf-8')
            self._producer_process = subprocess.Popen(
                cmd, cwd=self.producer_work_dir,
                stdout=self._producer_log_handle, stderr=subprocess.STDOUT,
            )
            time.sleep(3)
            if self._producer_process.poll() is not None:
                logger.error("新 Producer 启动失败: %d",
                             self._producer_process.returncode)
                return False
            logger.info("✅ 新 Producer 已启动: PID=%d, rate=%d",
                        self._producer_process.pid, new_rate)
            return True
        except Exception as e:
            logger.error("切换 Producer 失败: %s", e)
            return False

    # ================================================================
    # Step 5: DS2 Controller 管理
    # ================================================================
    def _start_ds2_controller(self):
        if not self.ds2_enabled:
            return

        ds2_log_path = os.path.join(self.run_dir, 'logs', 'ds2_controller.log')
        cmd = [
            sys.executable,          # 当前 python
            self.ds2_script,
            '--config', self.ds2_config,
        ]
        logger.info("启动 DS2 Controller: %s", ' '.join(cmd))

        try:
            self._ds2_log_handle = open(ds2_log_path, 'w', encoding='utf-8')
            self._ds2_process = subprocess.Popen(
                cmd,
                stdout=self._ds2_log_handle,
                stderr=subprocess.STDOUT,
            )
            time.sleep(2)
            if self._ds2_process.poll() is not None:
                logger.error("DS2 Controller 启动后退出: %d",
                             self._ds2_process.returncode)
                return
            logger.info("✅ DS2 Controller 已启动: PID=%d", self._ds2_process.pid)
        except Exception as e:
            logger.error("DS2 Controller 启动失败: %s", e)

    def _stop_ds2_controller(self):
        if self._ds2_process and self._ds2_process.poll() is None:
            logger.info("停止 DS2 Controller: PID=%d", self._ds2_process.pid)
            try:
                self._ds2_process.terminate()
                self._ds2_process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self._ds2_process.kill()
                self._ds2_process.wait(timeout=5)
            except Exception as e:
                logger.warning("停止 DS2 异常: %s", e)
        self._ds2_process = None
        if self._ds2_log_handle:
            try:
                self._ds2_log_handle.close()
            except Exception:
                pass
            self._ds2_log_handle = None

    # ================================================================
    # Step 6: 主采集循环
    # ================================================================
    def _run_experiment_loop(self):
        """执行负载剖面 + 指标采集"""
        logger.info("进入主实验循环, 总时长 %ds, 采集间隔 %ds",
                    self.duration, self.collect_interval)

        # 构建阶段时间表
        phases = []
        t_acc = 0
        for p in self.load_profile:
            phases.append({
                'name': p['name'],
                'rate': p['rate'],
                'start': t_acc,
                'end': t_acc + p['duration'],
            })
            t_acc += p['duration']

        exp_start = time.time()
        current_phase_idx = 0
        current_rate = phases[0]['rate'] if phases else 5000
        sample_idx = 0

        # 记录阶段切换事件
        phase_marks = []
        phase_marks.append({
            'timestamp': exp_start,
            'elapsed': 0,
            'phase_name': phases[0]['name'] if phases else 'default',
            'rate': current_rate,
            'event': 'phase_start',
        })

        while self._running:
            elapsed = time.time() - exp_start
            if elapsed >= self.duration:
                logger.info("实验时间到 (%.0fs), 结束主循环", elapsed)
                break

            # 检查是否需要切换阶段
            if phases and current_phase_idx < len(phases) - 1:
                next_phase = phases[current_phase_idx + 1]
                if elapsed >= next_phase['start']:
                    current_phase_idx += 1
                    new_rate = next_phase['rate']

                    logger.info("=" * 50)
                    logger.info("  阶段切换: %s → %s (rate: %d → %d)",
                                phases[current_phase_idx - 1]['name'],
                                next_phase['name'],
                                current_rate, new_rate)
                    logger.info("=" * 50)

                    phase_marks.append({
                        'timestamp': time.time(),
                        'elapsed': elapsed,
                        'phase_name': next_phase['name'],
                        'rate': new_rate,
                        'event': 'phase_switch',
                    })

                    if new_rate != current_rate:
                        self._switch_producer_rate(new_rate)
                        current_rate = new_rate

            # 采集指标
            snapshot = self._collect_snapshot(sample_idx, elapsed, current_rate,
                                              phases[current_phase_idx]['name']
                                              if phases else 'default')
            if snapshot:
                self._metrics_data.append(snapshot)

                # ---- 实时打印：全算子 busy + 并行度 ----
                busy_parts = []
                for k in OPERATOR_KEYS:
                    b = snapshot.get(f'{k}_busy')
                    if b is not None and not (isinstance(b, float) and np.isnan(b)):
                        busy_parts.append(f"{k}={b:.2f}")
                    else:
                        busy_parts.append(f"{k}=N/A")
                busy_str = " | ".join(busy_parts)

                par_parts = []
                for k in OPERATOR_KEYS:
                    p_val = snapshot.get(f'p_{k}', 0)
                    if p_val and p_val > 0:
                        par_parts.append(f"{k}={p_val}")
                par_str = " ".join(par_parts)

                logger.info(
                    "[%3d] t=%4.0fs | src_out=%.0f/s | busy: %s",
                    sample_idx, elapsed,
                    snapshot.get('source_out_rate', 0), busy_str
                )
                logger.info(
                    "              | parallelism: %s", par_str
                )

            sample_idx += 1
            # 等待下一个采集点
            next_collect = exp_start + sample_idx * self.collect_interval
            sleep_time = next_collect - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)

        # 保存阶段标记
        self._save_phase_marks(phase_marks)

    def _collect_snapshot(self, idx: int, elapsed: float,
                          current_rate: int, phase_name: str) -> Optional[dict]:
        """采集一次全算子指标"""
        # 先刷新拓扑（并行度可能被DS2改了）
        self._refresh_parallelism()

        snapshot = {
            'sample_index': idx,
            'timestamp': time.time(),
            'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'elapsed_seconds': round(elapsed, 1),
            'target_rate': current_rate,
            'phase_name': phase_name,
            'run_id': self.run_id,
            'job_id': self._job_id or '',
        }

        for op_key in OPERATOR_KEYS:
            vid = self._vertex_map.get(op_key)
            if not vid:
                continue

            par = self._vertex_parallelism.get(op_key, 0)
            snapshot[f'p_{op_key}'] = par

            # busy
            busy = self._get_operator_busy(vid, par)
            snapshot[f'{op_key}_busy'] = busy if busy is not None else float('nan')

            # throughput
            in_rate, out_rate = self._get_operator_throughput(vid)
            snapshot[f'{op_key}_in_rate'] = in_rate
            snapshot[f'{op_key}_out_rate'] = out_rate

        return snapshot

    def _refresh_parallelism(self):
        """刷新并行度（DS2 可能已改变）"""
        try:
            # 先检查当前 job 是否还在
            jobs = self._get("/jobs")
            running_jobs = [j for j in jobs.get("jobs", [])
                            if j["status"] == "RUNNING"]

            if not running_jobs:
                logger.warning("无 RUNNING 作业")
                return

            # 取最新的 RUNNING 作业（DS2 重启后 job_id 会变）
            latest_job = running_jobs[0]
            new_job_id = latest_job["id"]
            if new_job_id != self._job_id:
                logger.info("检测到新作业: %s → %s (DS2 伸缩)",
                            self._job_id, new_job_id)
                self._job_id = new_job_id

                # 记录伸缩事件
                self._scaling_events.append({
                    'timestamp': time.time(),
                    'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'new_job_id': new_job_id,
                    'event': 'job_changed',
                })

                # 重新发现算子
                self._discover_vertices()

            else:
                # job 没变, 但并行度可能被 DS2 改了, 重新查
                detail = self._get(f"/jobs/{self._job_id}")
                for v in detail.get("vertices", []):
                    vname = v.get("name", "")
                    vpar = v.get("parallelism", 1)
                    for op_key, pattern in VERTEX_PATTERNS.items():
                        if pattern.search(vname):
                            old_par = self._vertex_parallelism.get(op_key, 0)
                            if vpar != old_par and old_par > 0:
                                logger.info("  并行度变化: %s %d → %d",
                                            op_key, old_par, vpar)
                                self._scaling_events.append({
                                    'timestamp': time.time(),
                                    'datetime': datetime.now().strftime(
                                        '%Y-%m-%d %H:%M:%S'),
                                    'operator': op_key,
                                    'old_parallelism': old_par,
                                    'new_parallelism': vpar,
                                    'event': 'parallelism_changed',
                                })
                            self._vertex_parallelism[op_key] = vpar
                            break
        except Exception as e:
            logger.warning("刷新并行度异常: %s", e)

    # ================================================================
    # 指标采集工具
    # ================================================================
    def _get_operator_busy(self, vertex_id: str, parallelism: int) -> Optional[float]:
        data = self._query_subtask_metrics(vertex_id, ['busyTimeMsPerSecond'])
        busy_ms = data.get('busyTimeMsPerSecond', {}).get('avg')
        if busy_ms is not None:
            try:
                val = float(busy_ms)
                if val >= 0:
                    return val / 1000.0
            except (ValueError, TypeError):
                pass

        # 逐 subtask
        values = []
        for idx in range(parallelism):
            try:
                url = (f"{self.rest_url}/jobs/{self._job_id}"
                       f"/vertices/{vertex_id}/subtasks/{idx}/metrics"
                       f"?get=busyTimeMsPerSecond")
                resp = self._session.get(url, timeout=self._timeout)
                if resp.ok:
                    for item in resp.json():
                        if item.get('id') == 'busyTimeMsPerSecond':
                            values.append(float(item.get('value', 0)) / 1000.0)
                            break
            except Exception:
                pass
        if values:
            return float(np.mean(values))

        # idle 反算
        data2 = self._query_subtask_metrics(vertex_id, ['idleTimeMsPerSecond'])
        idle_ms = data2.get('idleTimeMsPerSecond', {}).get('avg')
        if idle_ms is not None:
            try:
                return max(0.0, 1.0 - float(idle_ms) / 1000.0)
            except (ValueError, TypeError):
                pass
        return None

    def _get_operator_throughput(self, vertex_id: str) -> Tuple[float, float]:
        data = self._query_subtask_metrics(
            vertex_id, ['numRecordsInPerSecond', 'numRecordsOutPerSecond'])
        in_rate = float(data.get('numRecordsInPerSecond', {}).get('sum', 0.0))
        out_rate = float(data.get('numRecordsOutPerSecond', {}).get('sum', 0.0))
        return in_rate, out_rate

    def _query_subtask_metrics(self, vertex_id: str, metric_names: list) -> dict:
        url = (f"{self.rest_url}/jobs/{self._job_id}"
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

    # ================================================================
    # 结果保存
    # ================================================================
    def _save_results(self):
        """保存所有实验数据"""
        # 1. 时间序列 CSV
        if self._metrics_data:
            csv_path = os.path.join(self.run_dir, 'metrics', 'timeline_metrics.csv')
            # 收集所有可能的字段名（不同轮次可能因为算子发现差异导致字段不同）
            all_fields = set()
            for row in self._metrics_data:
                all_fields.update(row.keys())
            fieldnames = sorted(all_fields)

            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames,
                                        extrasaction='ignore')
                writer.writeheader()
                for row in self._metrics_data:
                    writer.writerow(row)
            logger.info("✅ 时间序列已保存: %s (%d 条)", csv_path,
                        len(self._metrics_data))

        # 2. 伸缩事件 CSV
        if self._scaling_events:
            all_fields = set()
            for row in self._scaling_events:
                all_fields.update(row.keys())
            fieldnames = sorted(all_fields)

            events_path = os.path.join(self.run_dir, 'metrics',
                                       'scaling_events.csv')
            with open(events_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames,
                                        extrasaction='ignore')
                writer.writeheader()
                for row in self._scaling_events:
                    writer.writerow(row)
            logger.info("✅ 伸缩事件已保存: %s (%d 条)", events_path,
                        len(self._scaling_events))

        # 3. 汇总 JSON
        summary = self._compute_summary()
        summary_path = os.path.join(self.run_dir, 'results', 'summary.json')
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        logger.info("✅ 汇总已保存: %s", summary_path)

        # 4. 归档 ds2_logs（如果 DS2 运行了）
        if self.ds2_enabled and os.path.exists('ds2_logs'):
            dst = os.path.join(self.run_dir, 'logs', 'ds2_logs_archive')
            shutil.copytree('ds2_logs', dst, dirs_exist_ok=True)
            logger.info("✅ DS2 日志已归档: %s", dst)

    def _save_phase_marks(self, marks: list):
        if not marks:
            return
        path = os.path.join(self.run_dir, 'metrics', 'phase_marks.csv')
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=marks[0].keys())
            writer.writeheader()
            writer.writerows(marks)
        logger.info("✅ 阶段标记已保存: %s", path)

    def _compute_summary(self) -> dict:
        """计算汇总指标 —— 全算子版"""
        if not self._metrics_data:
            return {}

        # 取有效数据（跳过前 3 个采样）
        valid = self._metrics_data[3:] if len(self._metrics_data) > 5 \
            else self._metrics_data

        summary = {
            'run_id': self.run_id,
            'exp_name': self.exp_name,
            'ds2_enabled': self.ds2_enabled,
            'total_samples': len(self._metrics_data),
            'total_scaling_events': len(self._scaling_events),
        }

        # 全算子 busy 统计
        for op_key in OPERATOR_KEYS:
            values = [s.get(f'{op_key}_busy', 0) for s in valid
                      if s.get(f'{op_key}_busy') is not None
                      and not np.isnan(s.get(f'{op_key}_busy', float('nan')))]
            if values:
                summary[f'{op_key}_busy_mean'] = round(float(np.mean(values)), 4)
                summary[f'{op_key}_busy_std'] = round(float(np.std(values)), 4)
                summary[f'{op_key}_busy_max'] = round(float(np.max(values)), 4)

        # 全算子并行度变化范围
        for op_key in OPERATOR_KEYS:
            pars = [s.get(f'p_{op_key}', 0) for s in valid
                    if s.get(f'p_{op_key}', 0) > 0]
            if pars:
                summary[f'{op_key}_p_min'] = int(min(pars))
                summary[f'{op_key}_p_max'] = int(max(pars))
                summary[f'{op_key}_p_final'] = int(pars[-1])

        # 全算子吞吐统计
        for op_key in OPERATOR_KEYS:
            in_rates = [s.get(f'{op_key}_in_rate', 0) for s in valid
                        if s.get(f'{op_key}_in_rate', 0) > 0]
            out_rates = [s.get(f'{op_key}_out_rate', 0) for s in valid
                         if s.get(f'{op_key}_out_rate', 0) > 0]
            if in_rates:
                summary[f'{op_key}_in_rate_mean'] = round(float(np.mean(in_rates)), 1)
            if out_rates:
                summary[f'{op_key}_out_rate_mean'] = round(float(np.mean(out_rates)), 1)

        # 输入速率统计（保留向后兼容）
        in_rates = [s.get('source_out_rate', 0) for s in valid
                    if s.get('source_out_rate', 0) > 0]
        if in_rates:
            summary['input_rate_mean'] = round(float(np.mean(in_rates)), 1)

        return summary

    # ================================================================
    # 可视化
    # ================================================================
    def _generate_plots(self):
        """生成三张标准图 —— 全算子版"""
        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
            plt.rcParams['axes.unicode_minus'] = False
        except ImportError:
            logger.warning("matplotlib 未安装, 跳过画图")
            return

        if not self._metrics_data:
            logger.warning("无指标数据, 跳过画图")
            return

        elapsed = [s['elapsed_seconds'] for s in self._metrics_data]
        exp_start_ts = self._metrics_data[0]['timestamp']

        # ================================================================
        # 图1: 全算子并行度时间线
        # ================================================================
        fig1, ax1 = plt.subplots(figsize=(14, 6))
        max_p_seen = 1
        for op_key in OPERATOR_KEYS:
            pars = [s.get(f'p_{op_key}', 0) for s in self._metrics_data]
            if any(p > 0 for p in pars):
                ax1.step(elapsed, pars, where='post',
                         label=f'{op_key}',
                         color=OP_COLORS.get(op_key, 'gray'),
                         linewidth=2, alpha=0.8)
                local_max = max(p for p in pars if p > 0)
                if local_max > max_p_seen:
                    max_p_seen = local_max

        for evt in self._scaling_events:
            evt_elapsed = evt['timestamp'] - exp_start_ts
            ax1.axvline(x=evt_elapsed, color='gray', linestyle='--',
                        alpha=0.4, linewidth=0.8)

        ax1.set_xlabel('Time (s)')
        ax1.set_ylabel('Parallelism')
        ax1.set_title(f'[{self.exp_name}] All Operators Parallelism '
                      f'(DS2 {"ON" if self.ds2_enabled else "OFF"})')
        ax1.legend(loc='upper right', ncol=2, fontsize=9)
        ax1.grid(True, alpha=0.3)
        ax1.set_ylim(0, max_p_seen + 2)
        fig1.tight_layout()
        fig1.savefig(os.path.join(self.run_dir, 'figs',
                                  'fig1_parallelism_timeline.png'), dpi=150)
        plt.close(fig1)
        logger.info("✅ 图1 (全算子并行度) 已保存")

        # ================================================================
        # 图2: 全算子 busy 时间线 (分两组子图)
        # ================================================================
        fig2, (ax2a, ax2b) = plt.subplots(2, 1, figsize=(14, 9), sharex=True)

        # 上子图：计算密集算子
        compute_ops = ['source', 'filter', 'signal', 'feature']
        for op_key in compute_ops:
            values = [s.get(f'{op_key}_busy', 0) for s in self._metrics_data]
            ax2a.plot(elapsed, values, label=f'{op_key}',
                      color=OP_COLORS.get(op_key, 'gray'),
                      linewidth=1.5)

        ax2a.axhspan(0.35, 0.65, alpha=0.08, color='green')
        ax2a.axhline(y=1.0 / 1.5, color='orange', linestyle=':',
                     alpha=0.5, label='Target util (66.7%)')
        ax2a.set_ylabel('Busy Ratio')
        ax2a.set_title(f'[{self.exp_name}] Compute Operators Busy')
        ax2a.legend(loc='upper right', fontsize=9)
        ax2a.grid(True, alpha=0.3)
        ax2a.set_ylim(0, 1.05)

        # 标注伸缩事件
        for evt in self._scaling_events:
            evt_elapsed = evt['timestamp'] - exp_start_ts
            ax2a.axvline(x=evt_elapsed, color='purple', linestyle='--',
                         alpha=0.3, linewidth=0.8)

        # 下子图：IO / Sink 算子
        io_ops = ['grid', 'grid_sink', 'event_sink']
        for op_key in io_ops:
            values = [s.get(f'{op_key}_busy', 0) for s in self._metrics_data]
            ax2b.plot(elapsed, values, label=f'{op_key}',
                      color=OP_COLORS.get(op_key, 'gray'),
                      linewidth=1.5)

        ax2b.axhspan(0.35, 0.65, alpha=0.08, color='green')
        ax2b.axhline(y=1.0 / 1.5, color='orange', linestyle=':',
                     alpha=0.5, label='Target util (66.7%)')
        ax2b.set_xlabel('Time (s)')
        ax2b.set_ylabel('Busy Ratio')
        ax2b.set_title(f'[{self.exp_name}] IO/Sink Operators Busy')
        ax2b.legend(loc='upper right', fontsize=9)
        ax2b.grid(True, alpha=0.3)
        ax2b.set_ylim(0, 1.05)

        for evt in self._scaling_events:
            evt_elapsed = evt['timestamp'] - exp_start_ts
            ax2b.axvline(x=evt_elapsed, color='purple', linestyle='--',
                         alpha=0.3, linewidth=0.8)

        fig2.tight_layout()
        fig2.savefig(os.path.join(self.run_dir, 'figs',
                                  'fig2_busy_timeline.png'), dpi=150)
        plt.close(fig2)
        logger.info("✅ 图2 (全算子busy) 已保存")

        # ================================================================
        # 图3: 吞吐量 + 伸缩事件 (分两组子图)
        # ================================================================
        fig3, (ax3a, ax3b) = plt.subplots(2, 1, figsize=(14, 9), sharex=True)

        # 上子图：计算算子输出吞吐
        for op_key in ['source', 'filter', 'signal', 'feature']:
            out = [s.get(f'{op_key}_out_rate', 0) for s in self._metrics_data]
            ax3a.plot(elapsed, out, label=f'{op_key} out',
                      color=OP_COLORS.get(op_key, 'gray'),
                      linewidth=1.5)

        target_rates = [s.get('target_rate', 0) for s in self._metrics_data]
        ax3a.plot(elapsed, target_rates, label='Target rate',
                  color='black', linestyle='--', linewidth=1, alpha=0.6)

        for evt in self._scaling_events:
            evt_elapsed = evt['timestamp'] - exp_start_ts
            ax3a.axvline(x=evt_elapsed, color='purple', linestyle='--',
                         alpha=0.4, linewidth=1)

        ax3a.set_ylabel('Records/s')
        ax3a.set_title(f'[{self.exp_name}] Compute Operators Output & Scaling')
        ax3a.legend(loc='upper right', fontsize=9)
        ax3a.grid(True, alpha=0.3)

        # 下子图：下游算子输入吞吐
        for op_key in ['grid', 'grid_sink', 'event_sink']:
            inp = [s.get(f'{op_key}_in_rate', 0) for s in self._metrics_data]
            ax3b.plot(elapsed, inp, label=f'{op_key} in',
                      color=OP_COLORS.get(op_key, 'gray'),
                      linewidth=1.5)

        for evt in self._scaling_events:
            evt_elapsed = evt['timestamp'] - exp_start_ts
            ax3b.axvline(x=evt_elapsed, color='purple', linestyle='--',
                         alpha=0.4, linewidth=1)

        ax3b.set_xlabel('Time (s)')
        ax3b.set_ylabel('Records/s')
        ax3b.set_title(f'[{self.exp_name}] Downstream Input Throughput')
        ax3b.legend(loc='upper right', fontsize=9)
        ax3b.grid(True, alpha=0.3)

        fig3.tight_layout()
        fig3.savefig(os.path.join(self.run_dir, 'figs',
                                  'fig3_throughput_events.png'), dpi=150)
        plt.close(fig3)
        logger.info("✅ 图3 (全算子吞吐) 已保存")

    # ================================================================
    # JAR 上传
    # ================================================================
    def _ensure_jar_uploaded(self) -> bool:
        try:
            jars = self._get("/jars")
            for f in jars.get("files", []):
                if 'rocketmq-demo' in f['name'].lower() or 'seismic' in f['name'].lower():
                    self._jar_id = f["id"]
                    logger.info("找到已上传 JAR: %s", self._jar_id)
                    return True
        except Exception:
            pass

        logger.info("上传 JAR: %s", self.jar_path)
        try:
            cmd = (f'curl -s -X POST -H "Expect:" '
                   f'-F "jarfile=@{self.jar_path}" '
                   f'http://localhost:8081/jars/upload')
            out = self._ssh_exec(cmd)
            if '"status":"success"' in out:
                jars = self._get("/jars")
                for f in jars.get("files", []):
                    if 'rocketmq-demo' in f['name'].lower():
                        self._jar_id = f["id"]
                        logger.info("✅ JAR 上传成功: %s", self._jar_id)
                        return True
            logger.error("JAR 上传失败: %s", out[:300])
            return False
        except Exception as e:
            logger.error("JAR 上传异常: %s", e)
            return False

    # ================================================================
    # 清理
    # ================================================================
    def _full_cleanup(self):
        logger.info("开始全面清理...")

        self._stop_producer()
        self._stop_ds2_controller()

        if self._job_id:
            try:
                self._session.patch(f"{self.rest_url}/jobs/{self._job_id}",
                                    timeout=self._timeout)
                logger.info("已取消 Flink 作业: %s", self._job_id)
            except Exception:
                pass

        # 也取消可能由 DS2 重启的新作业
        try:
            jobs = self._get("/jobs")
            for j in jobs.get("jobs", []):
                if j["status"] == "RUNNING":
                    self._session.patch(f"{self.rest_url}/jobs/{j['id']}",
                                        timeout=self._timeout)
                    time.sleep(2)
        except Exception:
            pass

        if self._consumer_group:
            try:
                cmd = (f"sh {self.rmq_home}/bin/mqadmin deleteSubGroup "
                       f"-n {self.rmq_namesrv} "
                       f"-c {self.rmq_cluster} "
                       f"-g {self._consumer_group} || true")
                self._ssh_exec(cmd)
                logger.info("已清理 Consumer Group: %s", self._consumer_group)
            except Exception:
                pass

        self._session.close()
        logger.info("✅ 清理完成")

    # ================================================================
    # 工具
    # ================================================================
    def _get(self, path: str) -> dict:
        resp = self._session.get(f"{self.rest_url}{path}", timeout=self._timeout)
        resp.raise_for_status()
        return resp.json()

    def _ssh_exec(self, cmd: str) -> str:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(self.ssh_host, self.ssh_port,
                       self.ssh_user, self.ssh_password, timeout=10)
        _, stdout, stderr = client.exec_command(cmd, timeout=120)
        out = stdout.read().decode('utf-8', errors='replace').strip()
        err = stderr.read().decode('utf-8', errors='replace').strip()
        client.close()
        return out if out else err

    def _wait_seconds(self, seconds: int):
        for i in range(seconds):
            if not self._running:
                break
            time.sleep(1)
            if (i + 1) % 15 == 0:
                logger.info("  等待中... %d/%ds", i + 1, seconds)


# ================================================================
# 入口
# ================================================================
def main():
    config_path = "ds2_experiment_config.yaml"
    if not os.path.exists(config_path):
        print(f"配置文件不存在: {config_path}")
        sys.exit(1)

    with open(config_path, 'r', encoding='utf-8') as f:
        global_cfg = yaml.safe_load(f)

    # 解析命令行参数
    if len(sys.argv) < 2:
        print("用法: python ds2_experiment.py <实验名> [实验名2 ...]")
        print("  可选: E1, C1, E2, C2, E3, all")
        print("  示例: python ds2_experiment.py E1")
        print("  示例: python ds2_experiment.py E1 C1")
        print("  示例: python ds2_experiment.py all")
        sys.exit(1)

    all_experiments = global_cfg.get('experiments', {})
    requested = sys.argv[1:]

    if 'all' in requested:
        # 推荐执行顺序
        requested = ['E1', 'C1', 'E2', 'C2', 'E3']

    # 验证实验名
    for name in requested:
        if name not in all_experiments:
            print(f"未知实验: {name}, 可选: {list(all_experiments.keys())}")
            sys.exit(1)

    # 依次执行
    results = {}
    for name in requested:
        exp_cfg = all_experiments[name]

        logger.info("")
        logger.info("╔══════════════════════════════════════════════════╗")
        logger.info("║  准备执行实验: %-36s║", f"{name} - {exp_cfg.get('name', '')}")
        logger.info("╚══════════════════════════════════════════════════╝")

        # 实验间间隔（非首个实验前等 30s）
        if name != requested[0]:
            logger.info("实验间等待 30s...")
            time.sleep(30)

        experiment = DS2Experiment(global_cfg, name, exp_cfg)
        success = experiment.run()
        results[name] = {
            'success': success,
            'run_id': experiment.run_id,
            'run_dir': experiment.run_dir,
        }

    # 最终汇总
    logger.info("")
    logger.info("=" * 60)
    logger.info("  全部实验执行结果")
    logger.info("=" * 60)
    for name, r in results.items():
        status = "✅ 成功" if r['success'] else "❌ 失败"
        logger.info("  %s: %s → %s", name, status, r['run_dir'])
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
