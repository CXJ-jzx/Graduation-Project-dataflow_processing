#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
calibrate_source.py - Source 算子单实例容量标定脚本

全自动执行：Producer 作为子进程管理，无需人工介入。
每轮用不同 Source 并行度提交 Flink 作业，Producer 打满，采集稳态吞吐。

使用:
  python calibrate_source.py
  python calibrate_source.py ds2_calibration_config.yaml
  python calibrate_source.py ds2_calibration_config.yaml --parallelism-list 1 2 3
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
from typing import Dict, List, Optional

import requests
import paramiko
import yaml
import numpy as np

# ============================================================
# 日志
# ============================================================
LOG_FORMAT = "[%(asctime)s] %(levelname)-5s %(name)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("calibrate_source")

# 算子匹配（复用 ds2_calibration 的定义）
SOURCE_PATTERN = re.compile(r'Source.*RocketMQ', re.IGNORECASE)


# ============================================================
# 配置
# ============================================================
@dataclass
class SourceCalibrationConfig:

    # Flink
    flink_rest_url: str = ""
    flink_home: str = ""
    jar_path: str = ""
    main_class: str = ""

    # SSH
    ssh_host: str = ""
    ssh_port: int = 22
    ssh_user: str = "root"
    ssh_password: str = ""

    # RocketMQ
    rocketmq_home: str = ""
    rocketmq_cluster: str = "RaftCluster"
    rocketmq_namesrv: str = ""
    consumer_group_prefix: str = "seismic-flink-consumer-group"
    deregister_wait: int = 15
    cleanup_enabled: bool = True

    # Producer
    producer_class: str = ""
    producer_jar: str = ""
    producer_work_dir: str = ""
    producer_namesrv: str = ""
    producer_rate: int = 20000      # 标定时 Producer 速率（打满 Source）

    # 标定参数
    parallelism_list: List[int] = field(default_factory=lambda: [1, 2, 3, 4])
    warmup_seconds: int = 90
    sample_seconds: int = 60
    sample_interval: float = 2.0
    drain_seconds: int = 15

    # 下游算子并行度（足够大，消除反压）
    downstream_parallelism: Dict[str, int] = field(default_factory=lambda: {
        "p-filter": 4,
        "p-signal": 4,
        "p-feature": 4,
        "p-grid": 4,
        "p-sink": 2,
    })

    # 超时
    cancel_timeout: int = 60
    submit_timeout: int = 120
    job_start_timeout: int = 90

    # 输出
    output_dir: str = "calibration_results"

    # 运行时
    jar_id: str = ""

    @classmethod
    def from_yaml(cls, path: str) -> 'SourceCalibrationConfig':
        """从 YAML 加载（兼容 ds2_calibration_config.yaml 格式）"""
        config = cls()

        if not os.path.exists(path):
            logger.error("配置文件不存在: %s", path)
            sys.exit(1)

        with open(path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)

        # Flink
        flink_cfg = cfg.get('flink', {})
        config.flink_rest_url = flink_cfg.get('rest_url', '').rstrip('/')
        config.flink_home = flink_cfg.get('flink_home', '')
        config.jar_path = flink_cfg.get('jar_path', '')
        config.main_class = flink_cfg.get('main_class', '')

        # SSH
        ssh_cfg = cfg.get('ssh', {})
        config.ssh_host = ssh_cfg.get('host', '192.168.56.151')
        config.ssh_port = ssh_cfg.get('port', 22)
        config.ssh_user = ssh_cfg.get('user', 'root')
        config.ssh_password = ssh_cfg.get('password', '')

        # RocketMQ
        rmq_cfg = cfg.get('rocketmq', {})
        config.rocketmq_home = rmq_cfg.get('home', '/usr/local/rocketmq')
        config.rocketmq_cluster = rmq_cfg.get('cluster', 'RaftCluster')
        config.rocketmq_namesrv = rmq_cfg.get('namesrv_addr', '192.168.56.151:9876')
        config.consumer_group_prefix = rmq_cfg.get(
            'consumer_group_prefix', 'seismic-flink-consumer-group')
        config.deregister_wait = int(rmq_cfg.get('deregister_wait_seconds', 15))
        config.cleanup_enabled = bool(rmq_cfg.get('cleanup_old_consumer_group', True))

        # Producer
        prod_cfg = cfg.get('producer', {})
        config.producer_class = prod_cfg.get('main_class', '')
        config.producer_jar = os.path.abspath(prod_cfg.get('local_jar', ''))
        config.producer_work_dir = os.path.abspath(prod_cfg.get('work_dir', '..'))
        config.producer_namesrv = prod_cfg.get('namesrv_addr', config.rocketmq_namesrv)

        # 标定专用配置（在 calibration 节或 source_calibration 节下）
        cal_cfg = cfg.get('source_calibration', cfg.get('calibration', {}))
        if 'parallelism_list' in cal_cfg:
            config.parallelism_list = cal_cfg['parallelism_list']
        config.warmup_seconds = int(cal_cfg.get('warmup_seconds', 90))
        config.sample_seconds = int(cal_cfg.get('observe_seconds',
                                                 cal_cfg.get('sample_seconds', 60)))
        config.sample_interval = float(cal_cfg.get('observe_interval',
                                                    cal_cfg.get('sample_interval', 2.0)))
        config.drain_seconds = int(cal_cfg.get('drain_seconds', 15))
        config.producer_rate = int(cal_cfg.get('producer_rate', 20000))
        config.output_dir = cal_cfg.get('output_dir', 'calibration_results')

        # 下游并行度：从 calibration.default_parallelism 中提取非 source 的
        default_par = cal_cfg.get('default_parallelism', {})
        for k, v in default_par.items():
            if k != 'p-source':
                config.downstream_parallelism[k] = v

        # 超时
        timeouts = cfg.get('timeouts', {})
        config.cancel_timeout = timeouts.get('cancel', config.cancel_timeout)
        config.submit_timeout = timeouts.get('submit', config.submit_timeout)

        return config


# ============================================================
# SSH 执行器
# ============================================================
class SSHExecutor:

    def __init__(self, host: str, port: int, user: str, password: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

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

    def __init__(self, config: SourceCalibrationConfig):
        self.config = config
        self.rest_url = config.flink_rest_url
        self._session = requests.Session()
        self._session.headers.update({'Accept': 'application/json'})
        self._timeout = 15

    def _get(self, path: str) -> dict:
        url = f"{self.rest_url}{path}"
        resp = self._session.get(url, timeout=self._timeout)
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
        logger.error("无法连接 Flink 集群: %s", self.rest_url)
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

        logger.info("通过 SSH 上传 JAR: %s", self.config.jar_path)
        try:
            upload_cmd = (
                f'curl -s -X POST -H "Expect:" '
                f'-F "jarfile=@{self.config.jar_path}" '
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

        for i in range(self.config.cancel_timeout // 2):
            time.sleep(2)
            try:
                state = self._get(f"/jobs/{job_id}").get("state", "UNKNOWN")
                if state not in ["RUNNING", "CANCELLING", "FAILING"]:
                    logger.info("作业已停止: %s", state)
                    return True
                if i % 5 == 0:
                    logger.debug("等待停止: [%ds] %s", i * 2, state)
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
        logger.info("  entryClass : %s", self.config.main_class)
        logger.info("  programArgs: %s", program_args)

        run_body = {
            "entryClass": self.config.main_class,
            "programArgs": program_args,
            "allowNonRestoredState": True,
        }

        url = f"{self.rest_url}/jars/{jar_id}/run"
        try:
            resp = self._session.post(url, json=run_body,
                                       timeout=self.config.submit_timeout)
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

    def wait_job_running(self, job_id: str, timeout: int = 90) -> bool:
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
                if i % 5 == 0:
                    logger.debug("等待 RUNNING: [%ds] %s", i * 3, state)
            except Exception:
                pass
        logger.error("等待 RUNNING 超时 (%ds)", timeout)
        return False

    def wait_slots_available(self, min_slots: int = 4, timeout: int = 30):
        for _ in range(timeout // 2):
            time.sleep(2)
            try:
                overview = self._get("/overview")
                available = overview.get("slots-available", 0)
                if available >= min_slots:
                    return
            except Exception:
                pass
        logger.warning("等待 slot 释放超时，继续尝试提交")

    def find_source_vertex(self, job_id: str) -> Optional[dict]:
        try:
            data = self._get(f"/jobs/{job_id}")
            for v in data.get("vertices", []):
                if SOURCE_PATTERN.search(v.get("name", "")):
                    return v
            for v in data.get("vertices", []):
                if "source" in v.get("name", "").lower():
                    logger.warning("精确匹配失败，降级: %s", v["name"])
                    return v
        except Exception:
            pass
        return None

    def get_source_output_rate(self, job_id: str) -> Optional[float]:
        source = self.find_source_vertex(job_id)
        if not source:
            return None
        try:
            data = self._get(
                f"/jobs/{job_id}/vertices/{source['id']}"
                f"/metrics?get=numRecordsOutPerSecond")
            for m in data:
                if m.get("id") == "numRecordsOutPerSecond":
                    val = float(m["value"])
                    if not (math.isnan(val) or math.isinf(val)):
                        return val
        except Exception:
            pass
        return None

    def get_source_parallelism(self, job_id: str) -> int:
        source = self.find_source_vertex(job_id)
        return source.get("parallelism", 0) if source else 0

    def close(self):
        self._session.close()


# ============================================================
# Producer 管理器（自动子进程，与 ds2_calibration 完全一致）
# ============================================================
class ProducerManager:

    def __init__(self, config: SourceCalibrationConfig):
        self.config = config
        self._process: Optional[subprocess.Popen] = None
        self._log_handle = None
        self._log_dir = os.path.join(config.output_dir, "logs")

    def start(self, rate: int) -> bool:
        """启动指定速率的 Producer"""
        self.stop()

        jar = self.config.producer_jar
        if not os.path.exists(jar):
            logger.error("Producer JAR 不存在: %s", jar)
            return False

        os.makedirs(self._log_dir, exist_ok=True)
        log_path = os.path.join(self._log_dir,
                                f"producer_source_cal_{rate}.log")

        # 与 ds2_calibration 相同：arg[0]=rate, arg[1]=namesrvAddr
        cmd = [
            'java', '-cp', jar,
            self.config.producer_class,
            str(rate),
            self.config.producer_namesrv,
        ]

        logger.info("启动 Producer: rate=%d r/s", rate)
        logger.info("  命令: %s", ' '.join(cmd))

        try:
            self._log_handle = open(log_path, 'w', encoding='utf-8')
            self._process = subprocess.Popen(
                cmd,
                cwd=self.config.producer_work_dir,
                stdout=self._log_handle,
                stderr=subprocess.STDOUT,
            )

            time.sleep(3)

            if self._process.poll() is not None:
                logger.error("Producer 启动后立即退出, returncode=%d",
                             self._process.returncode)
                # 打印日志尾部帮助诊断
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
        """停止 Producer"""
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
# 单轮标定结果
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
    """
    Source 容量标定执行器（全自动）

    每轮流程（与 ds2_calibration + JobExecutor 对齐）：
      ① 停 Producer
      ② Cancel 旧 Flink 作业
      ③ 等待 slot 释放
      ④ 清理旧 Consumer Group (SSH → mqadmin deleteSubGroup)
      ⑤ 等待 deregister
      ⑥ 用全新 consumer-group-suffix 提交 Flink 作业（从最新消费）
      ⑦ 等待 RUNNING
      ⑧ 启动 Producer 子进程（打满 Source）
      ⑨ 预热
      ⑩ 稳态采样
      ⑪ 停 Producer
      ⑫ 排空等待
    """

    def __init__(self, config: SourceCalibrationConfig):
        self.config = config
        self.flink = FlinkClient(config)
        self.ssh = SSHExecutor(config.ssh_host, config.ssh_port,
                               config.ssh_user, config.ssh_password)
        self.producer = ProducerManager(config)
        self.results: List[CalibrationRound] = []
        self._last_consumer_group: Optional[str] = None
        self._running = True

        os.makedirs(config.output_dir, exist_ok=True)

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

        # 1. Flink
        if not self.flink.check_connection():
            return False

        # 2. JAR
        jar_id = self.flink.ensure_jar_uploaded(self.ssh)
        if not jar_id:
            return False
        self.config.jar_id = jar_id

        # 3. Producer JAR
        if not os.path.exists(self.config.producer_jar):
            logger.error("Producer JAR 不存在: %s", self.config.producer_jar)
            return False
        logger.info("Producer JAR: %s", self.config.producer_jar)

        # 4. SSH
        try:
            out = self.ssh.exec_command("echo OK", timeout=10)
            if "OK" in out:
                logger.info("SSH 连接正常: %s@%s",
                            self.config.ssh_user, self.config.ssh_host)
            else:
                logger.warning("SSH 响应异常: %s", out[:100])
        except Exception as e:
            logger.warning("SSH 连接失败: %s", e)

        # 5. 清理残留
        running = self.flink.find_running_job()
        if running:
            logger.warning("发现运行中的作业 %s，正在清理...", running)
            self.flink.cancel_all_running()
            time.sleep(5)

        logger.info("前置检查通过 ✓")
        return True

    # ---- 参数构建 ----

    def _build_program_args(self, source_parallelism: int) -> tuple:
        """返回 (program_args_str, consumer_group_name)"""
        parts = []

        # 全新 consumer group + 从最新消费
        suffix = f"_src_cal_{uuid.uuid4().hex[:8]}"
        consumer_group = f"{self.config.consumer_group_prefix}{suffix}"
        parts.append(f"--consumer-group-suffix {suffix}")
        parts.append("--consume-from-latest true")

        # Source 并行度
        parts.append(f"--p-source {source_parallelism}")

        # 下游并行度
        for arg_name, value in self.config.downstream_parallelism.items():
            parts.append(f"--{arg_name} {value}")

        return " ".join(parts), consumer_group

    # ---- Consumer Group 清理 ----

    def _cleanup_consumer_group(self, group_name: str):
        logger.info("清理 Consumer Group: %s", group_name)
        cmd = (
            f"sh {self.config.rocketmq_home}/bin/mqadmin deleteSubGroup "
            f"-n {self.config.rocketmq_namesrv} "
            f"-c {self.config.rocketmq_cluster} "
            f"-g {group_name} || true"
        )
        try:
            out = self.ssh.exec_command(cmd)
            logger.info("清理完成: %s", out[:200] if out else "(无输出)")
        except Exception as e:
            logger.warning("清理失败(非致命): %s", e)

    # ---- 采样 ----

    def _collect_samples(self, job_id: str) -> List[float]:
        samples = []
        total_attempts = int(self.config.sample_seconds / self.config.sample_interval)

        logger.info("稳态采样: %d 次 × %.0fs = %ds",
                     total_attempts, self.config.sample_interval,
                     self.config.sample_seconds)

        for i in range(total_attempts):
            if not self._running:
                break

            rate = self.flink.get_source_output_rate(job_id)

            if rate is not None and rate >= 0:
                samples.append(rate)
                status = f"  [{i + 1:3d}/{total_attempts}] rate = {rate:8.1f} r/s"
            else:
                status = f"  [{i + 1:3d}/{total_attempts}] rate = N/A"

            if (i + 1) % 5 == 0 or i == 0 or i == total_attempts - 1:
                logger.info(status)

            if i < total_attempts - 1:
                time.sleep(self.config.sample_interval)

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

        # 跳过前 2 个样本（可能还在预热尾部）
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

        # Step 1: 停 Producer
        self.producer.stop()

        # Step 2: Cancel 旧作业
        current_job = self.flink.find_running_job()
        if current_job:
            self.flink.cancel_job(current_job)

        # Step 3: 等待 slot 释放
        self.flink.wait_slots_available()

        # Step 4: 清理旧 Consumer Group
        if self._last_consumer_group:
            self._cleanup_consumer_group(self._last_consumer_group)

        # Step 5: 等待 deregister
        wait_sec = self.config.deregister_wait
        logger.info("等待 %ds 让旧 Consumer Group 注销...", wait_sec)
        time.sleep(wait_sec)

        if not self._running:
            return None

        # Step 6: 用全新 consumer group 提交作业
        program_args, consumer_group = self._build_program_args(source_parallelism)
        logger.info("Consumer Group: %s", consumer_group)

        new_job_id = self.flink.submit_job(self.config.jar_id, program_args)
        if not new_job_id:
            logger.error("作业提交失败，跳过本轮")
            return None

        # Step 7: 等待 RUNNING
        if not self.flink.wait_job_running(new_job_id, self.config.job_start_timeout):
            logger.error("作业启动失败，跳过本轮")
            return None

        self._last_consumer_group = consumer_group

        # 验证并行度
        actual_p = self.flink.get_source_parallelism(new_job_id)
        if actual_p != source_parallelism:
            logger.warning("Source 实际并行度 %d != 预期 %d",
                           actual_p, source_parallelism)

        # Step 8: 启动 Producer（自动子进程）
        if not self.producer.start(self.config.producer_rate):
            logger.error("Producer 启动失败，跳过本轮")
            self.flink.cancel_job(new_job_id)
            return None

        # Step 9: 预热
        logger.info("预热阶段: 等待 %ds...", self.config.warmup_seconds)
        warmup_start = time.time()
        while time.time() - warmup_start < self.config.warmup_seconds:
            if not self._running:
                self.producer.stop()
                return None

            remaining = self.config.warmup_seconds - int(time.time() - warmup_start)

            # 检查作业存活
            if not self.flink.find_running_job():
                logger.error("Flink 作业在预热期间退出！")
                self.producer.stop()
                return None

            # 检查 Producer 存活
            if not self.producer.is_running():
                logger.error("Producer 在预热期间退出！")
                self.flink.cancel_job(new_job_id)
                return None

            # 每 15s 打印当前速率
            if remaining % 15 == 0 or remaining <= 5:
                rate = self.flink.get_source_output_rate(new_job_id)
                rate_str = f"{rate:.1f} r/s" if rate is not None else "N/A"
                logger.info("  预热中... %d/%ds, 当前速率: %s",
                            self.config.warmup_seconds - remaining,
                            self.config.warmup_seconds, rate_str)

            time.sleep(5)

        # Step 10: 稳态采样
        samples = self._collect_samples(new_job_id)

        # Step 11: 停 Producer
        self.producer.stop()

        # Step 12: 排空等待
        if self.config.drain_seconds > 0:
            logger.info("排空阶段: 等待 %ds...", self.config.drain_seconds)
            time.sleep(self.config.drain_seconds)

        # 分析
        result = self._analyze_samples(source_parallelism, samples, consumer_group)

        logger.info("")
        logger.info("─" * 50)
        logger.info("│ 本轮结果: Source P=%d", source_parallelism)
        logger.info("├──────────────────────────────────────────────")
        logger.info("│ Consumer Group   : %s", consumer_group)
        logger.info("│ 有效样本数       : %d", result.valid_sample_count)
        logger.info("│ 平均总吞吐       : %.1f r/s", result.total_output_rate)
        logger.info("│ 单实例吞吐       : %.1f r/s", result.per_instance_rate)
        logger.info("│ 标准差           : %.1f", result.std_dev)
        logger.info("│ 范围             : [%.1f, %.1f]",
                     result.min_rate, result.max_rate)
        logger.info("└──────────────────────────────────────────────")

        return result

    # ---- 保存 ----

    def _save_results(self):
        if not self.results:
            logger.error("没有有效结果！")
            return

        csv_path = os.path.join(self.config.output_dir,
                                "source_calibration.csv")

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
        logger.info("  Source 容量标定 —— 最终汇总")
        logger.info("=" * 60)
        logger.info("")
        logger.info("  %6s  %12s  %12s  %8s  %s",
                     "并行度", "总吞吐(r/s)", "单实例(r/s)", "标准差", "范围")
        logger.info("  " + "─" * 65)

        for r in self.results:
            logger.info("  %6d  %12.1f  %12.1f  %8.1f  [%.0f, %.0f]",
                        r.parallelism, r.total_output_rate,
                        r.per_instance_rate, r.std_dev,
                        r.min_rate, r.max_rate)

        logger.info("  " + "─" * 65)
        logger.info("  均值:   %.0f r/s", avg)
        logger.info("  中位数: %.0f r/s", median)

        if n >= 2:
            cv = (max(per_rates) - min(per_rates)) / avg * 100
            if cv < 15:
                stability = "良好"
            elif cv < 30:
                stability = "一般（建议增加采样时间）"
            else:
                stability = "差（结果可能不可靠）"
            logger.info("  稳定性: %s (变异幅度 %.1f%%)", stability, cv)

        recommended = int(median)
        logger.info("")
        logger.info("┌──────────────────────────────────────────────────────┐")
        logger.info("│ 推荐 source_capacity_per_instance = %-16d │", recommended)
        logger.info("│                                                      │")
        logger.info("│ 请修改 ds2_controller.py 中:                          │")
        logger.info("│   capacity_per_instance = %.1f                       │", float(recommended))
        logger.info("└──────────────────────────────────────────────────────┘")

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
        logger.info("  Flink REST         : %s", self.config.flink_rest_url)
        logger.info("  Main class         : %s", self.config.main_class)
        logger.info("  SSH                : %s@%s:%d",
                     self.config.ssh_user, self.config.ssh_host, self.config.ssh_port)
        logger.info("  Consumer prefix    : %s", self.config.consumer_group_prefix)
        logger.info("  Producer class     : %s", self.config.producer_class)
        logger.info("  Producer JAR       : %s", self.config.producer_jar)
        logger.info("  Producer rate      : %d r/s", self.config.producer_rate)
        logger.info("  并行度列表         : %s", self.config.parallelism_list)
        logger.info("  预热               : %ds", self.config.warmup_seconds)
        logger.info("  采样               : %ds (间隔 %.0fs)",
                     self.config.sample_seconds, self.config.sample_interval)
        logger.info("  排空               : %ds", self.config.drain_seconds)
        logger.info("  下游并行度         : %s", self.config.downstream_parallelism)
        logger.info("  输出目录           : %s", self.config.output_dir)

        n = len(self.config.parallelism_list)
        per_round = (self.config.warmup_seconds + self.config.sample_seconds
                     + self.config.deregister_wait + self.config.drain_seconds + 30)
        logger.info("  预计总耗时         : ~%dm (%d 轮 × ~%ds)",
                     n * per_round // 60, n, per_round)
        logger.info("")

        # 前置检查
        if not self.preflight_check():
            sys.exit(1)

        # 逐轮执行
        for i, p in enumerate(self.config.parallelism_list):
            if not self._running:
                logger.warning("实验被中断")
                break

            logger.info("")
            logger.info("【标定 %d/%d】Source P = %d", i + 1, n, p)

            try:
                result = self.run_one_round(p)
                if result:
                    self.results.append(result)
                else:
                    logger.warning("并行度 %d 标定失败，跳过", p)
            except Exception as e:
                logger.error("轮次 p=%d 异常: %s", p, e, exc_info=True)

        # 清理 + 输出
        self._final_cleanup()
        self._save_results()
        self._print_summary()


# ============================================================
# 入口
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="Source 算子单实例容量标定（全自动）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
每轮流程（全自动，无需人工介入）:
  1. 停 Producer 子进程
  2. Cancel 旧 Flink 作业
  3. 清理旧 Consumer Group (SSH → mqadmin)
  4. 用全新 Consumer Group 提交 Flink (--consume-from-latest true)
  5. 启动 Producer 子进程（打满 Source）
  6. 预热 → 稳态采样 → 停 Producer → 排空
  7. 记录单实例最大吞吐

示例:
  python calibrate_source.py
  python calibrate_source.py ds2_calibration_config.yaml
  python calibrate_source.py ds2_calibration_config.yaml --parallelism-list 1 2 3
  python calibrate_source.py ds2_calibration_config.yaml --warmup 120 --sample 90
        """)

    parser.add_argument("config", nargs='?', default="ds2_calibration_config.yaml",
                        help="配置文件路径 (默认: ds2_calibration_config.yaml)")
    parser.add_argument("--parallelism-list", type=int, nargs="+", default=None,
                        help="Source 并行度列表 (默认: 1 2 3 4)")
    parser.add_argument("--warmup", type=int, default=None,
                        help="预热时间/秒")
    parser.add_argument("--sample", type=int, default=None,
                        help="采样时间/秒")
    parser.add_argument("--rate", type=int, default=None,
                        help="Producer 发送速率 (默认: 20000)")
    parser.add_argument("--output-dir", default=None,
                        help="输出目录")

    args = parser.parse_args()

    config = SourceCalibrationConfig.from_yaml(args.config)

    if args.parallelism_list:
        config.parallelism_list = args.parallelism_list
    if args.warmup:
        config.warmup_seconds = args.warmup
    if args.sample:
        config.sample_seconds = args.sample
    if args.rate:
        config.producer_rate = args.rate
    if args.output_dir:
        config.output_dir = args.output_dir

    calibrator = SourceCalibrator(config)
    calibrator.run()


if __name__ == "__main__":
    main()
