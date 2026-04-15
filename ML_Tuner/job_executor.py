#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
calibrate_source.py - Source 算子单实例容量标定脚本

采用与 ML_Tuner.JobExecutor 相同的作业生命周期管理模式：
  - 每轮使用全新 consumer-group-suffix（UUID），从最新位置消费
  - 轮次结束后清理旧 Consumer Group
  - 通过 SSH 执行远程 mqadmin 命令
  - Producer 作为本地子进程管理

使用方式：
  python3 calibrate_source.py
  python3 calibrate_source.py --parallelism-list 1 2 3
  python3 calibrate_source.py --warmup 120 --sample 90
  python3 calibrate_source.py --config my_config.yaml
"""

import argparse
import csv
import math
import logging
import os
import signal
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import requests
import paramiko
import yaml

# ============================================================
# 日志配置
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("SourceCalibration")


# ============================================================
# 配置加载
# ============================================================
@dataclass
class CalibrationConfig:
    """标定实验配置（全部从 YAML 加载，与 JobExecutor 共用同一份配置）"""

    # Flink
    flink_rest_url: str = "http://192.168.56.151:8081"
    flink_home: str = "/opt/app/flink-1.17.2"
    jar_path: str = "/opt/flink_jobs/flink-rocketmq-demo-1.0-SNAPSHOT.jar"
    main_class: str = "org.jzx.SeismicStreamJob"
    job_name: str = "SeismicStreamJob"

    # SSH
    ssh_host: str = "192.168.56.151"
    ssh_port: int = 22
    ssh_user: str = "root"
    ssh_password: str = ""

    # RocketMQ
    rocketmq_home: str = "/usr/local/rocketmq"
    rocketmq_cluster: str = "RaftCluster"
    rocketmq_namesrv: str = "192.168.56.151:9876"
    consumer_group_prefix: str = "seismic-flink-consumer-group"
    consumer_group_deregister_wait: int = 15

    # Producer
    producer_enabled: bool = True
    producer_class: str = "org.jzx.producer.SeismicProducer"
    producer_jar: str = ""
    producer_work_dir: str = ""
    producer_args: str = ""

    # Source 算子关键字
    source_keyword: str = "RocketMQ-Source"

    # 标定参数
    parallelism_list: List[int] = field(default_factory=lambda: [1, 2, 3, 4])
    warmup_seconds: int = 90
    sample_seconds: int = 60
    sample_interval: float = 2.0
    producer_rate: int = 20000

    # 下游算子并行度（足够大以消除反压）
    downstream_parallelism: Dict[str, int] = field(default_factory=lambda: {
        "p-filter": 4,
        "p-signal": 4,
        "p-feature": 4,
        "p-grid": 2,
        "p-sink": 2,
    })

    # 超时
    cancel_timeout: int = 60
    submit_timeout: int = 120
    job_start_timeout: int = 90

    # 输出
    result_file: str = "source_calibration.csv"

    # 运行时状态（不从配置文件读取）
    jar_id: str = ""

    @classmethod
    def from_yaml(cls, path: str) -> 'CalibrationConfig':
        """从 YAML 配置文件加载（兼容 ML_Tuner 的配置格式）"""
        config = cls()

        if not os.path.exists(path):
            logger.warning("配置文件 %s 不存在，使用默认值", path)
            return config

        with open(path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)

        # Flink
        flink_cfg = cfg.get('flink', {})
        config.flink_rest_url = flink_cfg.get('rest_url', config.flink_rest_url).rstrip('/')
        config.flink_home = flink_cfg.get('flink_home', config.flink_home)
        config.jar_path = flink_cfg.get('jar_path', config.jar_path)
        config.main_class = flink_cfg.get('main_class', config.main_class)
        config.job_name = flink_cfg.get('job_name', config.job_name)

        # SSH
        ssh_cfg = cfg.get('ssh', {})
        config.ssh_host = ssh_cfg.get('host', config.ssh_host)
        config.ssh_port = ssh_cfg.get('port', config.ssh_port)
        config.ssh_user = ssh_cfg.get('user', config.ssh_user)
        config.ssh_password = ssh_cfg.get('password', config.ssh_password)

        # RocketMQ
        rmq_cfg = cfg.get('rocketmq', {})
        config.rocketmq_home = rmq_cfg.get('home', config.rocketmq_home)
        config.rocketmq_cluster = rmq_cfg.get('cluster', config.rocketmq_cluster)
        config.rocketmq_namesrv = rmq_cfg.get('namesrv', config.rocketmq_namesrv)
        config.consumer_group_prefix = rmq_cfg.get(
            'consumer_group_prefix', config.consumer_group_prefix)
        config.consumer_group_deregister_wait = int(
            rmq_cfg.get('deregister_wait_seconds', config.consumer_group_deregister_wait))

        # Producer
        producer_cfg = cfg.get('producer', {})
        config.producer_enabled = producer_cfg.get('enabled', config.producer_enabled)
        config.producer_class = producer_cfg.get('main_class', config.producer_class)
        config.producer_jar = producer_cfg.get(
            'local_jar',
            os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                'target', 'flink-rocketmq-demo-1.0-SNAPSHOT.jar'))
        config.producer_work_dir = producer_cfg.get(
            'work_dir',
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config.producer_args = producer_cfg.get('args', config.producer_args)

        # 超时
        timeouts = cfg.get('timeouts', {})
        config.cancel_timeout = timeouts.get('cancel', config.cancel_timeout)
        config.submit_timeout = timeouts.get('submit', config.submit_timeout)

        # 算子映射 - Source 关键字
        op_mapping = cfg.get('operator_mapping', {})
        source_cfg = op_mapping.get('source', {})
        if 'keyword' in source_cfg:
            config.source_keyword = source_cfg['keyword']

        logger.info("配置加载完成: %s", path)
        return config


# ============================================================
# SSH 执行器
# ============================================================
class SSHExecutor:
    """通过 SSH 执行远程命令（与 JobExecutor 相同模式）"""

    def __init__(self, host: str, port: int, user: str, password: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def exec_command(self, cmd: str, timeout: int = 120) -> str:
        """执行远程命令并返回输出"""
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
    """Flink REST API 客户端（与 JobExecutor 相同风格）"""

    def __init__(self, config: CalibrationConfig):
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
        except Exception as e:
            logger.debug("GET %s 失败: %s", path, e)
            return None

    # ---- 集群检查 ----

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

    # ---- JAR 管理 ----

    def ensure_jar_uploaded(self, ssh: SSHExecutor) -> Optional[str]:
        """查找或上传 JAR，返回 jar_id"""
        # 先查已有的
        try:
            jars = self._get("/jars")
            for f in jars.get("files", []):
                if ('rocketmq-demo' in f['name'].lower()
                        or 'seismic' in f['name'].lower()):
                    logger.info("找到已上传的 JAR: %s", f["id"])
                    return f["id"]
        except Exception:
            pass

        # SSH 上传
        logger.info("上传 JAR: %s", self.config.jar_path)
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
                        logger.info("✅ JAR 上传成功: %s", f["id"])
                        return f["id"]
            logger.error("JAR 上传失败: %s", out[:300])
        except Exception as e:
            logger.error("JAR 上传异常: %s", e)
        return None

    # ---- 作业管理 ----

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
            resp = self._session.patch(
                f"{self.rest_url}/jobs/{job_id}", timeout=self._timeout)
            logger.info("Cancel 响应码: %d", resp.status_code)
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
        """取消所有运行中的作业"""
        while True:
            job_id = self.find_running_job()
            if not job_id:
                break
            self.cancel_job(job_id)

    def submit_job(self, jar_id: str, program_args: str) -> Optional[str]:
        """提交作业"""
        logger.info("提交作业（全新启动，无 savepoint）")
        logger.info("  参数: %s", program_args)

        run_body = {
            "entryClass": self.config.main_class,
            "programArgs": program_args,
            "allowNonRestoredState": True,
        }

        try:
            resp = self._session.post(
                f"{self.rest_url}/jars/{jar_id}/run",
                json=run_body, timeout=self.config.submit_timeout)
            if resp.status_code == 200:
                job_id = resp.json().get("jobid")
                logger.info("✅ 作业提交成功: %s", job_id)
                return job_id
            logger.error("❌ 提交失败 %d: %s", resp.status_code, resp.text[:300])
        except Exception as e:
            logger.error("提交异常: %s", e)
        return None

    def wait_job_running(self, job_id: str, timeout: int = 90) -> bool:
        """等待作业进入 RUNNING 状态"""
        for i in range(timeout // 3):
            time.sleep(3)
            try:
                state = self._get(f"/jobs/{job_id}").get("state", "UNKNOWN")
                if state == "RUNNING":
                    logger.info("作业 RUNNING: %s", job_id)
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
        """等待 slot 释放"""
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

    # ---- 算子指标 ----

    def find_source_vertex(self, job_id: str) -> Optional[dict]:
        """查找 Source 算子"""
        try:
            data = self._get(f"/jobs/{job_id}")
            for v in data.get("vertices", []):
                if self.config.source_keyword in v.get("name", ""):
                    return v
            # 降级
            for v in data.get("vertices", []):
                if "source" in v.get("name", "").lower():
                    logger.warning("精确匹配失败，降级匹配: %s", v["name"])
                    return v
        except Exception:
            pass
        return None

    def get_source_output_rate(self, job_id: str) -> Optional[float]:
        """获取 Source 算子 numRecordsOutPerSecond"""
        source = self.find_source_vertex(job_id)
        if not source:
            return None
        try:
            data = self._get(
                f"/jobs/{job_id}/vertices/{source['id']}"
                f"/metrics?get=numRecordsOutPerSecond"
            )
            for m in data:
                if m.get("id") == "numRecordsOutPerSecond":
                    val = float(m["value"])
                    if not (math.isnan(val) or math.isinf(val)):
                        return val
        except Exception:
            pass
        return None

    def get_source_parallelism(self, job_id: str) -> int:
        """获取 Source 算子实际并行度"""
        source = self.find_source_vertex(job_id)
        return source.get("parallelism", 0) if source else 0

    def close(self):
        self._session.close()


# ============================================================
# Producer 管理器
# ============================================================
class ProducerManager:
    """Producer 子进程管理（与 JobExecutor 相同模式）"""

    def __init__(self, config: CalibrationConfig):
        self.config = config
        self._process: Optional[subprocess.Popen] = None
        self._log_handle = None

        self._log_dir = os.path.join(config.producer_work_dir, 'calibration_logs')
        self._log_path = os.path.join(self._log_dir, 'producer_calibration.log')

    def start(self, rate: int) -> bool:
        """启动 Producer"""
        if not self.config.producer_enabled:
            logger.info("Producer 未启用，跳过")
            return True

        jar = self.config.producer_jar
        if not os.path.exists(jar):
            logger.error("Producer JAR 不存在: %s", jar)
            return False

        self.stop()
        os.makedirs(self._log_dir, exist_ok=True)

        try:
            self._log_handle = open(self._log_path, 'a', encoding='utf-8')

            cmd = ['java', '-cp', jar, self.config.producer_class]

            # 追加 producer_args（与 JobExecutor 相同逻辑）
            if self.config.producer_args:
                args = self.config.producer_args
                if isinstance(args, list):
                    cmd.extend([str(a) for a in args])
                else:
                    cmd.extend(str(args).split())

            # 追加标定速率（如果 Producer 支持命令行速率参数）
            cmd.append(str(rate))

            logger.info("Producer 启动命令: %s", ' '.join(cmd))

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
                self._process = None
                return False

            logger.info("✅ Producer 已启动: PID=%d, rate=%d, log=%s",
                        self._process.pid, rate, self._log_path)
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
        if self._process is None:
            return False
        return self._process.poll() is None


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


# ============================================================
# 标定执行器
# ============================================================
class SourceCalibrator:
    """
    Source 容量标定执行器

    每轮流程（与 JobExecutor.apply_parameters 对齐）：
      ① 停 Producer
      ② Cancel 旧 Flink 作业
      ③ 等待 slot 释放
      ④ 清理旧 Consumer Group
      ⑤ 等待 deregister
      ⑥ 用全新 consumer-group-suffix 提交 Flink 作业（从最新消费）
      ⑦ 等待 RUNNING
      ⑧ 启动 Producer
      ⑨ 预热
      ⑩ 采样
    """

    def __init__(self, config: CalibrationConfig):
        self.config = config
        self.flink = FlinkClient(config)
        self.ssh = SSHExecutor(config.ssh_host, config.ssh_port,
                               config.ssh_user, config.ssh_password)
        self.producer = ProducerManager(config)
        self.results: List[CalibrationRound] = []

        # 跟踪 consumer group（与 JobExecutor 相同）
        self._last_consumer_group: Optional[str] = None

    # ================================================================
    # 前置检查
    # ================================================================

    def preflight_check(self) -> bool:
        logger.info("=" * 60)
        logger.info("前置环境检查")
        logger.info("=" * 60)

        # 1. Flink 连接
        if not self.flink.check_connection():
            return False

        # 2. JAR
        jar_id = self.flink.ensure_jar_uploaded(self.ssh)
        if not jar_id:
            return False
        self.config.jar_id = jar_id

        # 3. Producer JAR 存在性
        if self.config.producer_enabled:
            if not os.path.exists(self.config.producer_jar):
                logger.error("Producer JAR 不存在: %s", self.config.producer_jar)
                logger.error("请检查配置 producer.local_jar")
                return False
            logger.info("Producer JAR: %s", self.config.producer_jar)

        # 4. SSH 连通性
        try:
            out = self.ssh.exec_command("echo OK", timeout=10)
            if "OK" in out:
                logger.info("SSH 连接正常: %s@%s",
                            self.config.ssh_user, self.config.ssh_host)
            else:
                logger.warning("SSH 响应异常: %s", out[:100])
        except Exception as e:
            logger.warning("SSH 连接失败: %s (mqadmin 清理将不可用)", e)

        # 5. 清理残留作业
        running = self.flink.find_running_job()
        if running:
            logger.warning("发现运行中的作业 %s，正在清理...", running)
            self.flink.cancel_all_running()
            time.sleep(5)

        logger.info("前置检查通过 ✓")
        return True

    # ================================================================
    # 参数构建（与 JobExecutor._build_program_args 对齐）
    # ================================================================

    def _build_program_args(self, source_parallelism: int) -> tuple:
        """
        构建程序参数，返回 (program_args_str, consumer_group_name)
        """
        parts = []

        # 全新 consumer group（核心：解决积压问题）
        suffix = f"_calibrate_{uuid.uuid4().hex[:8]}"
        consumer_group = f"{self.config.consumer_group_prefix}{suffix}"
        parts.append(f"--consumer-group-suffix {suffix}")
        parts.append("--consume-from-latest true")

        # Source 并行度
        parts.append(f"--p-source {source_parallelism}")

        # 下游并行度
        for arg_name, value in self.config.downstream_parallelism.items():
            parts.append(f"--{arg_name} {value}")

        return " ".join(parts), consumer_group

    # ================================================================
    # Consumer Group 清理（与 JobExecutor 相同）
    # ================================================================

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
            logger.info("清理完成: %s, out=%s", group_name, out[:200] if out else "")
        except Exception as e:
            logger.warning("清理失败(非致命): %s", e)

    # ================================================================
    # 采集样本
    # ================================================================

    def _collect_samples(self, job_id: str) -> List[float]:
        samples = []
        total_attempts = int(self.config.sample_seconds / self.config.sample_interval)

        logger.info("采样开始: %d 次 × %.1fs = %ds",
                    total_attempts, self.config.sample_interval,
                    self.config.sample_seconds)

        for i in range(total_attempts):
            rate = self.flink.get_source_output_rate(job_id)

            if rate is not None and rate >= 0:
                samples.append(rate)
                status = f"  #{i + 1:3d}/{total_attempts}  rate = {rate:8.1f} r/s"
            else:
                status = f"  #{i + 1:3d}/{total_attempts}  rate = N/A"

            if (i + 1) % 5 == 0 or i == 0 or i == total_attempts - 1:
                logger.info(status)

            time.sleep(self.config.sample_interval)

        return samples

    # ================================================================
    # 分析结果
    # ================================================================

    @staticmethod
    def _analyze_samples(parallelism: int, samples: List[float],
                         consumer_group: str = "") -> CalibrationRound:
        result = CalibrationRound(parallelism=parallelism,
                                  consumer_group=consumer_group,
                                  samples=samples)

        if not samples:
            logger.warning("没有有效样本！")
            return result

        # 去除两端各 5% 异常值
        sorted_s = sorted(samples)
        trim = max(1, len(sorted_s) // 20)
        trimmed = sorted_s[trim:-trim] if len(sorted_s) > 4 else sorted_s

        result.valid_sample_count = len(trimmed)
        result.total_output_rate = sum(trimmed) / len(trimmed)
        result.per_instance_rate = result.total_output_rate / max(parallelism, 1)
        result.min_rate = min(trimmed)
        result.max_rate = max(trimmed)

        mean = result.total_output_rate
        variance = sum((x - mean) ** 2 for x in trimmed) / len(trimmed)
        result.std_dev = variance ** 0.5

        return result

    # ================================================================
    # 单轮执行（对齐 JobExecutor.apply_parameters 流程）
    # ================================================================

    def run_one_round(self, source_parallelism: int) -> Optional[CalibrationRound]:
        logger.info("")
        logger.info("=" * 60)
        logger.info("  标定轮次: Source 并行度 = %d", source_parallelism)
        logger.info("=" * 60)

        # ===== Step 1: 停 Producer =====
        self.producer.stop()

        # ===== Step 2: Cancel 旧 Flink 作业 =====
        current_job = self.flink.find_running_job()
        if current_job:
            self.flink.cancel_job(current_job)

        # ===== Step 3: 等待 slot 释放 =====
        self.flink.wait_slots_available()

        # ===== Step 4: 清理旧 Consumer Group =====
        if self._last_consumer_group:
            self._cleanup_consumer_group(self._last_consumer_group)

        # ===== Step 5: 等待 deregister =====
        wait_sec = self.config.consumer_group_deregister_wait
        logger.info("等待 %ds 让旧 Consumer Group 注销...", wait_sec)
        time.sleep(wait_sec)

        # ===== Step 6: 用全新 consumer group 提交 Flink 作业 =====
        program_args, consumer_group = self._build_program_args(source_parallelism)
        logger.info("Consumer Group: %s", consumer_group)
        logger.info("从最新位置消费（跳过所有历史积压）")

        new_job_id = self.flink.submit_job(self.config.jar_id, program_args)
        if not new_job_id:
            logger.error("作业提交失败，跳过本轮")
            return None

        # ===== Step 7: 等待 RUNNING =====
        if not self.flink.wait_job_running(new_job_id, self.config.job_start_timeout):
            logger.error("作业启动失败，跳过本轮")
            return None

        # 记录 consumer group（用于下轮清理）
        self._last_consumer_group = consumer_group

        # 验证实际并行度
        actual_p = self.flink.get_source_parallelism(new_job_id)
        if actual_p != source_parallelism:
            logger.warning("Source 实际并行度 %d ≠ 预期 %d（可能受 slot 限制）",
                           actual_p, source_parallelism)

        # ===== Step 8: 启动 Producer =====
        if not self.producer.start(self.config.producer_rate):
            logger.error("Producer 启动失败，跳过本轮")
            self.flink.cancel_job(new_job_id)
            return None

        # ===== Step 9: 预热 =====
        logger.info("预热中... (%ds)", self.config.warmup_seconds)
        warmup_start = time.time()
        while time.time() - warmup_start < self.config.warmup_seconds:
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
                logger.info("  预热剩余 %ds, 当前速率: %s", remaining, rate_str)

            time.sleep(5)

        # ===== Step 10: 采样 =====
        samples = self._collect_samples(new_job_id)

        # 分析
        result = self._analyze_samples(source_parallelism, samples, consumer_group)

        logger.info("")
        logger.info("  本轮结果:")
        logger.info("    Source 并行度     : %d", source_parallelism)
        logger.info("    Consumer Group   : %s", consumer_group)
        logger.info("    有效样本数        : %d", result.valid_sample_count)
        logger.info("    平均总吞吐        : %.1f r/s", result.total_output_rate)
        logger.info("    单实例吞吐        : %.1f r/s", result.per_instance_rate)
        logger.info("    标准差            : %.1f", result.std_dev)
        logger.info("    范围              : [%.1f, %.1f]",
                    result.min_rate, result.max_rate)

        return result

    # ================================================================
    # 保存结果
    # ================================================================

    def _save_results(self):
        if not self.results:
            logger.error("没有有效结果！")
            return

        with open(self.config.result_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "parallelism", "total_output_rate", "per_instance_rate",
                "std_dev", "min_rate", "max_rate", "valid_samples",
                "consumer_group"
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
                    r.consumer_group
                ])
        logger.info("结果已保存: %s", self.config.result_file)

    # ================================================================
    # 打印汇总
    # ================================================================

    def _print_summary(self):
        if not self.results:
            return

        per_rates = [r.per_instance_rate for r in self.results]
        sorted_rates = sorted(per_rates)
        n = len(sorted_rates)
        median = (
            sorted_rates[n // 2]
            if n % 2 == 1
            else (sorted_rates[n // 2 - 1] + sorted_rates[n // 2]) / 2
        )
        avg = sum(per_rates) / n

        logger.info("")
        logger.info("╔══════════════════════════════════════════════════════╗")
        logger.info("║                  标定结果汇总                        ║")
        logger.info("╚══════════════════════════════════════════════════════╝")
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
                stability = "✅ 良好"
            elif cv < 30:
                stability = "⚠️ 一般（建议增加采样时间重试）"
            else:
                stability = "❌ 差（结果可能不可靠）"
            logger.info("  稳定性: %s (变异幅度 %.1f%%)", stability, cv)

        recommended = int(median)

        logger.info("")
        logger.info("╔══════════════════════════════════════════════════════════╗")
        logger.info("║  推荐 source_capacity_per_instance = %-18d ║", recommended)
        logger.info("║                                                          ║")
        logger.info("║  请在 ds2_config.yaml 的 ds2 节点下添加:                   ║")
        logger.info("║    source_capacity_per_instance: %-24d ║", recommended)
        logger.info("║                                                          ║")
        logger.info("║  然后修改 ds2_controller.py 中 Source busy 补偿逻辑        ║")
        logger.info("║  将硬编码的 2500.0 替换为该标定值                           ║")
        logger.info("╚══════════════════════════════════════════════════════════╝")

    # ================================================================
    # 最终清理
    # ================================================================

    def _final_cleanup(self):
        """清理所有资源"""
        logger.info("最终清理...")
        self.producer.stop()
        self.flink.cancel_all_running()

        # 清理最后一个 consumer group
        if self._last_consumer_group:
            time.sleep(5)
            self._cleanup_consumer_group(self._last_consumer_group)

        self.flink.close()

    # ================================================================
    # 主流程
    # ================================================================

    def run(self):
        logger.info("")
        logger.info("╔══════════════════════════════════════════════════════╗")
        logger.info("║        Source 容量标定实验                            ║")
        logger.info("╚══════════════════════════════════════════════════════╝")
        logger.info("")
        logger.info("  Flink REST         : %s", self.config.flink_rest_url)
        logger.info("  JAR path           : %s", self.config.jar_path)
        logger.info("  Main class         : %s", self.config.main_class)
        logger.info("  SSH                : %s@%s:%d",
                    self.config.ssh_user, self.config.ssh_host, self.config.ssh_port)
        logger.info("  RocketMQ home      : %s", self.config.rocketmq_home)
        logger.info("  Consumer prefix    : %s", self.config.consumer_group_prefix)
        logger.info("  Source keyword     : %s", self.config.source_keyword)
        logger.info("  Producer class     : %s", self.config.producer_class)
        logger.info("  Producer JAR       : %s", self.config.producer_jar)
        logger.info("  Producer rate      : %d r/s", self.config.producer_rate)
        logger.info("  并行度列表         : %s", self.config.parallelism_list)
        logger.info("  预热时间           : %ds", self.config.warmup_seconds)
        logger.info("  采样时间           : %ds", self.config.sample_seconds)
        logger.info("  下游并行度         : %s", self.config.downstream_parallelism)
        logger.info("  结果文件           : %s", self.config.result_file)

        n = len(self.config.parallelism_list)
        per_round = (self.config.warmup_seconds + self.config.sample_seconds
                     + self.config.consumer_group_deregister_wait + 30)
        logger.info("  预计总耗时         : ~%dm (%d 轮 × ~%ds)",
                    n * per_round // 60, n, per_round)
        logger.info("")

        # 前置检查
        if not self.preflight_check():
            sys.exit(1)

        # 逐轮执行
        for p in self.config.parallelism_list:
            try:
                result = self.run_one_round(p)
                if result:
                    self.results.append(result)
                else:
                    logger.warning("并行度 %d 标定失败，跳过", p)
            except KeyboardInterrupt:
                logger.info("\n用户中断，保存已有结果...")
                break
            except Exception as e:
                logger.error("轮次 p=%d 异常: %s", p, e, exc_info=True)

        # 清理 + 输出
        self._final_cleanup()
        self._save_results()
        self._print_summary()


# ============================================================
# 命令行入口
# ============================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Source 算子单实例容量标定工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
每轮执行流程:
  ① 停 Producer
  ② Cancel 旧 Flink 作业
  ③ 清理旧 Consumer Group (mqadmin deleteSubGroup)
  ④ 用全新 Consumer Group 提交 Flink 作业 (--consume-from-latest true)
  ⑤ 启动 Producer (打满 Source)
  ⑥ 预热等待
  ⑦ 采集稳态吞吐数据

  全新 Consumer Group 从最新 offset 消费，彻底避免历史积压问题。

示例:
  python3 calibrate_source.py --config ml_tuner_config.yaml
  python3 calibrate_source.py --config ml_tuner_config.yaml --parallelism-list 1 2
  python3 calibrate_source.py --config ml_tuner_config.yaml --warmup 120 --sample 90
        """
    )

    parser.add_argument(
        "--config", default="ml_tuner_config.yaml",
        help="配置文件路径（兼容 ML_Tuner 配置格式）"
    )
    parser.add_argument(
        "--parallelism-list", type=int, nargs="+", default=None,
        help="要测试的 Source 并行度列表 (默认: 1 2 3 4)"
    )
    parser.add_argument(
        "--warmup", type=int, default=None,
        help="预热时间/秒 (默认: 90)"
    )
    parser.add_argument(
        "--sample", type=int, default=None,
        help="采样时间/秒 (默认: 60)"
    )
    parser.add_argument(
        "--producer-rate", type=int, default=None,
        help="Producer 发送速率 (默认: 20000)"
    )
    parser.add_argument(
        "--output", default=None,
        help="结果 CSV 文件路径 (默认: source_calibration.csv)"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # 从 YAML 加载配置
    config = CalibrationConfig.from_yaml(args.config)

    # 命令行参数覆盖
    if args.parallelism_list:
        config.parallelism_list = args.parallelism_list
    if args.warmup:
        config.warmup_seconds = args.warmup
    if args.sample:
        config.sample_seconds = args.sample
    if args.producer_rate:
        config.producer_rate = args.producer_rate
    if args.output:
        config.result_file = args.output

    # 创建标定器
    calibrator = SourceCalibrator(config)

    # 注册退出清理
    def cleanup_handler(signum, frame):
        logger.info("\n收到信号 %d，清理中...", signum)
        calibrator._final_cleanup()
        calibrator._save_results()
        if calibrator.results:
            calibrator._print_summary()
        sys.exit(1)

    signal.signal(signal.SIGINT, cleanup_handler)
    signal.signal(signal.SIGTERM, cleanup_handler)

    # 执行
    calibrator.run()


if __name__ == "__main__":
    main()
