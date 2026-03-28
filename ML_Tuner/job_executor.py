#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import subprocess
import uuid
import requests
import paramiko
import logging
import yaml
from typing import Optional, Dict

logger = logging.getLogger("ml_tuner.job_executor")


class JobExecutor:
    """通过 REST API 管理 Flink 作业生命周期"""

    def __init__(self, config_path: str = None, config_dict: dict = None):
        if config_dict is not None:
            cfg = config_dict
        elif config_path is not None:
            with open(config_path, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f)
        else:
            raise ValueError("必须提供 config_path 或 config_dict")

        # Flink 配置
        flink_cfg = cfg['flink']
        self.rest_url = flink_cfg['rest_url'].rstrip('/')
        self.flink_home = flink_cfg.get('flink_home', '/opt/app/flink-1.17.2')
        self.jar_path = flink_cfg.get(
            'jar_path',
            '/opt/flink_jobs/flink-rocketmq-demo-1.0-SNAPSHOT.jar')
        self.main_class = flink_cfg.get('main_class', 'org.jzx.SeismicStreamJob')
        self.job_name = flink_cfg.get('job_name', 'SeismicStreamJob')
        self.savepoint_dir = flink_cfg.get(
            'savepoint_dir',
            'hdfs://node01:9000/flink/savepoints')

        # SSH 配置
        ssh_cfg = cfg.get('ssh', {})
        self.ssh_host = ssh_cfg.get('host', '192.168.56.151')
        self.ssh_port = ssh_cfg.get('port', 22)
        self.ssh_user = ssh_cfg.get('user', 'root')
        self.ssh_password = ssh_cfg.get('password', '')

        # DS2 协调
        ds2_cfg = cfg.get('ds2', {})
        self.pause_signal_file = ds2_cfg.get('pause_signal_file', '')
        self.ds2_lock_file = ds2_cfg.get('lock_file', '')
        self.ds2_enabled = bool(self.pause_signal_file)

        # 超时配置
        self.savepoint_timeout = cfg.get('timeouts', {}).get('savepoint', 60)
        self.cancel_timeout = cfg.get('timeouts', {}).get('cancel', 60)
        self.submit_timeout = cfg.get('timeouts', {}).get('submit', 120)
        self.warm_up_seconds = cfg.get('scheduler', {}).get(
            'warm_up_after_apply_seconds', 90)

        # RocketMQ 配置
        rmq_cfg = cfg.get('rocketmq', {})
        self._consumer_group_deregister_wait = int(
            rmq_cfg.get('deregister_wait_seconds', 15))
        self._rocketmq_home = rmq_cfg.get('home', '/usr/local/rocketmq')
        self._rocketmq_cluster = rmq_cfg.get('cluster', 'RaftCluster')
        self._consumer_group_prefix = rmq_cfg.get(
            'consumer_group_prefix', 'seismic-flink-consumer-group')
        self._cleanup_old_consumer_group_enabled = bool(
            rmq_cfg.get('cleanup_old_consumer_group', True))

        # 状态
        self._jar_id: Optional[str] = None
        self._current_job_id: Optional[str] = None
        self._session = requests.Session()
        self._session.headers.update({'Accept': 'application/json'})
        self._timeout = 15

        # 失败计数
        self._consecutive_failures = 0
        self._max_failures = 3

        # Producer 配置
        producer_cfg = cfg.get('producer', {})
        self._producer_enabled = producer_cfg.get('enabled', True)
        self._producer_class = producer_cfg.get(
            'main_class', 'org.jzx.producer.SeismicProducer')
        self._producer_local_jar = producer_cfg.get(
            'local_jar',
            os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                'target',
                'flink-rocketmq-demo-1.0-SNAPSHOT.jar'))
        self._producer_work_dir = producer_cfg.get(
            'work_dir',
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self._producer_process: Optional[subprocess.Popen] = None
        self._producer_log_handle = None

        self._producer_log_dir = os.path.join(
            self._producer_work_dir, 'ML_Tuner', 'logs')
        self._producer_log_path = os.path.join(
            self._producer_log_dir, 'producer.log')
        self._producer_last_log_size = 0
        self._producer_last_check_ts = 0.0

        # 当前/上一个 consumer group 信息
        self._pending_consumer_suffix: Optional[str] = None
        self._pending_consumer_group: Optional[str] = None
        self._last_consumer_suffix: Optional[str] = None
        self._last_consumer_group: Optional[str] = None

        logger.info("JobExecutor 初始化: rest=%s, jar=%s, main=%s",
                    self.rest_url, self.jar_path, self.main_class)

    # ================================================================
    # 公开接口
    # ================================================================

    def apply_parameters(self, params: Dict[str, int], *,
                         use_savepoint: bool = True) -> bool:
        """
        统一策略：停旧启新（不使用 Savepoint）。
        use_savepoint 参数保留仅为兼容调用方。
        """
        try:
            if self.ds2_enabled:
                self._acquire_ds2_lock()

            if not self._ensure_jar_uploaded():
                raise RuntimeError("JAR 上传失败")

            current_job = self._find_running_job()
            mode_str = "停旧启新" if current_job else "首次启动"

            logger.info("=" * 50)
            logger.info("开始应用新参数 [%s]: %s", mode_str, params)
            logger.info("=" * 50)

            old_group = self._last_consumer_group

            # 1) 停旧作业
            if current_job:
                self._cancel_job(current_job)
                self._wait_slots_available()

                # 可选：清理旧 Consumer Group，防止 Broker 累积历史位点
                if self._cleanup_old_consumer_group_enabled and old_group:
                    self._cleanup_old_consumer_group(old_group)

                wait_sec = self._consumer_group_deregister_wait
                logger.info("等待 %ds 让旧 Consumer Group 注销...", wait_sec)
                time.sleep(wait_sec)

            # 2) 新作业（新 Group + latest）
            program_args = self._build_program_args(params, fresh_start=True)
            new_job_id = self._submit_job(program_args, None)
            if not new_job_id:
                raise RuntimeError("作业提交失败")

            if not self._wait_job_running(new_job_id):
                raise RuntimeError(f"作业 {new_job_id} 未能进入 RUNNING")

            self._current_job_id = new_job_id
            self._consecutive_failures = 0

            # 提交成功后确认 current group
            self._last_consumer_suffix = self._pending_consumer_suffix
            self._last_consumer_group = self._pending_consumer_group

            # 3) Producer 健康保障
            self._ensure_producer_running()
            if not self.is_producer_healthy():
                logger.warning("Producer 似乎异常，尝试重启...")
                self._start_producer()

            logger.info("✅ 参数应用成功 [%s]: job=%s", mode_str, new_job_id)
            return True

        except Exception as e:
            self._consecutive_failures += 1
            logger.error("❌ 参数应用失败 (%d/%d): %s",
                         self._consecutive_failures,
                         self._max_failures, e)
            return False

        finally:
            if self.ds2_enabled:
                self._release_ds2_lock()

    def get_current_job_id(self) -> Optional[str]:
        job_id = self._find_running_job()
        if job_id:
            self._current_job_id = job_id
        return self._current_job_id

    def is_job_healthy(self) -> bool:
        job_id = self._find_running_job()
        if not job_id:
            return False
        try:
            detail = self._get(f"/jobs/{job_id}")
            for v in detail.get('vertices', []):
                status = v.get('status', '')
                if status != 'RUNNING':
                    logger.warning("算子 %s 状态异常: %s",
                                   v.get('name'), status)
                    return False
            return True
        except Exception:
            return False

    def should_abort(self) -> bool:
        return self._consecutive_failures >= self._max_failures

    # ================================================================
    # 参数构建
    # ================================================================

    def _build_program_args(self, params: Dict[str, int],
                            fresh_start: bool = False) -> str:
        param_to_arg = {
            "checkpoint_interval": "checkpoint-interval",
            "buffer_timeout": "buffer-timeout",
            "watermark_delay": "watermark-delay",
        }

        parts = []
        for key, cli_name in param_to_arg.items():
            if key in params:
                parts.append(f"--{cli_name} {int(params[key])}")

        if fresh_start:
            suffix = f"_{uuid.uuid4().hex[:8]}"
            self._pending_consumer_suffix = suffix
            self._pending_consumer_group = f"{self._consumer_group_prefix}{suffix}"

            parts.append(f"--consumer-group-suffix {suffix}")
            parts.append("--consume-from-latest true")
            logger.info("全新启动: Consumer Group=%s, 从最新位置消费",
                        self._pending_consumer_group)

        default_parallelism = {
            "p-source": 2,
            "p-filter": 4,
            "p-signal": 4,
            "p-feature": 4,
            "p-grid": 4,
            "p-sink": 2,
        }
        for p_name, p_val in default_parallelism.items():
            parts.append(f"--{p_name} {p_val}")

        return " ".join(parts)

    # ================================================================
    # Cancel / Submit
    # ================================================================

    def _cancel_job(self, job_id: str) -> bool:
        logger.info("Cancel 作业: %s", job_id)

        try:
            resp = self._session.patch(f"{self.rest_url}/jobs/{job_id}",
                                       timeout=self._timeout)
            logger.info("Cancel 响应码: %d", resp.status_code)
        except Exception as e:
            logger.error("Cancel 请求异常: %s", e)

        for i in range(self.cancel_timeout // 2):
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

    def _submit_job(self, program_args: str,
                    savepoint_path: Optional[str] = None) -> Optional[str]:
        logger.info("提交作业: savepoint=%s",
                    savepoint_path if savepoint_path else "无（全新启动）")
        logger.info("参数: %s", program_args)

        run_body = {
            "entryClass": self.main_class,
            "programArgs": program_args,
            "allowNonRestoredState": True,
        }
        if savepoint_path:
            run_body["savepointPath"] = savepoint_path

        try:
            resp = self._session.post(
                f"{self.rest_url}/jars/{self._jar_id}/run",
                json=run_body, timeout=self.submit_timeout
            )
            if resp.status_code == 200:
                job_id = resp.json().get("jobid")
                logger.info("✅ 作业提交成功: %s", job_id)
                return job_id

            logger.error("❌ 提交失败 %d: %s", resp.status_code, resp.text[:300])
            return None

        except Exception as e:
            logger.error("提交异常: %s", e)
            return None

    def _ensure_jar_uploaded(self) -> bool:
        if self._jar_id:
            try:
                jars = self._get("/jars")
                for f in jars.get("files", []):
                    if f["id"] == self._jar_id:
                        return True
            except Exception:
                pass
            self._jar_id = None

        try:
            jars = self._get("/jars")
            for f in jars.get("files", []):
                if ('rocketmq-demo' in f['name'].lower()
                        or 'seismic' in f['name'].lower()):
                    self._jar_id = f["id"]
                    logger.info("找到已上传的 JAR: %s", self._jar_id)
                    return True
        except Exception:
            pass

        logger.info("上传 JAR: %s", self.jar_path)
        try:
            upload_cmd = (
                f'curl -s -X POST -H "Expect:" '
                f'-F "jarfile=@{self.jar_path}" '
                f'http://localhost:8081/jars/upload'
            )
            out = self._ssh_exec(upload_cmd)

            if '"status":"success"' in out:
                jars = self._get("/jars")
                for f in jars.get("files", []):
                    if ('rocketmq-demo' in f['name'].lower()
                            or 'seismic' in f['name'].lower()):
                        self._jar_id = f["id"]
                        logger.info("✅ JAR 上传成功: %s", self._jar_id)
                        return True

            logger.error("JAR 上传失败: %s", out[:300])
            return False

        except Exception as e:
            logger.error("JAR 上传异常: %s", e)
            return False

    # ================================================================
    # Consumer Group 清理（可选）
    # ================================================================

    def _cleanup_old_consumer_group(self, group_name: str):
        """
        删除旧 Consumer Group 订阅关系，减少 Broker 历史积压压力。
        失败不影响主流程。
        """
        logger.info("尝试清理旧 Consumer Group: %s", group_name)
        cmd = (
            f"sh {self._rocketmq_home}/bin/mqadmin deleteSubGroup "
            f"-n 192.168.56.151:9876 "
            f"-c {self._rocketmq_cluster} "
            f"-g {group_name} || true"
        )
        try:
            out = self._ssh_exec(cmd)
            logger.info("清理旧 Consumer Group 完成: %s, out=%s",
                        group_name, out[:200] if out else "")
        except Exception as e:
            logger.warning("清理旧 Consumer Group 失败(非致命): %s", e)

    # ================================================================
    # Producer 生命周期
    # ================================================================

    def is_producer_healthy(self) -> bool:
        if not self._producer_enabled:
            return True

        # 1) 进程存活
        if self._producer_process is None:
            return False
        if self._producer_process.poll() is not None:
            return False

        # 2) 日志活跃度检查（防止“进程活着但不发送”）
        now = time.time()
        if not os.path.exists(self._producer_log_path):
            return True

        try:
            mtime = os.path.getmtime(self._producer_log_path)
            size = os.path.getsize(self._producer_log_path)

            # 日志超过 180s 没更新，判定可疑
            if now - mtime > 180:
                logger.warning("Producer 日志超过 180s 未更新: %s",
                               self._producer_log_path)
                return False

            # 周期性检查大小是否增长
            if now - self._producer_last_check_ts > 60:
                if size <= self._producer_last_log_size:
                    logger.warning("Producer 日志大小无增长: %d -> %d",
                                   self._producer_last_log_size, size)
                    # 不直接判死，给一次机会
                self._producer_last_log_size = size
                self._producer_last_check_ts = now

        except Exception:
            pass

        return True

    def _ensure_producer_running(self):
        if not self._producer_enabled:
            return

        if self.is_producer_healthy():
            logger.debug("Producer 健康: PID=%s",
                         self._producer_process.pid if self._producer_process else "N/A")
            return

        logger.warning("Producer 不健康，准备重启...")
        self._start_producer()

    def _start_producer(self):
        if not os.path.exists(self._producer_local_jar):
            logger.error("Producer JAR 不存在: %s", self._producer_local_jar)
            return

        self._stop_producer()

        os.makedirs(self._producer_log_dir, exist_ok=True)

        try:
            self._producer_log_handle = open(self._producer_log_path, 'a', encoding='utf-8')
            cmd = ['java', '-cp', self._producer_local_jar, self._producer_class]

            self._producer_process = subprocess.Popen(
                cmd,
                cwd=self._producer_work_dir,
                stdout=self._producer_log_handle,
                stderr=subprocess.STDOUT,
            )

            time.sleep(3)

            if self._producer_process.poll() is not None:
                logger.error("Producer 启动后立即退出, returncode=%d",
                             self._producer_process.returncode)
                self._producer_process = None
                return

            self._producer_last_check_ts = time.time()
            self._producer_last_log_size = os.path.getsize(self._producer_log_path) \
                if os.path.exists(self._producer_log_path) else 0

            logger.info("✅ Producer 已启动: PID=%d, log=%s",
                        self._producer_process.pid, self._producer_log_path)

        except Exception as e:
            logger.error("Producer 启动失败: %s", e)
            self._producer_process = None

    def _stop_producer(self):
        if self._producer_process is not None and self._producer_process.poll() is None:
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

        if self._producer_log_handle is not None:
            try:
                self._producer_log_handle.close()
            except Exception:
                pass
            self._producer_log_handle = None

    # ================================================================
    # 辅助方法
    # ================================================================

    def _find_running_job(self) -> Optional[str]:
        try:
            jobs = self._get("/jobs")
            for j in jobs.get("jobs", []):
                if j["status"] == "RUNNING":
                    return j["id"]
        except Exception:
            pass
        return None

    def _wait_job_running(self, job_id: str, timeout: int = 90) -> bool:
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
                        logger.error("作业 %s", state)
                    return False
                if i % 5 == 0:
                    logger.debug("等待 RUNNING: [%ds] %s", i * 3, state)
            except Exception:
                pass

        logger.error("等待 RUNNING 超时 (%ds)", timeout)
        return False

    def _wait_slots_available(self, min_slots: int = 4, timeout: int = 30):
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

    # ================================================================
    # DS2 协调
    # ================================================================

    def _acquire_ds2_lock(self):
        if not self.pause_signal_file:
            return

        logger.info("暂停 DS2: 写入 %s", self.pause_signal_file)
        try:
            os.makedirs(os.path.dirname(self.pause_signal_file), exist_ok=True)
            with open(self.pause_signal_file, 'w') as f:
                f.write(str(time.time()))
        except Exception as e:
            logger.warning("写入暂停信号失败: %s", e)
            return

        if self.ds2_lock_file:
            for i in range(60):
                if not os.path.exists(self.ds2_lock_file):
                    logger.info("DS2 锁已释放")
                    return
                time.sleep(2)
                if i % 5 == 0:
                    logger.debug("等待 DS2 释放锁: [%ds]", i * 2)

            logger.warning("等待 DS2 锁超时，继续执行")

    def _release_ds2_lock(self):
        if self.pause_signal_file and os.path.exists(self.pause_signal_file):
            try:
                os.remove(self.pause_signal_file)
                logger.info("DS2 暂停信号已删除")
            except Exception as e:
                logger.warning("删除暂停信号失败: %s", e)

    # ================================================================
    # HTTP / SSH 工具
    # ================================================================

    def _get(self, path: str) -> dict:
        url = f"{self.rest_url}{path}" if not path.startswith("http") else path
        resp = self._session.get(url, timeout=self._timeout)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, body: dict) -> dict:
        url = f"{self.rest_url}{path}" if not path.startswith("http") else path
        resp = self._session.post(url, json=body, timeout=self._timeout)
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

    # ================================================================
    # 清理
    # ================================================================

    def close(self):
        self._stop_producer()
        self._session.close()
        logger.info("JobExecutor 已关闭")

    def __repr__(self) -> str:
        return (f"JobExecutor(rest={self.rest_url}, jar_id={self._jar_id}, "
                f"job={self._current_job_id})")
