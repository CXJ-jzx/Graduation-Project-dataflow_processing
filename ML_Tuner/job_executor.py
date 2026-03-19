"""
Flink 作业执行器 (REST API 版)

修改说明:
1. Cancel: REST PATCH /jobs/{id}（已验证）
2. 提交:  REST /jars/{jar_id}/run（绕过 CLI commons-cli bug）
3. JAR 上传: SSH curl 到 localhost:8081/jars/upload
4. DS2 协调: 文件锁（同一台 Windows 宿主机，路径互通）

验证记录:
  Savepoint  ✅  瞬间完成
  Cancel     ✅  REST PATCH 202
  Submit     ✅  REST /jars/run 200
  JAR Upload ✅  SSH curl 上传
"""

import os
import time
import requests
import paramiko
import logging
import yaml
from typing import Optional, Dict

logger = logging.getLogger("ml_tuner.job_executor")


class JobExecutor:
    """通过 REST API 管理 Flink 作业的生命周期"""

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
        self.jar_path = flink_cfg.get('jar_path',
                                       '/opt/flink_jobs/flink-rocketmq-demo-1.0-SNAPSHOT.jar')
        self.main_class = flink_cfg.get('main_class', 'org.jzx.SeismicStreamJob')
        self.job_name = flink_cfg.get('job_name', 'SeismicStreamJob')
        self.savepoint_dir = flink_cfg.get('savepoint_dir',
                                            'hdfs://node01:9000/flink/savepoints')

        # SSH 配置（仅用于 JAR 上传）
        ssh_cfg = cfg.get('ssh', {})
        self.ssh_host = ssh_cfg.get('host', '192.168.56.151')
        self.ssh_port = ssh_cfg.get('port', 22)
        self.ssh_user = ssh_cfg.get('user', 'root')
        self.ssh_password = ssh_cfg.get('password', '')

        # DS2 协调（文件锁）
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

        # 状态
        self._jar_id: Optional[str] = None
        self._current_job_id: Optional[str] = None
        self._session = requests.Session()
        self._session.headers.update({'Accept': 'application/json'})
        self._timeout = 15

        # 失败计数
        self._consecutive_failures = 0
        self._max_failures = 3

        logger.info("JobExecutor 初始化: rest=%s, jar=%s, main=%s",
                     self.rest_url, self.jar_path, self.main_class)

    # ================================================================
    # 公开接口
    # ================================================================

    def apply_parameters(self, params: Dict[str, int]) -> bool:
        """
        应用新参数：Savepoint → Cancel → Resubmit

        Args:
            params: {"checkpoint_interval": 30000,
                     "buffer_timeout": 100,
                     "watermark_delay": 5000}

        Returns:
            bool: 是否成功
        """
        logger.info("=" * 50)
        logger.info("开始应用新参数: %s", params)
        logger.info("=" * 50)

        try:
            # 1. 暂停 DS2
            if self.ds2_enabled:
                self._acquire_ds2_lock()

            # 2. 确保 JAR 已上传
            if not self._ensure_jar_uploaded():
                raise RuntimeError("JAR 上传失败")

            # 3. 找到当前运行中的作业
            current_job = self._find_running_job()

            savepoint_path = None
            if current_job:
                # 4. Savepoint
                savepoint_path = self._trigger_savepoint(current_job)

                # 5. Cancel
                self._cancel_job(current_job)

                # 6. 等待资源释放
                self._wait_slots_available()

            # 7. 构建参数并提交
            program_args = self._build_program_args(params)
            new_job_id = self._submit_job(program_args, savepoint_path)

            if not new_job_id:
                raise RuntimeError("作业提交失败")

            # 8. 等待 RUNNING
            if not self._wait_job_running(new_job_id):
                raise RuntimeError(f"作业 {new_job_id} 未能进入 RUNNING 状态")

            self._current_job_id = new_job_id
            self._consecutive_failures = 0

            logger.info("✅ 参数应用成功: job=%s", new_job_id)
            return True

        except Exception as e:
            self._consecutive_failures += 1
            logger.error("❌ 参数应用失败 (%d/%d): %s",
                         self._consecutive_failures, self._max_failures, e)
            return False

        finally:
            # 释放 DS2 锁
            if self.ds2_enabled:
                self._release_ds2_lock()

    def get_current_job_id(self) -> Optional[str]:
        """获取当前作业 ID（优先从 REST 查询）"""
        job_id = self._find_running_job()
        if job_id:
            self._current_job_id = job_id
        return self._current_job_id

    def is_job_healthy(self) -> bool:
        """检查当前作业是否健康运行"""
        job_id = self._find_running_job()
        if not job_id:
            return False

        try:
            detail = self._get(f"/jobs/{job_id}")
            # 检查所有 vertex 是否都是 RUNNING
            for v in detail.get('vertices', []):
                status = v.get('status', '')
                if status != 'RUNNING':
                    logger.warning("算子 %s 状态异常: %s", v.get('name'), status)
                    return False
            return True
        except Exception:
            return False

    def should_abort(self) -> bool:
        """是否应该中止（连续失败太多次）"""
        return self._consecutive_failures >= self._max_failures

    # ================================================================
    # Savepoint
    # ================================================================

    def _trigger_savepoint(self, job_id: str) -> Optional[str]:
        """触发 Savepoint 并等待完成"""
        logger.info("触发 Savepoint: job=%s", job_id)

        resp = self._post(f"/jobs/{job_id}/savepoints", {
            "cancel-job": False,
            "target-directory": self.savepoint_dir
        })

        trigger_id = resp.get("request-id")
        if not trigger_id:
            logger.error("Savepoint 触发失败: %s", resp)
            return None

        # 等待完成
        for i in range(self.savepoint_timeout // 2):
            time.sleep(2)
            status = self._get(f"/jobs/{job_id}/savepoints/{trigger_id}")
            s = status.get("status", {}).get("id", "UNKNOWN")

            if s == "COMPLETED":
                path = status["operation"]["location"]
                logger.info("✅ Savepoint 完成: %s", path)
                return path

            if s == "FAILED":
                cause = status.get("operation", {}).get("failure-cause", {})
                logger.error("❌ Savepoint 失败: %s", str(cause)[:300])
                return None

        logger.error("Savepoint 超时 (%ds)", self.savepoint_timeout)
        return None

    # ================================================================
    # Cancel
    # ================================================================

    def _cancel_job(self, job_id: str) -> bool:
        """Cancel 作业（REST PATCH，已验证可用）"""
        logger.info("Cancel 作业: %s", job_id)

        try:
            resp = self._session.patch(
                f"{self.rest_url}/jobs/{job_id}",
                timeout=self._timeout
            )
            logger.info("Cancel 响应码: %d", resp.status_code)

            if resp.status_code not in [200, 202]:
                logger.warning("REST Cancel 返回 %d，尝试 stop-with-savepoint",
                               resp.status_code)
                self._post(f"/jobs/{job_id}/savepoints", {
                    "cancel-job": True,
                    "target-directory": self.savepoint_dir
                })
        except Exception as e:
            logger.error("Cancel 请求异常: %s", e)

        # 等待作业停止
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

    # ================================================================
    # 提交作业（REST API）
    # ================================================================

    def _submit_job(self, program_args: str,
                     savepoint_path: Optional[str] = None) -> Optional[str]:
        """
        通过 REST /jars/{id}/run 提交作业

        已验证: 绕过 CLI commons-cli 版本冲突
        """
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
                json=run_body,
                timeout=self.submit_timeout
            )

            if resp.status_code == 200:
                job_id = resp.json().get("jobid")
                logger.info("✅ 作业提交成功: %s", job_id)
                return job_id

            logger.warning("提交返回 %d: %s", resp.status_code, resp.text[:300])

            # 如果带 savepoint 失败，尝试不带
            if savepoint_path:
                logger.info("从 Savepoint 恢复失败，尝试全新启动...")
                run_body.pop("savepointPath", None)
                resp = self._session.post(
                    f"{self.rest_url}/jars/{self._jar_id}/run",
                    json=run_body,
                    timeout=self.submit_timeout
                )
                if resp.status_code == 200:
                    job_id = resp.json().get("jobid")
                    logger.info("✅ 全新启动成功: %s", job_id)
                    return job_id

            logger.error("❌ 所有提交方式失败")
            return None

        except Exception as e:
            logger.error("提交异常: %s", e)
            return None

    def _ensure_jar_uploaded(self) -> bool:
        """确保 JAR 已上传到 Flink REST API"""
        # 检查缓存的 jar_id 是否仍有效
        if self._jar_id:
            try:
                jars = self._get("/jars")
                for f in jars.get("files", []):
                    if f["id"] == self._jar_id:
                        return True
            except Exception:
                pass
            self._jar_id = None

        # 检查是否已上传过
        try:
            jars = self._get("/jars")
            for f in jars.get("files", []):
                if 'rocketmq-demo' in f['name'].lower() or 'seismic' in f['name'].lower():
                    self._jar_id = f["id"]
                    logger.info("找到已上传的 JAR: %s", self._jar_id)
                    return True
        except Exception:
            pass

        # 通过 SSH 在 node01 上 curl 上传
        logger.info("上传 JAR: %s", self.jar_path)
        try:
            upload_cmd = (
                f'curl -s -X POST '
                f'-H "Expect:" '
                f'-F "jarfile=@{self.jar_path}" '
                f'http://localhost:8081/jars/upload'
            )
            out = self._ssh_exec(upload_cmd)

            if '"status":"success"' in out:
                # 重新查询获取 jar_id
                jars = self._get("/jars")
                for f in jars.get("files", []):
                    if 'rocketmq-demo' in f['name'].lower() or 'seismic' in f['name'].lower():
                        self._jar_id = f["id"]
                        logger.info("✅ JAR 上传成功: %s", self._jar_id)
                        return True

            logger.error("JAR 上传失败: %s", out[:300])
            return False

        except Exception as e:
            logger.error("JAR 上传异常: %s", e)
            return False

    # ================================================================
    # 辅助方法
    # ================================================================

    def _find_running_job(self) -> Optional[str]:
        """查找运行中的作业"""
        try:
            jobs = self._get("/jobs")
            for j in jobs.get("jobs", []):
                if j["status"] == "RUNNING":
                    return j["id"]
        except Exception:
            pass
        return None

    def _wait_job_running(self, job_id: str, timeout: int = 90) -> bool:
        """等待作业进入 RUNNING 状态"""
        for i in range(timeout // 3):
            time.sleep(3)
            try:
                state = self._get(f"/jobs/{job_id}").get("state", "UNKNOWN")
                if state == "RUNNING":
                    logger.info("作业 RUNNING: %s", job_id)
                    return True
                if state in ["FAILED", "CANCELED", "CANCELLED"]:
                    # 获取异常信息
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
        """等待足够的 slot 释放"""
        for i in range(timeout // 2):
            time.sleep(2)
            try:
                overview = self._get("/overview")
                available = overview.get("slots-available", 0)
                if available >= min_slots:
                    logger.debug("Slots 可用: %d", available)
                    return
            except Exception:
                pass

        logger.warning("等待 slot 释放超时，继续尝试提交")

    def _build_program_args(self, params: Dict[str, int]) -> str:
        """
        构建程序参数字符串

        params 中的 key（Python 风格）映射到 CLI 参数（横杠风格）:
          checkpoint_interval → --checkpoint-interval
          buffer_timeout      → --buffer-timeout
          watermark_delay     → --watermark-delay
        """
        # 参数名映射
        param_to_arg = {
            "checkpoint_interval": "checkpoint-interval",
            "buffer_timeout": "buffer-timeout",
            "watermark_delay": "watermark-delay",
        }

        parts = []
        for key, cli_name in param_to_arg.items():
            if key in params:
                parts.append(f"--{cli_name} {int(params[key])}")

        # 并行度参数（保持当前值，由 DS2 控制）
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
    # DS2 协调
    # ================================================================

    def _acquire_ds2_lock(self):
        """暂停 DS2 并等待它释放锁"""
        if not self.pause_signal_file:
            return

        logger.info("暂停 DS2: 写入 %s", self.pause_signal_file)

        # 写入暂停信号
        try:
            os.makedirs(os.path.dirname(self.pause_signal_file), exist_ok=True)
            with open(self.pause_signal_file, 'w') as f:
                f.write(str(time.time()))
        except Exception as e:
            logger.warning("写入暂停信号失败: %s", e)
            return

        # 等待 DS2 释放锁
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
        """恢复 DS2"""
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
        """SSH 执行命令（仅用于 JAR 上传）"""
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(self.ssh_host, self.ssh_port,
                       self.ssh_user, self.ssh_password, timeout=10)
        stdin, stdout, stderr = client.exec_command(cmd, timeout=60)
        out = stdout.read().decode('utf-8').strip()
        client.close()
        return out

    # ================================================================
    # 清理
    # ================================================================

    def close(self):
        self._session.close()
        logger.info("JobExecutor 已关闭")

    def __repr__(self) -> str:
        return (f"JobExecutor(rest={self.rest_url}, "
                f"jar_id={self._jar_id}, "
                f"job={self._current_job_id})")
