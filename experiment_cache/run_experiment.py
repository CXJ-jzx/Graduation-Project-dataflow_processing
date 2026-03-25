#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
缓存淘汰策略对比实验 — 宿主机控制脚本

架构：
  宿主机：运行本脚本 + Producer（Java 本地进程）
  虚拟机集群（node01/02/03）：Flink 集群 + RocketMQ

修复：每轮实验前重启 Flink 集群，确保日志文件干净可采集
"""

import os
import sys
import time
import json
import subprocess
import argparse
import yaml
import requests
from datetime import datetime
from collections import OrderedDict

try:
    import paramiko
except ImportError:
    print("请先安装依赖: pip install -r requirements.txt")
    sys.exit(1)

# ==================== 实验参数 ====================

WARMUP_SECONDS = 20
COLLECT_SECONDS = 40
COOLDOWN_SECONDS = 10

FLINK_JOB_PARAMS = {
    "p-source": 2,
    "p-filter": 4,
    "p-signal": 4,
    "p-feature": 4,
    "p-grid": 4,
    "p-sink": 2,
}

ALL_STRATEGIES = ["fifo", "lru", "lfu", "lru-k", "w-tinylfu"]

PRODUCER_CLASS = "org.jzx.producer.SeismicProducer"

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PRODUCER_WORK_DIR = PROJECT_ROOT

LOCAL_JAR_PATH = os.path.join(
    PROJECT_ROOT, "target",
    "flink-rocketmq-demo-1.0-SNAPSHOT.jar"
)

ROCKETMQ_HOME = "/opt/app/rocketmq-5.3.1"

DEFAULT_CONFIG_PATH = os.path.join(
    PROJECT_ROOT, "ML_Tuner", "ml_tuner_config.yaml"
)

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

CLUSTER_NODES = [
    {"host": "192.168.56.151", "name": "node01"},
    {"host": "192.168.56.152", "name": "node02"},
    {"host": "192.168.56.153", "name": "node03"},
]

FLINK_LOG_PATTERN = "*taskexecutor*.log"

# ==================== 工具类 ====================


def load_config(config_path):
    if not os.path.exists(config_path):
        print(f"[ERROR] 配置文件不存在: {config_path}")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    config = {
        "flink_rest_url": cfg["flink"]["rest_url"],
        "flink_home": cfg["flink"]["flink_home"],
        "remote_jar_path": cfg["flink"]["jar_path"],
        "main_class": cfg["flink"]["main_class"],
        "job_name": cfg["flink"]["job_name"],
        "ssh_host": cfg["ssh"]["host"],
        "ssh_port": cfg["ssh"]["port"],
        "ssh_user": cfg["ssh"]["user"],
        "ssh_password": cfg["ssh"]["password"],
    }

    config["flink_log_dir"] = config["flink_home"] + "/log"
    config["flink_bin"] = config["flink_home"] + "/bin/flink"
    config["namesrv_addr"] = f"{cfg['ssh']['host']}:9876"
    config["topic"] = "seismic-raw"
    config["consumer_group"] = "seismic-flink-consumer-group"

    return config


class FlinkRESTClient:
    def __init__(self, rest_url):
        self.base_url = rest_url.rstrip("/")

    def _get(self, path, timeout=10):
        try:
            resp = requests.get(f"{self.base_url}{path}", timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"  [REST] GET {path} 失败: {e}")
            return None

    def _patch(self, path, timeout=10):
        try:
            resp = requests.patch(f"{self.base_url}{path}", timeout=timeout)
            return resp.status_code
        except Exception as e:
            print(f"  [REST] PATCH {path} 失败: {e}")
            return None

    def get_running_jobs(self):
        data = self._get("/jobs/overview")
        if not data:
            return []
        return [j for j in data.get("jobs", []) if j.get("state") == "RUNNING"]

    def cancel_job(self, job_id):
        print(f"  [REST] 取消 Job: {job_id}")
        self._patch(f"/jobs/{job_id}?mode=cancel")

    def cancel_all_running_jobs(self):
        jobs = self.get_running_jobs()
        if not jobs:
            print("  [REST] 无运行中的 Job")
            return
        for job in jobs:
            self.cancel_job(job["jid"])
        for _ in range(15):
            time.sleep(2)
            if not self.get_running_jobs():
                print("  [REST] 所有 Job 已停止")
                return
        print("  [WARN] 部分 Job 可能仍在运行")

    def wait_for_job_running(self, job_name, timeout=90):
        print(f"  [REST] 等待 Job '{job_name}' 进入 RUNNING...")
        start = time.time()
        while time.time() - start < timeout:
            jobs = self.get_running_jobs()
            for j in jobs:
                if j.get("name", "") == job_name:
                    print(f"  [REST] Job 已运行: {j['jid']}")
                    return j["jid"]
            time.sleep(3)
        print(f"  [WARN] 等待超时（{timeout}s）")
        return None

    def get_overview(self):
        return self._get("/overview")


class SSHController:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.connected = False

    def connect(self):
        print(f"  [SSH] 连接 {self.host}...")
        self.client.connect(
            hostname=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
        )
        self.connected = True

    def exec_cmd(self, cmd, timeout=30, ignore_error=False):
        print(f"  [{self.host}] {cmd}")
        stdin, stdout, stderr = self.client.exec_command(cmd, timeout=timeout)
        exit_code = stdout.channel.recv_exit_status()
        out = stdout.read().decode("utf-8").strip()
        err = stderr.read().decode("utf-8").strip()
        if exit_code != 0 and not ignore_error:
            if err:
                print(f"  [WARN] exit={exit_code}, stderr={err[:200]}")
        return out

    def download_file(self, remote_path, local_path):
        sftp = self.client.open_sftp()
        sftp.get(remote_path, local_path)
        sftp.close()

    def find_files(self, remote_dir, pattern):
        output = self.exec_cmd(
            f"find {remote_dir} -maxdepth 1 -name '{pattern}' -size +0c 2>/dev/null",
            timeout=10, ignore_error=True
        )
        if not output:
            return []
        return [line.strip() for line in output.split("\n") if line.strip()]

    def close(self):
        if self.connected:
            self.client.close()
            self.connected = False


class ClusterSSH:
    def __init__(self, config):
        self.config = config
        self.connections = {}

    def connect_all(self):
        print("[SSH] 连接集群所有节点...")
        for node in CLUSTER_NODES:
            ssh = SSHController(
                host=node["host"],
                port=self.config["ssh_port"],
                user=self.config["ssh_user"],
                password=self.config["ssh_password"],
            )
            try:
                ssh.connect()
                self.connections[node["name"]] = ssh
                print(f"  ✓ {node['name']} ({node['host']}) 已连接")
            except Exception as e:
                print(f"  ✗ {node['name']} ({node['host']}) 连接失败: {e}")

    def get_master(self):
        return self.connections.get("node01")

    def exec_on_all(self, cmd, ignore_error=True):
        results = {}
        for name, ssh in self.connections.items():
            try:
                results[name] = ssh.exec_cmd(cmd, ignore_error=ignore_error)
            except Exception as e:
                print(f"  [WARN] {name} 执行失败: {e}")
                results[name] = ""
        return results

    def restart_flink_cluster(self, config, flink_client):
        """重启 Flink 集群：停止 → 删日志 → 启动 → 等待 TM 注册"""
        master = self.get_master()
        if not master:
            print("  [ERROR] 无法获取主节点")
            return False

        flink_home = config["flink_home"]
        flink_log_dir = config["flink_log_dir"]

        # 1. 停止集群
        print("[STEP] 停止 Flink 集群...")
        master.exec_cmd(f"{flink_home}/bin/stop-cluster.sh",
                        timeout=30, ignore_error=True)
        time.sleep(5)

        # 确认进程已退出
        self.exec_on_all("jps | grep -i taskexecutor || echo 'TM已停止'",
                         ignore_error=True)

        # 2. 删除所有节点的 taskexecutor 日志
        print("[STEP] 删除所有节点的 TaskExecutor 日志文件...")
        self.exec_on_all(
            f"rm -f {flink_log_dir}/flink-*-taskexecutor-*.log "
            f"{flink_log_dir}/flink-*-taskexecutor-*.log.[0-9]* "
            f"{flink_log_dir}/flink-*-taskexecutor-*.out",
            ignore_error=True
        )
        time.sleep(2)

        # 3. 启动集群
        print("[STEP] 启动 Flink 集群...")
        master.exec_cmd(f"{flink_home}/bin/start-cluster.sh",
                        timeout=30, ignore_error=True)

        # 4. 等待 TaskManager 注册
        print("[STEP] 等待 TaskManager 注册...")
        for attempt in range(20):
            time.sleep(3)
            overview = flink_client.get_overview()
            if overview:
                tm_count = overview.get("taskmanagers", 0)
                slots_total = overview.get("slots-total", 0)
                print(f"  第{attempt + 1}次检查: TM={tm_count}, Slots={slots_total}")
                if tm_count >= 3 and slots_total >= 12:
                    print(f"  ✓ Flink 集群就绪: {tm_count} TM, {slots_total} Slots")
                    return True
            else:
                if attempt < 5:
                    print(f"  第{attempt + 1}次检查: REST API 未就绪...")

        print("  [WARN] 等待 TM 注册超时，当前状态继续执行...")
        return True

    def collect_logs(self, flink_log_dir, strategy, save_dir):
        """从所有节点收集 TaskExecutor 日志（含轮转文件）"""
        print("[STEP] 从所有节点收集 TaskExecutor 日志...")
        total_files = 0

        patterns = ["*taskexecutor*.log", "*taskexecutor*.log.[0-9]*"]

        for name, ssh in self.connections.items():
            all_files = []
            for pattern in patterns:
                found = ssh.find_files(flink_log_dir, pattern)
                all_files.extend(found)

            all_files = list(set(all_files))

            if not all_files:
                print(f"  {name}: 无日志文件")
                continue

            for remote_path in sorted(all_files):
                filename = os.path.basename(remote_path)
                local_file = os.path.join(save_dir,
                                          f"{strategy}_{name}_{filename}")
                try:
                    ssh.download_file(remote_path, local_file)
                    file_size = os.path.getsize(local_file)
                    if file_size > 0:
                        print(f"  ✓ {name}: {filename} "
                              f"({file_size / 1024:.1f} KB)")
                        total_files += 1
                    else:
                        os.remove(local_file)
                except Exception as e:
                    print(f"  ✗ {name}: {filename} 下载失败: {e}")

        print(f"  共收集 {total_files} 个有效日志文件")
        return total_files

    def close_all(self):
        for name, ssh in self.connections.items():
            ssh.close()
        self.connections.clear()
        print("[SSH] 所有节点连接已关闭")


class LocalProducer:
    def __init__(self, jar_path, work_dir, log_dir):
        self.jar_path = jar_path
        self.work_dir = work_dir
        self.log_dir = log_dir
        self.process = None
        self.log_file_handle = None

    def start(self, strategy_name=""):
        print("[STEP] 启动 Producer（宿主机本地）...")

        if not os.path.exists(self.jar_path):
            print(f"  [ERROR] JAR 不存在: {self.jar_path}")
            return False

        log_filename = (f"producer_{strategy_name}_"
                        f"{datetime.now().strftime('%H%M%S')}.log")
        log_path = os.path.join(self.log_dir, log_filename)
        self.log_file_handle = open(log_path, "w", encoding="utf-8")

        cmd = ["java", "-cp", self.jar_path, PRODUCER_CLASS]

        print(f"  命令: {' '.join(cmd)}")
        print(f"  工作目录: {self.work_dir}")
        print(f"  日志: {log_path}")

        self.process = subprocess.Popen(
            cmd,
            cwd=self.work_dir,
            stdout=self.log_file_handle,
            stderr=subprocess.STDOUT,
        )

        time.sleep(3)

        if self.process.poll() is not None:
            print(f"  [ERROR] Producer 启动后退出，"
                  f"exit_code={self.process.returncode}")
            print(f"  查看日志: {log_path}")
            return False

        print(f"  ✓ Producer 已启动, PID={self.process.pid}")
        return True

    def stop(self):
        print("[STEP] 停止 Producer...")
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
                print(f"  ✓ Producer 已停止")
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
                print(f"  ✓ Producer 已强制停止")
        else:
            print("  Producer 未在运行")

        if self.log_file_handle:
            self.log_file_handle.close()
            self.log_file_handle = None

    def is_running(self):
        return self.process is not None and self.process.poll() is None


# ==================== 实验流程 ====================

def reset_rocketmq(master_ssh: SSHController, config: dict):
    print("[STEP] 重置 RocketMQ 消费进度...")
    master_ssh.exec_cmd(
        f"{ROCKETMQ_HOME}/bin/mqadmin resetOffsetByTime "
        f"-n {config['namesrv_addr']} -t {config['topic']} "
        f"-g {config['consumer_group']} -s -1",
        timeout=15, ignore_error=True
    )
    time.sleep(2)


def submit_flink_job(master_ssh: SSHController,
                     flink_client: FlinkRESTClient,
                     config: dict, strategy: str):
    print(f"[STEP] 提交 Flink Job（策略: {strategy}）...")

    params_str = " ".join(f"--{k} {v}" for k, v in FLINK_JOB_PARAMS.items())
    cmd = (f"{config['flink_bin']} run -d "
           f"-c {config['main_class']} {config['remote_jar_path']} "
           f"{params_str} --cache-strategy {strategy}")

    output = master_ssh.exec_cmd(cmd, timeout=60)
    print(f"  提交输出: {output[:300]}")

    job_id = flink_client.wait_for_job_running(config["job_name"], timeout=90)
    return job_id


def print_progress(phase, total_seconds):
    interval = 30 if total_seconds > 60 else 10
    for remaining in range(total_seconds, 0, -interval):
        elapsed = total_seconds - remaining
        bar_len = 30
        filled = int(bar_len * elapsed / total_seconds)
        bar = "█" * filled + "░" * (bar_len - filled)
        print(f"\r  [{phase}] {bar} {remaining:>4d}s 剩余",
              end="", flush=True)
        time.sleep(min(interval, remaining))
    print(f"\r  [{phase}] {'█' * 30} 完成!{'':>20s}")


def run_single_experiment(cluster: ClusterSSH,
                          flink_client: FlinkRESTClient,
                          producer: LocalProducer, config: dict,
                          strategy: str, save_dir: str,
                          exp_no: int, total_exp: int):
    """执行单组实验"""
    total_time = WARMUP_SECONDS + COLLECT_SECONDS
    master = cluster.get_master()

    print()
    print("╔" + "═" * 63 + "╗")
    print(f"║  实验 E{exp_no}/{total_exp}: 策略 = {strategy.upper():<10s}"
          f"  时长 = {total_time}s ({total_time // 60}min)"
          f"{'':>13s}║")
    print("╚" + "═" * 63 + "╝")

    # 1. 清理残留 Producer
    producer.stop()

    # 2. ★ 重启 Flink 集群（替代原来的 truncate）
    cluster.restart_flink_cluster(config, flink_client)

    # 3. 重置 RocketMQ
    reset_rocketmq(master, config)
    time.sleep(2)

    # 4. 提交 Flink Job
    job_id = submit_flink_job(master, flink_client, config, strategy)
    if not job_id:
        print("  [ERROR] Flink Job 未能启动，跳过此组")
        return False

    # 5. 启动 Producer
    success = producer.start(strategy_name=strategy)
    if not success:
        flink_client.cancel_all_running_jobs()
        return False

    # 6. 预热
    print(f"\n[PHASE 1/3] 预热（{WARMUP_SECONDS}s）— 数据不计入统计")
    print_progress("预热", WARMUP_SECONDS)

    if not producer.is_running():
        print("\n  [WARN] Producer 在预热阶段退出")

    # 7. 有效采集
    print(f"\n[PHASE 2/3] 有效采集（{COLLECT_SECONDS}s）")
    print_progress("采集", COLLECT_SECONDS)

    running = flink_client.get_running_jobs()
    if not running:
        print("\n  [WARN] Flink Job 在采集阶段已停止")

    if not producer.is_running():
        print("\n  [WARN] Producer 在采集阶段退出")

    # 8. 停止 Producer
    print(f"\n[PHASE 3/3] 停止并收集数据")
    producer.stop()

    # 9. 取消 Flink Job（触发 close() → 写入关闭日志）
    print("[STEP] 取消 Flink Job（触发 close 日志）...")
    flink_client.cancel_all_running_jobs()

    # 10. 等待日志刷盘
    print(f"  等待 {COOLDOWN_SECONDS}s 确保日志刷盘...")
    time.sleep(COOLDOWN_SECONDS)

    # 11. 从所有节点收集日志
    collected = cluster.collect_logs(config["flink_log_dir"],
                                     strategy, save_dir)
    if collected == 0:
        print("  [WARN] 未收集到任何日志文件")

    print(f"\n  ✓ 实验 E{exp_no}（{strategy.upper()}）完成")
    return True


def main():
    parser = argparse.ArgumentParser(description="缓存淘汰策略对比实验")
    parser.add_argument("--config", type=str, default=DEFAULT_CONFIG_PATH,
                        help="ml_tuner_config.yaml 路径")
    parser.add_argument("--strategy", type=str, default=None,
                        help="只执行指定策略（fifo/lru/lfu/lru-k/w-tinylfu）")
    parser.add_argument("--skip-run", action="store_true",
                        help="跳过实验，直接解析日志并绘图")
    parser.add_argument("--results-dir", type=str, default=None,
                        help="指定已有结果目录（配合 --skip-run）")
    parser.add_argument("--mock", action="store_true",
                        help="模拟数据预览图表")
    parser.add_argument("--jar", type=str, default=None,
                        help="覆盖宿主机本地 JAR 路径")
    parser.add_argument("--producer-dir", type=str, default=None,
                        help="覆盖 Producer 工作目录")
    args = parser.parse_args()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ===== 模式1：模拟数据预览 =====
    if args.mock:
        from plot_results import plot_all
        mock_data = {
            "FIFO":      {"hitRate": 16.2,
                          "tierRates": {"T1": 20.1, "T2": 17.8, "T3": 14.5, "T4": 9.8},
                          "hitrate_series": [5, 10, 13, 15, 16, 16, 16, 16, 16, 16]},
            "LRU":       {"hitRate": 27.5,
                          "tierRates": {"T1": 52.3, "T2": 24.1, "T3": 14.8, "T4": 7.5},
                          "hitrate_series": [8, 15, 20, 24, 26, 27, 27, 28, 27, 27]},
            "LFU":       {"hitRate": 31.8,
                          "tierRates": {"T1": 97.2, "T2": 8.5, "T3": 3.2, "T4": 1.1},
                          "hitrate_series": [10, 18, 24, 28, 30, 31, 32, 32, 32, 32]},
            "LRU-K":     {"hitRate": 35.6,
                          "tierRates": {"T1": 85.0, "T2": 30.2, "T3": 12.1, "T4": 5.0},
                          "hitrate_series": [9, 17, 25, 30, 33, 35, 35, 36, 35, 36]},
            "W-TINYLFU": {"hitRate": 42.3,
                          "tierRates": {"T1": 98.1, "T2": 41.5, "T3": 18.2, "T4": 4.3},
                          "hitrate_series": [12, 22, 30, 36, 39, 41, 42, 42, 42, 42]},
        }
        mock_dir = os.path.join(RESULTS_DIR, "mock")
        plot_all(mock_data, mock_dir)
        return

    # ===== 模式2：跳过实验，直接解析绘图 =====
    if args.skip_run:
        target_dir = args.results_dir or RESULTS_DIR
        print(f"跳过实验，解析: {target_dir}")
        from parse_log import parse_all_logs
        from plot_results import plot_all
        data = parse_all_logs(target_dir)
        if data:
            plot_all(data, target_dir)
        else:
            print("[ERROR] 未解析到有效数据")
        return

    # ===== 模式3：执行实验 =====
    print(f"加载配置: {args.config}")
    config = load_config(args.config)

    local_jar = args.jar or LOCAL_JAR_PATH
    producer_dir = args.producer_dir or PRODUCER_WORK_DIR

    strategies = [args.strategy] if args.strategy else ALL_STRATEGIES
    save_dir = os.path.join(RESULTS_DIR, timestamp)
    os.makedirs(save_dir, exist_ok=True)

    # 每组多 ~30s 重启开销
    total_time_per = (WARMUP_SECONDS + COLLECT_SECONDS
                      + COOLDOWN_SECONDS + 60)
    total_time_all = len(strategies) * total_time_per

    nodes_str = ", ".join(n["name"] for n in CLUSTER_NODES)

    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║              缓存淘汰策略对比实验                                ║")
    print("╠══════════════════════════════════════════════════════════════════╣")
    print(f"║  时间戳:       {timestamp}                                   ║")
    print(f"║  策略:         {', '.join(s.upper() for s in strategies):<46s} ║")
    print(f"║  每组流程:     重启集群→预热{WARMUP_SECONDS}s→采集{COLLECT_SECONDS}s→冷却{COOLDOWN_SECONDS}s{'':>16s} ║")
    print(f"║  预计总时:     {total_time_all}s ≈ {total_time_all // 60}min{'':>38s} ║")
    print("╠══════════════════════════════════════════════════════════════════╣")
    print(f"║  [集群] 节点:         {nodes_str:<40s}   ║")
    print(f"║  [集群] Flink REST:   {config['flink_rest_url']:<40s}   ║")
    print(f"║  [集群] Job JAR:      {os.path.basename(config['remote_jar_path']):<40s}   ║")
    print(f"║  [宿主机] Producer:   {PRODUCER_CLASS:<40s}   ║")
    print(f"║  [宿主机] 本地 JAR:   {os.path.basename(local_jar):<40s}   ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print()

    # 前置检查
    print("[CHECK] 前置检查...")

    if not os.path.exists(local_jar):
        print(f"  ✗ 宿主机 JAR 不存在: {local_jar}")
        sys.exit(1)
    jar_mb = os.path.getsize(local_jar) / 1024 / 1024
    print(f"  ✓ 宿主机 JAR: ({jar_mb:.1f} MB)")

    dataset_dir = os.path.join(producer_dir, "dataset")
    if os.path.isdir(dataset_dir):
        fc = len([f for f in os.listdir(dataset_dir)
                  if os.path.isdir(os.path.join(dataset_dir, f))])
        print(f"  ✓ dataset 目录: {fc} 个节点")
    else:
        print(f"  [WARN] dataset 不存在: {dataset_dir}")

    try:
        java_ver = subprocess.check_output(
            ["java", "-version"], stderr=subprocess.STDOUT
        ).decode()
        print(f"  ✓ Java: {java_ver.split(chr(10))[0]}")
    except Exception:
        print("  ✗ java 不可用")
        sys.exit(1)

    # 保存实验配置
    exp_config = {
        "timestamp": timestamp,
        "strategies": strategies,
        "warmup_seconds": WARMUP_SECONDS,
        "collect_seconds": COLLECT_SECONDS,
        "cooldown_seconds": COOLDOWN_SECONDS,
        "flink_job_params": FLINK_JOB_PARAMS,
        "cluster_nodes": [n["name"] for n in CLUSTER_NODES],
        "local_jar": local_jar,
        "remote_jar": config["remote_jar_path"],
        "cluster_restart": True,
    }
    with open(os.path.join(save_dir, "experiment_config.json"),
              "w", encoding="utf-8") as f:
        json.dump(exp_config, f, indent=2, ensure_ascii=False)

    input("\n按 Enter 开始实验（Ctrl+C 取消）...")

    # 初始化
    flink_client = FlinkRESTClient(config["flink_rest_url"])
    cluster = ClusterSSH(config)
    producer = LocalProducer(local_jar, producer_dir, save_dir)

    try:
        cluster.connect_all()

        master = cluster.get_master()
        if not master:
            print("[ERROR] 无法连接主节点 node01")
            sys.exit(1)

        # 检查虚拟机上 JAR
        jar_check = master.exec_cmd(
            f"test -f {config['remote_jar_path']} && echo OK || echo MISSING",
            timeout=5, ignore_error=True
        )
        if "MISSING" in jar_check:
            print(f"  ✗ 虚拟机 JAR 不存在: {config['remote_jar_path']}")
            sys.exit(1)
        print(f"  ✓ 虚拟机 JAR 已确认")

        print()

        # 逐组执行
        results = OrderedDict()
        for i, strategy in enumerate(strategies):
            success = run_single_experiment(
                cluster, flink_client, producer, config,
                strategy, save_dir,
                exp_no=i + 1, total_exp=len(strategies)
            )
            results[strategy] = success

        # 全部完成
        print()
        print("═" * 65)
        print("  全部实验完成！解析日志并生成图表...")
        print("═" * 65)

        print("\n  执行结果:")
        for strategy, success in results.items():
            status = "✓ 成功" if success else "✗ 失败"
            print(f"    {strategy.upper():<10s} {status}")

        from parse_log import parse_all_logs
        from plot_results import plot_all

        data = parse_all_logs(save_dir)
        if data:
            plot_all(data, save_dir)

            print()
            print("┌────────────┬──────────┬────────┬────────┬────────┬────────┐")
            print("│   策略     │ 总命中率  │  T1    │  T2    │  T3    │  T4    │")
            print("├────────────┼──────────┼────────┼────────┼────────┼────────┤")
            for s_name, s_data in data.items():
                tiers = s_data.get("tierRates", {})
                marker = " ◀" if s_name == "W-TINYLFU" else ""
                print(f"│ {s_name:<10s} │ "
                      f"{s_data['hitRate']:>6.1f}%  │"
                      f" {tiers.get('T1', 0):>5.1f}% │"
                      f" {tiers.get('T2', 0):>5.1f}% │"
                      f" {tiers.get('T3', 0):>5.1f}% │"
                      f" {tiers.get('T4', 0):>5.1f}% │{marker}")
            print("└────────────┴──────────┴────────┴────────┴────────┴────────┘")
        else:
            print("[WARN] 未能解析到有效数据")

        print(f"\n全部结果保存在: {save_dir}/")

    except KeyboardInterrupt:
        print("\n\n[中断] 用户取消...")
        try:
            producer.stop()
            flink_client.cancel_all_running_jobs()
        except:
            pass
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        try:
            producer.stop()
            flink_client.cancel_all_running_jobs()
        except:
            pass
    finally:
        producer.stop()
        cluster.close_all()


if __name__ == "__main__":
    main()
