#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
缓存僵化（Cache Stagnation）对比实验 v4

支持两种运行模式：
  QUICK_MODE=True:  快速验证（每轮~35s，总共~3min），确认数据链路正确
  QUICK_MODE=False: 正式实验（每轮~220s，总共~17min），用于论文数据

修复：每轮实验前重启 Flink 集群，确保日志文件干净可采集
"""

import os
import sys
import time
import json
import re
import glob
import subprocess
from datetime import datetime, timedelta
from collections import OrderedDict, defaultdict
from typing import Optional

try:
    import paramiko
except ImportError:
    print("请先安装依赖: pip install -r requirements.txt")
    sys.exit(1)

from run_experiment import (
    load_config,
    FlinkRESTClient,
    ClusterSSH,
    SSHController,
    reset_rocketmq,
    print_progress,
    DEFAULT_CONFIG_PATH,
    RESULTS_DIR,
    CLUSTER_NODES,
    FLINK_LOG_PATTERN,
    FLINK_JOB_PARAMS,
)


# ==================== 快速验证模式 ====================
QUICK_MODE = False

# ==================== 僵化实验参数 ====================
if QUICK_MODE:
    WARMUP_SECONDS   = 20
    PHASE1_SECONDS   = 30
    PHASE2_SECONDS   = 40
    COOLDOWN_SECONDS = 5
    STAGNATION_STRATEGIES = ["lfu", "w-tinylfu"]
else:
    WARMUP_SECONDS   = 30
    PHASE1_SECONDS   = 60
    PHASE2_SECONDS   = 120
    COOLDOWN_SECONDS = 10
    STAGNATION_STRATEGIES = ["lfu", "w-tinylfu", "lru-k"]


# Producer 切换时间 = WARMUP + PHASE1
PRODUCER_SWITCH_TIME = WARMUP_SECONDS + PHASE1_SECONDS
# Producer 总时长 = WARMUP + PHASE1 + PHASE2 + 余量
PRODUCER_DURATION = WARMUP_SECONDS + PHASE1_SECONDS + PHASE2_SECONDS + 30

# 僵化实验 Producer 主类
STAGNATION_PRODUCER_CLASS = "org.jzx.producer.StagnationProducer"

PROJECT_ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_JAR_PATH    = os.path.join(PROJECT_ROOT, "target",
                                  "flink-rocketmq-demo-1.0-SNAPSHOT.jar")
PRODUCER_WORK_DIR = PROJECT_ROOT

# 6 组子分层标签
STAG_GROUPS = ['T1a', 'T1b', 'T2', 'T3', 'T4a', 'T4b']

# 漂移时间偏移（相对于该策略第一条日志的秒数）
PHASE_SWITCH_OFFSET_SEC = WARMUP_SECONDS + PHASE1_SECONDS - 5


# ==================== Flink Job 提交 ====================

def submit_flink_job_stagnation(master: SSHController,
                                flink_client: FlinkRESTClient,
                                config: dict,
                                strategy: str) -> Optional[str]:
    """提交僵化实验 Flink Job"""
    flink_bin  = config["flink_bin"]
    remote_jar = config["remote_jar_path"]
    main_class = config["main_class"]
    job_name   = config.get("job_name", "SeismicStreamJob")

    params_str = " ".join(
        f"--{k} {v}" for k, v in FLINK_JOB_PARAMS.items()
    )

    cmd = (
        f"{flink_bin} run -d "
        f"-c {main_class} "
        f"{remote_jar} "
        f"{params_str} "
        f"--cache-strategy {strategy} "
        f"--stagnation-mode true"
    )

    print(f"[STEP] 提交 Flink Job: 策略={strategy.upper()}, stagnation-mode=true")
    print(f"  命令: {cmd}")

    stdout_text = master.exec_cmd(cmd, timeout=60, ignore_error=True)

    job_id_match = re.search(r'JobID\s+([a-f0-9]{32})', stdout_text)
    if not job_id_match:
        job_id_match = re.search(r'([a-f0-9]{32})', stdout_text)

    if not job_id_match:
        print(f"  ✗ 未找到 JobID，输出: {stdout_text[:300]}")
        return None

    job_id = job_id_match.group(1)
    print(f"  ✓ Job 已提交: {job_id}")

    print(f"  等待 Job 进入 RUNNING...")
    job_id_found = flink_client.wait_for_job_running(job_name, timeout=90)
    if job_id_found:
        return job_id_found

    print(f"  [WARN] 按 job_name 未找到，尝试直接检查 JobID...")
    for attempt in range(20):
        time.sleep(3)
        running_jobs = flink_client.get_running_jobs()
        for j in running_jobs:
            if j.get("jid", "").startswith(job_id[:8]):
                print(f"  ✓ Job 已 RUNNING: {j['jid']}")
                return j["jid"]
        if attempt % 3 == 0:
            print(f"  等待中... (attempt={attempt+1})")

    print("  ✗ 等待 RUNNING 超时")
    return None


# ==================== Producer 管理 ====================

class StagnationProducer:
    """僵化实验专用 Producer（宿主机本地进程）"""

    def __init__(self, jar_path: str, work_dir: str, log_dir: str):
        self.jar_path        = jar_path
        self.work_dir        = work_dir
        self.log_dir         = log_dir
        self.process         = None
        self.log_file_handle = None

    def start(self, strategy_name: str = "") -> bool:
        print("[STEP] 启动 StagnationProducer...")
        print(f"  切换时间: {PRODUCER_SWITCH_TIME}s, 总时长: {PRODUCER_DURATION}s")

        if not os.path.exists(self.jar_path):
            print(f"  [ERROR] JAR 不存在: {self.jar_path}")
            return False

        log_filename = (f"stag_producer_{strategy_name}_"
                        f"{datetime.now().strftime('%H%M%S')}.log")
        log_path = os.path.join(self.log_dir, log_filename)
        self.log_file_handle = open(log_path, "w", encoding="utf-8")

        cmd = [
            "java", "-cp", self.jar_path, STAGNATION_PRODUCER_CLASS,
            "--switch-time", str(PRODUCER_SWITCH_TIME),
            "--duration", str(PRODUCER_DURATION),
        ]
        print(f"  命令:    {' '.join(cmd)}")
        print(f"  工作目录: {self.work_dir}")
        print(f"  日志:    {log_path}")

        self.process = subprocess.Popen(
            cmd,
            cwd=self.work_dir,
            stdout=self.log_file_handle,
            stderr=subprocess.STDOUT,
        )

        time.sleep(5)
        if self.process.poll() is not None:
            print(f"  [ERROR] Producer 启动后立即退出，"
                  f"exit_code={self.process.returncode}")
            print(f"  请查看日志: {log_path}")
            return False

        print(f"  ✓ StagnationProducer 已启动, PID={self.process.pid}")
        return True

    def stop(self):
        print("[STEP] 停止 StagnationProducer...")
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
                print("  ✓ 已停止")
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
                print("  ✓ 已强制停止")
        else:
            print("  未在运行")

        if self.log_file_handle:
            self.log_file_handle.close()
            self.log_file_handle = None

    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None


# ==================== 单组实验流程 ====================

def run_single_stagnation(cluster: ClusterSSH,
                          flink_client: FlinkRESTClient,
                          config: dict,
                          strategy: str,
                          save_dir: str,
                          exp_no: int,
                          total_exp: int) -> bool:
    """执行单组僵化实验"""

    total_time = WARMUP_SECONDS + PHASE1_SECONDS + PHASE2_SECONDS
    master     = cluster.get_master()

    print()
    print("╔" + "═" * 63 + "╗")
    print(f"║  僵化实验 S{exp_no}/{total_exp}: "
          f"策略 = {strategy.upper():<10s} 时长 = {total_time}s"
          f"{'':>12s}║")
    if QUICK_MODE:
        print(f"║  ⚡ 快速验证模式"
              f"{'':>47s}║")
    print("╚" + "═" * 63 + "╝")

    producer = StagnationProducer(LOCAL_JAR_PATH, PRODUCER_WORK_DIR, save_dir)

    # 1. 清理残留
    producer.stop()

    # 2. ★ 重启 Flink 集群（替代原来的 cancel + truncate）
    cluster.restart_flink_cluster(config, flink_client)

    # 3. 重置 RocketMQ 消费进度
    reset_rocketmq(master, config)
    time.sleep(2)

    # 4. 提交 Flink Job
    job_id = submit_flink_job_stagnation(master, flink_client, config, strategy)
    if not job_id:
        print("  [ERROR] Job 未能启动，跳过本组")
        return False

    # 5. 启动 StagnationProducer
    if not producer.start(strategy_name=strategy):
        flink_client.cancel_all_running_jobs()
        return False

    # 6. 预热
    print(f"\n[PHASE 0/3] 预热（{WARMUP_SECONDS}s）")
    print_progress("预热", WARMUP_SECONDS)

    # 7. Phase1
    print(f"\n[PHASE 1/3] 正常频率（{PHASE1_SECONDS}s）")
    print_progress("Phase1", PHASE1_SECONDS)

    # 8. Phase2
    print(f"\n[PHASE 2/3] 频率漂移（{PHASE2_SECONDS}s）")
    print_progress("Phase2", PHASE2_SECONDS)

    # 9. 停止 Producer
    print(f"\n[PHASE 3/3] 停止并收集数据")
    producer.stop()

    # 10. 取消 Flink Job（触发 close 日志）
    print("[STEP] 取消 Flink Job...")
    flink_client.cancel_all_running_jobs()

    # 11. 等待日志刷盘
    print(f"  等待 {COOLDOWN_SECONDS}s 确保日志刷盘...")
    time.sleep(COOLDOWN_SECONDS)

    # 12. 收集日志（使用更新后的 collect_logs，含轮转文件）
    collected = cluster.collect_logs(
        config["flink_log_dir"],
        f"stag_{strategy}",
        save_dir
    )
    if collected == 0:
        print("  [WARN] 未收集到任何日志文件")

    print(f"\n  ✓ 僵化实验 S{exp_no}（{strategy.upper()}）完成")
    return True


# ==================== 日志解析（时间戳版）====================

STAG_FIELD_PATTERN = re.compile(
    r'(T1a|T1b|T2|T3|T4a|T4b)(?:\([^)]*\))?=([\d.]+)%\((\d+)/(\d+)\)'
)

HIT_PATTERN     = re.compile(r'(?<!\w)hit=(\d+)')
MISS_PATTERN    = re.compile(r'(?<!\w)miss=(\d+)')
SUBTASK_PATTERN = re.compile(r'\[S(\d+)\]')
FN_PATTERN      = re.compile(r'^stag_([\w-]+)_node\d+_', re.IGNORECASE)
LOG_TS_PATTERN  = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d{3})')


def _parse_log_timestamp(line: str) -> Optional[datetime]:
    """从日志行提取时间戳"""
    m = LOG_TS_PATTERN.search(line)
    if not m:
        return None
    dt = datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S')
    return dt.replace(microsecond=int(m.group(2)) * 1000)


def _parse_stag_line(line: str) -> Optional[dict]:
    """用 findall 逐字段提取 6 组 (hit, total)"""
    matches = STAG_FIELD_PATTERN.findall(line)
    if not matches:
        return None

    result = {
        'T1a': (0, 0), 'T1b': (0, 0),
        'T2':  (0, 0), 'T3':  (0, 0),
        'T4a': (0, 0), 'T4b': (0, 0),
    }
    found = 0
    for name, rate_str, hit_str, total_str in matches:
        if name in result:
            result[name] = (int(hit_str), int(total_str))
            found += 1
    return result if found >= 3 else None


def parse_stagnation_logs(results_dir: str) -> Optional[dict]:
    """解析僵化实验日志（时间戳版）"""
    all_files = glob.glob(os.path.join(results_dir, 'stag_*.log'))
    if not all_files:
        print("[WARN] 未找到僵化实验日志（stag_*.log）")
        return None

    print(f"\n找到 {len(all_files)} 个日志文件")

    raw: dict = defaultdict(lambda: defaultdict(list))

    for filepath in sorted(all_files):
        basename = os.path.basename(filepath)

        if 'producer' in basename.lower():
            print(f"  跳过: {basename}（Producer 日志）")
            continue

        fn_match = FN_PATTERN.match(basename)
        if not fn_match:
            print(f"  跳过: {basename}（文件名不匹配）")
            continue

        strategy = fn_match.group(1).upper()
        periodic_count = 0
        close_count    = 0
        skipped_lines  = 0

        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if 'T1a' not in line:
                    continue

                ts = _parse_log_timestamp(line)
                stag_data = _parse_stag_line(line)
                if not stag_data or not ts:
                    skipped_lines += 1
                    continue

                st_match   = SUBTASK_PATTERN.search(line)
                subtask_id = int(st_match.group(1)) if st_match else 0

                hit_m  = HIT_PATTERN.search(line)
                miss_m = MISS_PATTERN.search(line)

                sample = dict(stag_data)
                sample['timestamp']    = ts
                sample['overall_hit']  = int(hit_m.group(1))  if hit_m  else 0
                sample['overall_miss'] = int(miss_m.group(1)) if miss_m else 0

                is_close = '关闭' in line
                raw[strategy][subtask_id].append(sample)

                if is_close:
                    close_count += 1
                else:
                    periodic_count += 1

        n_subtasks = len(raw.get(strategy, {}))
        total_samples = sum(len(v) for v in raw.get(strategy, {}).values())
        print(f"  {basename}: 策略={strategy}, subtask={n_subtasks}, "
              f"周期行={periodic_count}, 关闭行={close_count}, "
              f"总采样={total_samples}"
              + (f", 跳过={skipped_lines}" if skipped_lines > 0 else ""))

    if not raw:
        print("[WARN] 所有日志均未匹配到僵化模式统计行")
        return None

    results = {}

    for strategy, subtask_data in raw.items():

        for st_id in subtask_data:
            subtask_data[st_id].sort(key=lambda s: s['timestamp'])

        all_first_ts = []
        all_last_ts  = []
        for samples in subtask_data.values():
            if samples:
                all_first_ts.append(samples[0]['timestamp'])
                all_last_ts.append(samples[-1]['timestamp'])

        if not all_first_ts:
            continue

        first_ts        = min(all_first_ts)
        last_ts         = max(all_last_ts)
        phase_boundary  = first_ts + timedelta(seconds=PHASE_SWITCH_OFFSET_SEC)
        total_duration  = (last_ts - first_ts).total_seconds()
        boundary_offset = (phase_boundary - first_ts).total_seconds()

        print(f"\n  ── {strategy} 时间分析 ──")
        print(f"    首条日志: {first_ts.strftime('%H:%M:%S.%f')[:-3]}")
        print(f"    末条日志: {last_ts.strftime('%H:%M:%S.%f')[:-3]}")
        print(f"    总时长:   {total_duration:.0f}s")
        print(f"    漂移分界: {phase_boundary.strftime('%H:%M:%S.%f')[:-3]} "
              f"(+{boundary_offset:.0f}s)")

        all_groups = STAG_GROUPS + ['overall']
        group_phase_rates = {
            grp: {'phase1_rates': [], 'phase2_rates': []}
            for grp in all_groups
        }
        phase1_point_counts = []

        for st_id, samples in subtask_data.items():
            if len(samples) < 2:
                print(f"    [WARN] subtask {st_id}: "
                      f"仅 {len(samples)} 个采样点，跳过")
                continue

            st_p1_count = 0

            for i in range(1, len(samples)):
                prev = samples[i - 1]
                curr = samples[i]

                is_phase2 = curr['timestamp'] >= phase_boundary

                if not is_phase2:
                    st_p1_count += 1

                for grp in STAG_GROUPS:
                    d_hit   = curr[grp][0] - prev[grp][0]
                    d_total = curr[grp][1] - prev[grp][1]
                    if d_total > 0:
                        rate = round(d_hit / d_total * 100, 2)
                    else:
                        rate = 0.0

                    if is_phase2:
                        group_phase_rates[grp]['phase2_rates'].append(rate)
                    else:
                        group_phase_rates[grp]['phase1_rates'].append(rate)

                prev_total_all = prev['overall_hit'] + prev['overall_miss']
                curr_total_all = curr['overall_hit'] + curr['overall_miss']
                d_hit_all   = curr['overall_hit'] - prev['overall_hit']
                d_total_all = curr_total_all - prev_total_all
                if d_total_all > 0:
                    rate_all = round(d_hit_all / d_total_all * 100, 2)
                else:
                    rate_all = 0.0

                if is_phase2:
                    group_phase_rates['overall']['phase2_rates'].append(
                        rate_all)
                else:
                    group_phase_rates['overall']['phase1_rates'].append(
                        rate_all)

            phase1_point_counts.append(st_p1_count)

        avg_p1_points = (sum(phase1_point_counts) / len(phase1_point_counts)
                         if phase1_point_counts else 0)

        result = {}
        for grp in all_groups:
            p1 = group_phase_rates[grp]['phase1_rates']
            p2 = group_phase_rates[grp]['phase2_rates']

            p1_avg = sum(p1) / len(p1) if p1 else 0.0
            p2_avg = sum(p2) / len(p2) if p2 else 0.0
            drop   = p1_avg - p2_avg

            recovery = "未恢复"
            if len(p2) >= 6:
                third = max(len(p2) // 3, 1)
                early = sum(p2[:third]) / third
                late  = sum(p2[-third:]) / third
                if late > early + 3.0:
                    recovery = "恢复"

            full_series = p1 + p2

            result[grp] = {
                'phase1':        round(p1_avg, 1),
                'phase2':        round(p2_avg, 1),
                'drop':          round(drop, 1),
                'recovery':      recovery,
                'series':        full_series,
                'phase1_series': p1,
                'phase2_series': p2,
                'phase1_count':  len(p1),
                'phase2_count':  len(p2),
            }

        result['_phase1_points'] = round(avg_p1_points)
        results[strategy] = result

        n_p1 = result['overall']['phase1_count']
        n_p2 = result['overall']['phase2_count']
        print(f"\n  ── {strategy} 子分层增量分析 "
              f"(Phase1:{n_p1}点, Phase2:{n_p2}点, "
              f"漂移线≈第{result['_phase1_points']}点) ──")
        print(f"    总体:        "
              f"Phase1={result['overall']['phase1']:>5.1f}% → "
              f"Phase2={result['overall']['phase2']:>5.1f}%  "
              f"变化={result['overall']['drop']:>+5.1f}%")
        print(f"    T1a(僵尸):   "
              f"Phase1={result['T1a']['phase1']:>5.1f}% → "
              f"Phase2={result['T1a']['phase2']:>5.1f}%  "
              f"下降={result['T1a']['drop']:>5.1f}%  "
              f"← {'越大越好' if result['T1a']['drop'] > 0 else '僵化!'}")
        print(f"    T1b(活跃):   "
              f"Phase1={result['T1b']['phase1']:>5.1f}% → "
              f"Phase2={result['T1b']['phase2']:>5.1f}%  "
              f"变化={result['T1b']['drop']:>+5.1f}%  "
              f"← {'✓稳定' if abs(result['T1b']['drop']) < 5 else '✗异常'}")
        print(f"    T4a(新热点): "
              f"Phase1={result['T4a']['phase1']:>5.1f}% → "
              f"Phase2={result['T4a']['phase2']:>5.1f}%  "
              f"上升={-result['T4a']['drop']:>+5.1f}%  "
              f"← {'越大越好' if result['T4a']['drop'] < 0 else '未晋升'}")
        print(f"    T4b(冷节点): "
              f"Phase1={result['T4b']['phase1']:>5.1f}% → "
              f"Phase2={result['T4b']['phase2']:>5.1f}%")

    return results if results else None


# ==================== 可视化 ====================

def plot_stagnation(data: dict, save_dir: str):
    """生成僵化实验可视化图表"""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np

    plt.rcParams['font.sans-serif'] = [
        'SimHei', 'Microsoft YaHei', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['figure.dpi'] = 150

    COLORS = {
        "LFU":       "#e67e22",
        "W-TINYLFU": "#e74c3c",
        "LRU-K":     "#3498db",
    }
    strategies = sorted(data.keys(),
                        key=lambda s: (s == "W-TINYLFU", s))

    def _get_series(strategy, group):
        return data[strategy][group]['series']

    def _get_drift_x(strategy):
        return data[strategy].get('_phase1_points', 0)

    def _plot_series(ax, group_key, y_label, title_text,
                     drift_label, marker='o'):
        max_len = 0
        drift_x = 0

        for strategy in strategies:
            series = _get_series(strategy, group_key)
            if not series:
                continue
            drift_x = max(drift_x, _get_drift_x(strategy))
            max_len = max(max_len, len(series))

            x  = list(range(1, len(series) + 1))
            lw = 3.0 if strategy == "W-TINYLFU" else 2.0
            ax.plot(x, series,
                    label=strategy,
                    color=COLORS.get(strategy, '#333'),
                    linewidth=lw, marker=marker,
                    markersize=3, alpha=0.85)

        if drift_x > 0 and max_len > 0:
            ax.axvline(x=drift_x, color='red', linestyle='--',
                       linewidth=2, alpha=0.7)
            ymin, ymax = ax.get_ylim()
            span = ymax - ymin if ymax != ymin else 1

            ax.annotate(drift_label,
                        xy=(drift_x, ymin + span * 0.8),
                        xytext=(drift_x + max(1, max_len * 0.03),
                                ymin + span * 0.85),
                        fontsize=9, color='red', fontweight='bold',
                        arrowprops=dict(arrowstyle='->', color='red'),
                        bbox=dict(boxstyle='round,pad=0.3',
                                  facecolor='yellow', alpha=0.3))
            ax.text(drift_x / 2, ymin + span * 0.03,
                    f'Phase1 ({PHASE1_SECONDS}s)',
                    ha='center', fontsize=10,
                    color='#2c3e50', fontstyle='italic')
            ax.text(drift_x + (max_len - drift_x) / 2,
                    ymin + span * 0.03,
                    f'Phase2 ({PHASE2_SECONDS}s, 漂移后)',
                    ha='center', fontsize=10,
                    color='red', fontstyle='italic')

        ax.set_xlabel('采样序号', fontsize=12)
        ax.set_ylabel(y_label, fontsize=12)
        ax.set_title(title_text, fontsize=13, fontweight='bold')
        ax.legend(loc='best', fontsize=11, framealpha=0.9)
        ax.grid(alpha=0.3)
        ax.set_axisbelow(True)

    # ===== 图5: T1a 僵尸节点层趋势 =====
    fig, ax = plt.subplots(figsize=(14, 6))
    _plot_series(ax, 'T1a',
                 'T1a 窗口命中率 (%)',
                 'T1a 僵尸节点层命中率变化\n'
                 '(LFU仍高=僵化严重  '
                 'W-TinyLFU快速下降=成功淘汰僵尸)',
                 '← 漂移点\nT1a: 1s→5s')
    plt.tight_layout()
    path = os.path.join(save_dir, 'fig5_T1a_zombie.png')
    fig.savefig(path)
    plt.close(fig)
    print(f"  图5 已保存: {path}")

    # ===== 图6: T4a 新热点层趋势 =====
    fig, ax = plt.subplots(figsize=(14, 6))
    _plot_series(ax, 'T4a',
                 'T4a 窗口命中率 (%)',
                 'T4a 新热点层命中率变化\n'
                 '(W-TinyLFU快速上升=新热点成功进入缓存)',
                 '← 漂移点\nT4a: 5s→1s',
                 marker='s')
    plt.tight_layout()
    path = os.path.join(save_dir, 'fig6_T4a_hotspot.png')
    fig.savefig(path)
    plt.close(fig)
    print(f"  图6 已保存: {path}")

    # ===== 图7: Phase1 vs Phase2 柱状对比 =====
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    x     = np.arange(len(strategies))
    width = 0.35

    for col, (grp, title, p2_color) in enumerate([
        ('T1a',
         'T1a 僵尸节点层\n(Phase2仍高=僵化 / 下降=淘汰)',
         '#e74c3c'),
        ('T4a',
         'T4a 新热点层\n(Phase2上升=成功进入缓存)',
         '#2ecc71'),
    ]):
        ax  = axes[col]
        p1v = [data[s][grp]['phase1'] for s in strategies]
        p2v = [data[s][grp]['phase2'] for s in strategies]

        b1 = ax.bar(x - width / 2, p1v, width,
                     label='Phase1(漂移前)',
                     color='#3498db', alpha=0.85)
        b2 = ax.bar(x + width / 2, p2v, width,
                     label='Phase2(漂移后)',
                     color=p2_color, alpha=0.85)

        for bar, val in zip(b1, p1v):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    max(bar.get_height(), 0) + 0.5,
                    f'{val:.1f}%', ha='center', va='bottom',
                    fontsize=10, fontweight='bold')
        for bar, val in zip(b2, p2v):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    max(bar.get_height(), 0) + 0.5,
                    f'{val:.1f}%', ha='center', va='bottom',
                    fontsize=10, fontweight='bold')

        ax.set_xticks(x)
        ax.set_xticklabels(strategies, fontsize=10)
        ax.set_ylabel('命中率 (%)', fontsize=11)
        ax.set_title(title, fontsize=12, fontweight='bold')
        ax.legend(fontsize=10)
        ax.grid(axis='y', alpha=0.3)
        ax.set_axisbelow(True)

    fig.suptitle('缓存僵化实验 — 漂移前后分层命中率对比',
                 fontsize=15, fontweight='bold', y=1.02)
    plt.tight_layout()
    path = os.path.join(save_dir, 'fig7_tier_bar_comparison.png')
    fig.savefig(path, bbox_inches='tight')
    plt.close(fig)
    print(f"  图7 已保存: {path}")

    # ===== 图8: T1a下降 + T4a上升 =====
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    colors_list = [COLORS.get(s, '#333') for s in strategies]

    ax        = axes[0]
    t1a_drops = [data[s]['T1a']['drop'] for s in strategies]
    bars      = ax.bar(strategies, t1a_drops, color=colors_list,
                       width=0.5, edgecolor='white', linewidth=1.5)
    max_d = max(t1a_drops) if t1a_drops else 0
    min_d = min(t1a_drops) if t1a_drops else 0
    for bar, val in zip(bars, t1a_drops):
        suffix = ''
        if val == max_d and max_d > 0:
            suffix = '\n(淘汰最快 ✓)'
        elif val == min_d and len(set(t1a_drops)) > 1:
            suffix = '\n(僵化严重 ✗)'
        ax.text(bar.get_x() + bar.get_width() / 2,
                max(bar.get_height(), 0) + 0.5,
                f'{val:.1f}%{suffix}',
                ha='center', va='bottom',
                fontsize=10, fontweight='bold')
    ax.set_ylabel('T1a 命中率下降 (%)', fontsize=12)
    ax.set_title('T1a 僵尸节点命中率下降幅度\n'
                 '(越大 = 淘汰越快 = 抗僵化越强)',
                 fontsize=12, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    ax.axhline(y=0, color='black', linewidth=0.5)

    ax        = axes[1]
    t4a_rises = [-data[s]['T4a']['drop'] for s in strategies]
    bars      = ax.bar(strategies, t4a_rises, color=colors_list,
                       width=0.5, edgecolor='white', linewidth=1.5)
    max_r = max(t4a_rises) if t4a_rises else 0
    for bar, val in zip(bars, t4a_rises):
        suffix = ('\n(适应最快 ✓)'
                  if val == max_r and max_r > 0 else '')
        label  = (f'+{val:.1f}%{suffix}'
                  if val >= 0 else f'{val:.1f}%{suffix}')
        ax.text(bar.get_x() + bar.get_width() / 2,
                max(bar.get_height(), 0) + 0.5,
                label, ha='center', va='bottom',
                fontsize=10, fontweight='bold')
    ax.set_ylabel('T4a 命中率上升 (%)', fontsize=12)
    ax.set_title('T4a 新热点命中率上升幅度\n'
                 '(越大 = 新热点进入缓存越快)',
                 fontsize=12, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    ax.axhline(y=0, color='black', linewidth=0.5)

    plt.tight_layout()
    path = os.path.join(save_dir, 'fig8_adaptability.png')
    fig.savefig(path)
    plt.close(fig)
    print(f"  图8 已保存: {path}")

    # ===== 图9: T1b 对照组 =====
    fig, ax = plt.subplots(figsize=(14, 6))
    _plot_series(ax, 'T1b',
                 'T1b 窗口命中率 (%)',
                 'T1b 活跃节点对照组（频率未变，命中率应保持稳定）\n'
                 '用于验证实验有效性：'
                 '若 T1b 也大幅下降则说明策略存在整体问题',
                 '← 漂移点\nT1b 保持高频',
                 marker='^')
    plt.tight_layout()
    path = os.path.join(save_dir, 'fig9_T1b_control_group.png')
    fig.savefig(path)
    plt.close(fig)
    print(f"  图9 已保存: {path}")


# ==================== 主函数 ====================

def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)

    print(f"加载配置: {DEFAULT_CONFIG_PATH}")
    config = load_config(DEFAULT_CONFIG_PATH)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    save_dir  = os.path.join(RESULTS_DIR, f"stagnation_{timestamp}")
    os.makedirs(save_dir, exist_ok=True)

    # 每组多 ~30s 重启开销
    total_time_per = (WARMUP_SECONDS + PHASE1_SECONDS + PHASE2_SECONDS
                      + COOLDOWN_SECONDS + 60)
    total_time_all = len(STAGNATION_STRATEGIES) * total_time_per

    st_str = ', '.join(s.upper() for s in STAGNATION_STRATEGIES)
    mode_label = "⚡ 快速验证" if QUICK_MODE else "📊 正式实验"
    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print(f"║       缓存僵化（Stagnation）对比实验 v4  "
          f"{mode_label:<12s}          ║")
    print("╠══════════════════════════════════════════════════════════════════╣")
    print(f"║  策略:     {st_str:<52s}   ║")
    print(f"║  缓存容量: 120"
          f"{'':>51s}║")
    print(f"║  流程:     重启集群→预热{WARMUP_SECONDS}s→"
          f"Phase1({PHASE1_SECONDS}s)→Phase2({PHASE2_SECONDS}s)→冷却"
          f"{'':>13s}║")
    print(f"║  Producer: switch-time={PRODUCER_SWITCH_TIME}s"
          f"{'':>41s}║")
    print(f"║  预计:     ~{total_time_all // 60}min ({total_time_all}s)"
          f"{'':>42s}║")
    if QUICK_MODE:
        print(f"║  ⚠ 快速模式数据点少，仅用于验证链路，"
              f"正式实验请设QUICK_MODE=False  ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print()

    if not os.path.exists(LOCAL_JAR_PATH):
        print(f"  ✗ 宿主机 JAR 不存在: {LOCAL_JAR_PATH}")
        sys.exit(1)
    jar_mb = os.path.getsize(LOCAL_JAR_PATH) / 1024 / 1024
    print(f"  ✓ JAR: {os.path.basename(LOCAL_JAR_PATH)} ({jar_mb:.1f} MB)")

    exp_config = {
        "experiment":           "stagnation_v4",
        "quick_mode":           QUICK_MODE,
        "timestamp":            timestamp,
        "strategies":           STAGNATION_STRATEGIES,
        "cache_capacity":       120,
        "warmup_seconds":       WARMUP_SECONDS,
        "phase1_seconds":       PHASE1_SECONDS,
        "phase2_seconds":       PHASE2_SECONDS,
        "producer_switch_time": PRODUCER_SWITCH_TIME,
        "producer_duration":    PRODUCER_DURATION,
        "phase_switch_offset":  PHASE_SWITCH_OFFSET_SEC,
        "stagnation_mode":      True,
        "sub_groups":           STAG_GROUPS,
        "cluster_restart":      True,
    }
    with open(os.path.join(save_dir, "stagnation_config.json"),
              "w", encoding="utf-8") as f:
        json.dump(exp_config, f, indent=2, ensure_ascii=False)

    input("\n按 Enter 开始僵化实验（Ctrl+C 取消）...")

    flink_client = FlinkRESTClient(config["flink_rest_url"])
    cluster      = ClusterSSH(config)

    try:
        cluster.connect_all()
        master = cluster.get_master()
        if not master:
            print("[ERROR] 无法连接主节点 node01")
            sys.exit(1)

        jar_check = master.exec_cmd(
            f"test -f {config['remote_jar_path']} "
            f"&& echo OK || echo MISSING",
            timeout=5, ignore_error=True
        )
        if "MISSING" in jar_check:
            print(f"  ✗ 虚拟机 JAR 不存在: {config['remote_jar_path']}")
            sys.exit(1)
        print(f"  ✓ 虚拟机 JAR 已确认")

        exp_results = OrderedDict()
        for i, strategy in enumerate(STAGNATION_STRATEGIES):
            success = run_single_stagnation(
                cluster, flink_client, config,
                strategy, save_dir,
                exp_no=i + 1,
                total_exp=len(STAGNATION_STRATEGIES)
            )
            exp_results[strategy] = success

        print()
        print("═" * 65)
        print("  解析僵化实验日志...")
        print("═" * 65)

        for s, ok in exp_results.items():
            print(f"    {s.upper():<12s} "
                  f"{'✓ 成功' if ok else '✗ 失败'}")

        data = parse_stagnation_logs(save_dir)
        if data:
            print("\n  生成可视化图表...")
            plot_stagnation(data, save_dir)

            print()
            print("┌────────────┬──────────────────────────"
                  "──────┬────────────────────────────────┐")
            print("│   策略     │  T1a 僵尸节点            "
                  "      │  T4a 新热点                    │")
            print("│            │  P1→P2        下降   自适应"
                  "    │  P1→P2        上升   自适应    │")
            print("├────────────┼──────────────────────────"
                  "──────┼────────────────────────────────┤")
            for s in sorted(data.keys()):
                if s.startswith('_'):
                    continue
                d      = data[s]
                marker = " ◀" if s == "W-TINYLFU" else ""
                t1a    = d['T1a']
                t4a    = d['T4a']
                print(
                    f"│ {s:<10s} │ "
                    f"{t1a['phase1']:>5.1f}%→{t1a['phase2']:<5.1f}% "
                    f"{t1a['drop']:>6.1f}%  {t1a['recovery']:<7s} │ "
                    f"{t4a['phase1']:>5.1f}%→{t4a['phase2']:<5.1f}% "
                    f"{-t4a['drop']:>+6.1f}%  "
                    f"{t4a['recovery']:<7s} │{marker}"
                )
            print("└────────────┴──────────────────────────"
                  "──────┴────────────────────────────────┘")
            print()

            print("  对照组验证 T1b（仍高频，应保持稳定）:")
            for s in sorted(data.keys()):
                if s.startswith('_'):
                    continue
                t1b = data[s]['T1b']
                stable = ("✓ 稳定"
                          if abs(t1b['drop']) < 5 else "✗ 异常")
                print(f"    {s:<10s}: "
                      f"{t1b['phase1']:.1f}% → {t1b['phase2']:.1f}%  "
                      f"变化={t1b['drop']:+.1f}%  {stable}")

            print()
            print("  解读:")
            print("    T1a 下降越大 = 僵尸节点被淘汰越快 "
                  "= 抗僵化能力越强")
            print("    T4a 上升越大 = 新热点进入缓存越快 "
                  "= 自适应能力越强")
            print("    T1b 应保持稳定（变化 < 5%）"
                  "= 实验有效性验证")

            if QUICK_MODE:
                print()
                print("  ⚠ 当前为快速验证模式，数据点较少，"
                      "仅用于确认链路正确")
                print("  ⚠ 正式实验请修改 QUICK_MODE = False 后重跑")

            json_data = {}
            for s in sorted(data.keys()):
                if s.startswith('_'):
                    continue
                json_data[s] = {}
                for grp in STAG_GROUPS + ['overall']:
                    d = data[s][grp]
                    json_data[s][grp] = {
                        'phase1':       d['phase1'],
                        'phase2':       d['phase2'],
                        'drop':         d['drop'],
                        'recovery':     d['recovery'],
                        'phase1_count': d['phase1_count'],
                        'phase2_count': d['phase2_count'],
                    }
            with open(os.path.join(save_dir, "stagnation_results.json"),
                      "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            print(f"\n  数值结果已保存: stagnation_results.json")

        else:
            print("[WARN] 未解析到有效数据")

        print(f"\n结果保存在: {save_dir}/")

    except KeyboardInterrupt:
        print("\n[中断] 用户取消")
        try:
            flink_client.cancel_all_running_jobs()
        except Exception:
            pass
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        try:
            flink_client.cancel_all_running_jobs()
        except Exception:
            pass
    finally:
        cluster.close_all()


if __name__ == "__main__":
    main()
