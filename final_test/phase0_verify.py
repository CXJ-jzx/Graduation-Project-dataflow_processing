"""
phase0_collector.py
Phase 0 - 持续指标采集脚本

功能：
  每隔 INTERVAL 秒从 Flink REST API 采集全部关键指标，写入 CSV 文件
  用于后续所有 Phase 的实验数据记录

使用方式：
  python phase0_collector.py --experiment P1-baseline --duration 600

参数：
  --experiment : 实验编号（写入 CSV 的 experiment_id 列）
  --duration   : 采集持续时间（秒），默认 600（10分钟），0 表示无限运行
  --interval   : 采集间隔（秒），默认 10
  --output     : 输出 CSV 文件路径，默认 metrics_{experiment}_{timestamp}.csv
"""

import requests
import time
import csv
import sys
import os
import argparse
from datetime import datetime

FLINK_REST = "http://192.168.56.151:8081"
TIMEOUT = 10

# ========== 需要采集的指标 ==========

# 各算子需要采集的内置指标
BUILTIN_METRICS = [
    "numRecordsInPerSecond",
    "numRecordsOutPerSecond",
    "busyTimeMsPerSecond",
]

# 自定义指标及其所属算子关键字
CUSTOM_METRICS = {
    "Feature-Extraction": ["l2_hit_rate", "l2_occupancy"],
    "Grid-Aggregation": ["l3_hit_rate", "l3_occupancy"],
    "EventFeature-Sink": ["latency_p99"],
}

# 关注的算子关键字（用于 CSV 列名生成）
OPERATOR_KEYWORDS = [
    "Source",
    "Hardware-Filter",
    "Signal-Extraction",
    "Feature-Extraction",
    "Grid-Aggregation",
    "EventFeature-Sink",
    "GridSummary-Sink",
]


def get_json(url, params=None):
    try:
        resp = requests.get(url, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def find_running_job():
    """查找运行中的 Flink 作业"""
    jobs_data = get_json(f"{FLINK_REST}/jobs")
    if jobs_data is None:
        return None, None

    for j in jobs_data.get("jobs", []):
        if j["status"] == "RUNNING":
            job_id = j["id"]
            detail = get_json(f"{FLINK_REST}/jobs/{job_id}")
            return job_id, detail
    return None, None


def match_operator(vertex_name, keyword):
    """判断算子名称是否匹配关键字"""
    return keyword.lower() in vertex_name.lower()


def get_subtask_metric(job_id, vertex_id, metric_name):
    """获取某个算子某个指标值"""
    data = get_json(
        f"{FLINK_REST}/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics",
        params={"get": metric_name}
    )
    if data and len(data) > 0:
        raw = data[0].get("value", None)
        if raw is not None:
            try:
                return float(raw)
            except (ValueError, TypeError):
                return None
    return None


def build_csv_header():
    """构建 CSV 表头"""
    header = ["timestamp", "experiment_id", "elapsed_sec"]

    for op in OPERATOR_KEYWORDS:
        short = op.replace("-", "_").lower()
        header.append(f"{short}_in_per_sec")
        header.append(f"{short}_out_per_sec")
        header.append(f"{short}_busy_ms")

    # 自定义指标
    header.extend([
        "l2_hit_rate",
        "l2_occupancy",
        "l3_hit_rate",
        "l3_occupancy",
        "latency_p99",
    ])

    return header


def collect_one_round(job_id, detail, experiment_id, start_time):
    """采集一轮指标，返回一行 CSV 数据"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    elapsed = int(time.time() - start_time)
    row = {"timestamp": now, "experiment_id": experiment_id, "elapsed_sec": elapsed}

    vertices = detail.get("vertices", [])

    # 采集各算子内置指标
    for op_keyword in OPERATOR_KEYWORDS:
        short = op_keyword.replace("-", "_").lower()

        # 找到匹配的 vertex
        target_vid = None
        for v in vertices:
            if match_operator(v["name"], op_keyword):
                target_vid = v["id"]
                break

        if target_vid:
            in_rate = get_subtask_metric(job_id, target_vid, "numRecordsInPerSecond")
            out_rate = get_subtask_metric(job_id, target_vid, "numRecordsOutPerSecond")
            busy = get_subtask_metric(job_id, target_vid, "busyTimeMsPerSecond")
        else:
            in_rate = out_rate = busy = None

        row[f"{short}_in_per_sec"] = in_rate
        row[f"{short}_out_per_sec"] = out_rate
        row[f"{short}_busy_ms"] = busy

    # 采集自定义指标
    for op_keyword, metrics in CUSTOM_METRICS.items():
        target_vid = None
        for v in vertices:
            if match_operator(v["name"], op_keyword):
                target_vid = v["id"]
                break

        for metric in metrics:
            value = None
            if target_vid:
                value = get_subtask_metric(job_id, target_vid, metric)
            row[metric] = value

    return row


def print_live_status(row, round_num):
    """在终端打印当前轮次的关键信息"""
    # Source 吞吐
    src_in = row.get("source_in_per_sec")
    src_in_str = f"{src_in:.1f}" if src_in is not None else "N/A"

    # Sink 吞吐
    sink_out = row.get("eventfeature_sink_out_per_sec")
    sink_out_str = f"{sink_out:.1f}" if sink_out is not None else "N/A"

    # 延迟
    lat = row.get("latency_p99")
    lat_str = f"{lat:.0f}ms" if lat is not None else "N/A"

    # L2 命中率
    l2_hr = row.get("l2_hit_rate")
    l2_str = f"{l2_hr:.2%}" if l2_hr is not None else "N/A"

    # L3 命中率
    l3_hr = row.get("l3_hit_rate")
    l3_str = f"{l3_hr:.2%}" if l3_hr is not None else "N/A"

    elapsed = row.get("elapsed_sec", 0)

    print(f"  [{round_num:>4}] {elapsed:>5}s | "
          f"Src={src_in_str:>8}/s | "
          f"Sink={sink_out_str:>8}/s | "
          f"Lat={lat_str:>10} | "
          f"L2={l2_str:>8} | "
          f"L3={l3_str:>8}")


def main():
    parser = argparse.ArgumentParser(description="Phase 0 - 持续指标采集")
    parser.add_argument("--experiment", type=str, default="phase0-test",
                        help="实验编号 (默认: phase0-test)")
    parser.add_argument("--duration", type=int, default=600,
                        help="采集持续时间/秒 (默认: 600, 0=无限)")
    parser.add_argument("--interval", type=int, default=10,
                        help="采集间隔/秒 (默认: 10)")
    parser.add_argument("--output", type=str, default=None,
                        help="输出文件路径 (默认: 自动生成)")
    args = parser.parse_args()

    # 生成输出文件名
    if args.output is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = "experiment_data"
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"metrics_{args.experiment}_{ts}.csv")
    else:
        output_file = args.output
        os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else ".", exist_ok=True)

    print()
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║           Phase 0 — 持续指标采集                                    ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    print()
    print(f"  实验编号  : {args.experiment}")
    print(f"  采集间隔  : {args.interval}s")
    print(f"  持续时间  : {'无限' if args.duration == 0 else f'{args.duration}s ({args.duration / 60:.1f}min)'}")
    print(f"  输出文件  : {output_file}")
    print()

    # 查找作业
    print("  正在查找运行中的 Flink 作业...")
    job_id, detail = find_running_job()
    if job_id is None:
        print("  ❌ 没有运行中的 Flink 作业，退出")
        sys.exit(1)

    job_name = detail.get("name", "unknown")
    print(f"  ✅ 找到作业: {job_name} ({job_id})")
    print()

    # 构建 CSV
    header = build_csv_header()
    csv_file = open(output_file, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=header, extrasaction="ignore")
    writer.writeheader()

    start_time = time.time()
    round_num = 0

    print(f"  {'轮次':>6} {'已运行':>6} | "
          f"{'Source入':>12} | {'Sink出':>12} | "
          f"{'延迟':>12} | {'L2命中':>8} | {'L3命中':>8}")
    print(f"  {'─' * 6} {'─' * 6}   {'─' * 12}   {'─' * 12}   {'─' * 12}   {'─' * 8}   {'─' * 8}")

    try:
        while True:
            # 检查是否超时
            elapsed = time.time() - start_time
            if args.duration > 0 and elapsed >= args.duration:
                print(f"\n  ⏱ 已达到设定的采集时长 {args.duration}s，停止采集")
                break

            round_num += 1

            # 重新获取 job detail（应对 DS2 重启后 job_id 变化）
            current_job_id, current_detail = find_running_job()
            if current_job_id is None:
                print(f"\n  ⚠️ [{round_num}] 未找到运行中的作业，等待重试...")
                time.sleep(args.interval)
                continue

            if current_job_id != job_id:
                print(f"\n  🔄 [{round_num}] 检测到作业变更: {job_id[:8]}... → {current_job_id[:8]}...")
                job_id = current_job_id
                detail = current_detail

            # 采集
            row = collect_one_round(job_id, detail, args.experiment, start_time)

            # 写入 CSV
            writer.writerow(row)
            csv_file.flush()

            # 终端实时显示
            print_live_status(row, round_num)

            # 等待下一轮
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print(f"\n\n  ⏹ 手动停止采集 (Ctrl+C)")

    finally:
        csv_file.close()
        print(f"\n  📊 共采集 {round_num} 轮数据")
        print(f"  💾 数据已保存至: {output_file}")
        print()


if __name__ == "__main__":
    main()
