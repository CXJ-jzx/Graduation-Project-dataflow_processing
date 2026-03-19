"""
verify_step2_values.py
验证能拉到真实的数值（非 N/A）
"""

import requests
import json

FLINK_REST = "http://192.168.56.151:8081"

def get_metric(job_id, vertex_id, metric_name):
    """拉取指标的正确方式"""
    url = f"{FLINK_REST}/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics"
    resp = requests.get(url, params={"get": metric_name}).json()
    if resp and isinstance(resp, list) and len(resp) > 0:
        entry = resp[0]
        return {
            "min": entry.get("min"),
            "max": entry.get("max"),
            "avg": entry.get("avg"),
            "sum": entry.get("sum"),
        }
    return None


def get_metric_agg(job_id, vertex_id, metric_name):
    """尝试用 /aggregated 端点拉取（Flink 1.17 支持）"""
    url = f"{FLINK_REST}/jobs/{job_id}/vertices/{vertex_id}/metrics"
    resp = requests.get(url, params={"get": metric_name}).json()
    if resp and isinstance(resp, list) and len(resp) > 0:
        return resp[0]
    return None


def main():
    # 找到作业
    jobs = requests.get(f"{FLINK_REST}/jobs").json()
    job_id = None
    for j in jobs.get("jobs", []):
        if j["status"] == "RUNNING":
            job_id = j["id"]
            break
    if not job_id:
        print("❌ 没有运行中的作业")
        return

    detail = requests.get(f"{FLINK_REST}/jobs/{job_id}").json()
    vertices = {v["name"]: v["id"] for v in detail["vertices"]}

    print("=" * 70)
    print("Part A: 通过 subtasks/metrics 端点拉取（你的代码当前用的方式）")
    print("=" * 70)

    for op_name, vid in vertices.items():
        print(f"\n--- {op_name} ---")

        # 不带前缀的指标名
        for metric in ["numRecordsInPerSecond", "numRecordsOutPerSecond",
                        "busyTimeMsPerSecond", "idleTimeMsPerSecond"]:
            val = get_metric(job_id, vid, metric)
            print(f"  {metric}: {val}")

    print(f"\n{'=' * 70}")
    print("Part B: 通过 vertex/metrics 端点拉取（聚合模式）")
    print("=" * 70)

    for op_name, vid in vertices.items():
        print(f"\n--- {op_name} ---")

        # 列出所有可用指标（这个端点不需要 subtasks 路径）
        all_metrics = requests.get(
            f"{FLINK_REST}/jobs/{job_id}/vertices/{vid}/metrics"
        ).json()
        metric_names = [m["id"] for m in all_metrics]

        # 打印前 15 个
        for mn in sorted(metric_names)[:15]:
            val = get_metric_agg(job_id, vid, mn)
            print(f"  {mn}: {val}")

    print(f"\n{'=' * 70}")
    print("Part C: 自定义指标（带算子名前缀）")
    print("=" * 70)

    # Feature-Extraction 的 L2 缓存指标
    if "Feature-Extraction" in vertices:
        vid = vertices["Feature-Extraction"]
        print(f"\n--- Feature-Extraction (L2 Cache) ---")

        # 尝试两种名称格式
        for name in ["l2_hit_rate", "Feature-Extraction.l2_hit_rate",
                      "l2_occupancy", "Feature-Extraction.l2_occupancy"]:
            val_sub = get_metric(job_id, vid, name)
            val_agg = get_metric_agg(job_id, vid, name)
            print(f"  subtasks/{name}: {val_sub}")
            print(f"  metrics/{name}:  {val_agg}")

    # Grid-Aggregation 的 L3 缓存指标
    if "Grid-Aggregation" in vertices:
        vid = vertices["Grid-Aggregation"]
        print(f"\n--- Grid-Aggregation (L3 Cache) ---")

        for name in ["l3_hit_rate", "Grid-Aggregation.l3_hit_rate",
                      "l3_occupancy", "Grid-Aggregation.l3_occupancy"]:
            val_sub = get_metric(job_id, vid, name)
            val_agg = get_metric_agg(job_id, vid, name)
            print(f"  subtasks/{name}: {val_sub}")
            print(f"  metrics/{name}:  {val_agg}")

    print(f"\n{'=' * 70}")
    print("Part D: Sink 延迟指标")
    print("=" * 70)

    if "Sink: EventFeature-Sink" in vertices:
        vid = vertices["Sink: EventFeature-Sink"]
        print(f"\n--- EventFeature-Sink ---")

        for name in ["latency_p99", "Sink__EventFeature-Sink.latency_p99"]:
            val_sub = get_metric(job_id, vid, name)
            val_agg = get_metric_agg(job_id, vid, name)
            print(f"  subtasks/{name}: {val_sub}")
            print(f"  metrics/{name}:  {val_agg}")

    print(f"\n{'=' * 70}")
    print("Part E: 累积指标（DS2 用的）")
    print("=" * 70)

    if "Hardware-Filter" in vertices:
        vid = vertices["Hardware-Filter"]
        print(f"\n--- Hardware-Filter (累积指标) ---")

        for name in ["numRecordsIn", "numRecordsOut",
                      "busyTimeMsPerSecond",
                      "Hardware-Filter.numRecordsIn",
                      "Hardware-Filter.numRecordsOut"]:
            val_sub = get_metric(job_id, vid, name)
            val_agg = get_metric_agg(job_id, vid, name)
            print(f"  subtasks/{name}: {val_sub}")
            print(f"  metrics/{name}:  {val_agg}")

    # ━━━ 汇总 ━━━
    print(f"\n{'=' * 70}")
    print("汇总：哪个端点 + 哪个指标名格式能拿到数值？")
    print("=" * 70)
    print("""
    请把上面输出中：
    1. 返回了实际数字（非 None）的行标记出来
    2. 特别关注 Part C 的自定义指标——哪种名称格式能拿到值
    3. 把完整输出发给我
    """)


if __name__ == "__main__":
    main()
