"""
verify_step1_metrics.py
在 Windows 宿主机上运行，验证能否从 Flink 拉到指标
前提：Flink 作业正在运行中
"""

import requests
import json
import sys

FLINK_REST = "http://192.168.56.151:8081"

def main():
    # ━━━ 1. 测试连通性 ━━━
    print("=" * 60)
    print("Step 1: 测试 Flink REST API 连通性")
    print("=" * 60)
    try:
        resp = requests.get(f"{FLINK_REST}/overview", timeout=5)
        overview = resp.json()
        print(f"  Flink 版本: {overview.get('flink-version', '未知')}")
        print(f"  Task Slots: {overview.get('slots-total', 0)} 总 / "
              f"{overview.get('slots-available', 0)} 空闲")
        print(f"  运行中作业: {overview.get('jobs-running', 0)}")
    except Exception as e:
        print(f"  ❌ 连接失败: {e}")
        print(f"  检查: 虚拟机是否启动? Flink 是否运行? 防火墙?")
        sys.exit(1)

    # ━━━ 2. 查找运行中的作业 ━━━
    print(f"\n{'=' * 60}")
    print("Step 2: 查找运行中的作业")
    print("=" * 60)
    jobs = requests.get(f"{FLINK_REST}/jobs").json()
    job_id = None
    for j in jobs.get("jobs", []):
        if j["status"] == "RUNNING":
            job_id = j["id"]
            break

    if not job_id:
        print("  ❌ 没有运行中的 Flink 作业")
        print("  请先启动 SeismicProducer0 和 SeismicStreamJob")
        sys.exit(1)
    print(f"  ✅ 作业 ID: {job_id}")

    # ━━━ 3. 遍历所有算子，列出可用指标 ━━━
    print(f"\n{'=' * 60}")
    print("Step 3: 遍历算子与指标")
    print("=" * 60)
    detail = requests.get(f"{FLINK_REST}/jobs/{job_id}").json()

    # ML Tuner 和 DS2 需要的关键指标
    CRITICAL_METRICS = [
        "numRecordsInPerSecond",
        "numRecordsOutPerSecond",
        "busyTimeMsPerSecond",
        "idleTimeMsPerSecond",
        "numRecordsIn",
        "numRecordsOut",
        "currentInputWatermark",
    ]

    # 你的自定义 Gauge 指标
    CUSTOM_METRICS = [
        "l2_hit_rate",
        "l2_occupancy",
        "l3_hit_rate",
        "l3_occupancy",
    ]

    all_found = {}
    custom_found = {}

    for v in detail["vertices"]:
        vid = v["id"]
        name = v["name"]
        parallelism = v["parallelism"]
        print(f"\n  ┌── 算子: {name}")
        print(f"  │   并行度: {parallelism}")
        print(f"  │   Vertex ID: {vid}")

        # 拉取该 vertex 的所有可用指标名
        metrics_resp = requests.get(
            f"{FLINK_REST}/jobs/{job_id}/vertices/{vid}/subtasks/metrics"
        ).json()
        metric_names = [m["id"] for m in metrics_resp]

        print(f"  │   可用指标数量: {len(metric_names)}")

        # 检查关键指标
        print(f"  │")
        print(f"  │   关键指标检查:")
        for key in CRITICAL_METRICS:
            found = key in metric_names
            status = "✅" if found else "❌"
            value = ""
            if found:
                val_resp = requests.get(
                    f"{FLINK_REST}/jobs/{job_id}/vertices/{vid}/subtasks/metrics",
                    params={"get": key}
                ).json()
                if val_resp:
                    value = f" = {val_resp[0].get('value', 'N/A')}"
                all_found[f"{name}.{key}"] = True
            print(f"  │     {status} {key}{value}")

        # 检查自定义指标
        has_custom = False
        for key in CUSTOM_METRICS:
            if key in metric_names:
                has_custom = True
                break

        if has_custom:
            print(f"  │")
            print(f"  │   自定义指标:")
            for key in CUSTOM_METRICS:
                found = key in metric_names
                status = "✅" if found else "──"
                value = ""
                if found:
                    val_resp = requests.get(
                        f"{FLINK_REST}/jobs/{job_id}/vertices/{vid}/subtasks/metrics",
                        params={"get": key}
                    ).json()
                    if val_resp:
                        value = f" = {val_resp[0].get('value', 'N/A')}"
                    custom_found[f"{name}.{key}"] = True
                print(f"  │     {status} {key}{value}")

        # 打印前 20 个指标名（帮助调试）
        print(f"  │")
        print(f"  │   指标名清单 (前20):")
        for mn in sorted(metric_names)[:20]:
            print(f"  │     - {mn}")
        if len(metric_names) > 20:
            print(f"  │     ... 还有 {len(metric_names) - 20} 个")
        print(f"  └──")

    # ━━━ 4. 汇总报告 ━━━
    print(f"\n{'=' * 60}")
    print("Step 4: 汇总报告")
    print("=" * 60)

    total_critical = len(CRITICAL_METRICS) * len(detail["vertices"])
    found_critical = len(all_found)
    print(f"\n  关键指标: {found_critical}/{total_critical} 可用")

    total_custom = len(CUSTOM_METRICS)
    found_custom = len(custom_found)
    print(f"  自定义指标: {found_custom}/{total_custom} 可用")

    if found_critical >= len(detail["vertices"]) * 3:
        print(f"\n  ✅ 基本指标充足，ML Tuner 可以工作")
    else:
        print(f"\n  ⚠️ 部分关键指标缺失，需要检查:")
        print(f"     1. Flink 版本是否为 1.17.x")
        print(f"     2. 作业是否有数据在流动（Producer 是否在运行）")
        print(f"     3. flink-conf.yaml 中 metrics 相关配置")

    if found_custom == 0:
        print(f"\n  ⚠️ 自定义指标全部缺失")
        print(f"     可能原因: Flink REST API 默认不暴露自定义 Gauge")
        print(f"     但不影响 ML Tuner 核心功能（只影响缓存命中率监控）")

    # ━━━ 5. 测试 Savepoint 端点 ━━━
    print(f"\n{'=' * 60}")
    print("Step 5: 测试 Savepoint 端点 (不实际触发)")
    print("=" * 60)
    try:
        # 只测试端点是否可达，用 GET 查询而不是 POST 触发
        sp_resp = requests.get(
            f"{FLINK_REST}/jobs/{job_id}/checkpoints"
        ).json()
        latest = sp_resp.get("latest", {}).get("completed", {})
        if latest:
            print(f"  ✅ Checkpoint 机制正常")
            print(f"  最近 Checkpoint ID: {latest.get('id', 'N/A')}")
            print(f"  大小: {latest.get('state_size', 0) / 1024:.1f} KB")
            print(f"  耗时: {latest.get('duration', 0)} ms")
        else:
            print(f"  ⚠️ 尚未完成任何 Checkpoint")
            print(f"     检查 SeismicStreamJob 中 checkpoint 是否已启用")
    except Exception as e:
        print(f"  ❌ Checkpoint 端点异常: {e}")


if __name__ == "__main__":
    main()
