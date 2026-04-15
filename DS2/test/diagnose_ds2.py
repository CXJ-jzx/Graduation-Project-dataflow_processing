#!/usr/bin/env python3
# diagnose_ds2.py
"""
DS2 诊断脚本 - 在宿主机上运行，远程检查 Flink 集群状态
"""

import sys
import time
import json
import requests

FLINK_URL = "http://192.168.56.151:8081"
JOB_NAME  = "Seismic-Stream-Job"
TARGET_JAR = "flink-rocketmq-demo-1.0-SNAPSHOT"

PASS_COUNT = 0
FAIL_COUNT = 0


def check(label, condition, detail=""):
    global PASS_COUNT, FAIL_COUNT
    if condition:
        PASS_COUNT += 1
        print(f"  [PASS] {label}")
    else:
        FAIL_COUNT += 1
        print(f"  [FAIL] {label}")
    if detail:
        print(f"         {detail}")
    return condition


def main():
    print("=" * 60)
    print("DS2 Diagnostics (from host machine)")
    print(f"Target: {FLINK_URL}")
    print("=" * 60)

    # ========================================
    # 1. Flink 连接
    # ========================================
    print("\n1. Flink Connection")
    try:
        resp = requests.get(f"{FLINK_URL}/overview", timeout=5)
        data = resp.json()
        check("REST API reachable", True,
              f"Flink {data.get('flink-version', '?')}")

        slots_total = data.get('slots-total', 0)
        slots_avail = data.get('slots-available', 0)
        tms = data.get('taskmanagers', 0)
        check(f"TaskManagers: {tms}", tms > 0)
        check(f"Slots: total={slots_total}, available={slots_avail}",
              slots_total > 0,
              "WARNING: need enough slots for max_parallelism=16" if slots_total < 16 else "")

    except requests.exceptions.ConnectionError:
        check("REST API reachable", False,
              "Cannot connect! Check:\n"
              "         1. Flink is running: jps | grep StandaloneSession\n"
              "         2. Firewall: systemctl stop firewalld\n"
              "         3. IP correct: 192.168.56.151")
        return
    except Exception as e:
        check("REST API reachable", False, str(e))
        return

    # ========================================
    # 2. JAR 文件
    # ========================================
    print("\n2. JAR Files")
    try:
        resp = requests.get(f"{FLINK_URL}/jars", timeout=5).json()
        jars = resp.get('files', [])
        check(f"Uploaded JARs: {len(jars)}", len(jars) > 0,
              "If 0, upload JAR first" if not jars else "")

        for j in jars:
            print(f"    - {j['name']}  (id: {j['id'][:30]}...)")

        matched = [j for j in jars if TARGET_JAR in j.get('name', '')]
        check(f"Target JAR '{TARGET_JAR}' found", len(matched) > 0,
              f"Available: {[j['name'] for j in jars]}" if not matched else "")

    except Exception as e:
        check("JAR list", False, str(e))

    # ========================================
    # 3. 查找作业
    # ========================================
    print("\n3. Job Discovery")
    job_id = None
    try:
        resp = requests.get(f"{FLINK_URL}/jobs/overview", timeout=5).json()
        all_jobs = resp.get('jobs', [])
        running = [j for j in all_jobs if j['state'] == 'RUNNING']

        print(f"  All jobs ({len(all_jobs)}):")
        for j in all_jobs:
            print(f"    - '{j['name']}'  state={j['state']}  jid={j['jid'][:16]}...")

        check(f"Running jobs: {len(running)}", len(running) > 0,
              "No running jobs! Submit a job first." if not running else "")

        target = [j for j in running if j['name'] == JOB_NAME]
        if not target:
            # 尝试模糊匹配
            fuzzy = [j for j in running if 'seismic' in j['name'].lower() or 'Seismic' in j['name']]
            hint = ""
            if fuzzy:
                hint = f"Found similar: '{fuzzy[0]['name']}' - job_name mismatch!"
            check(f"Job '{JOB_NAME}' found", False, hint)
        else:
            check(f"Job '{JOB_NAME}' found", True)
            job_id = target[0]['jid']
            print(f"  Job ID: {job_id}")

    except Exception as e:
        check("Job list", False, str(e))

    if not job_id:
        print("\n*** Cannot proceed without running job ***")
        print("*** Check Java SeismicStreamJob.JOB_NAME matches ds2_config.yaml ***")
        return

    # ========================================
    # 4. 拓扑结构
    # ========================================
    print("\n4. Topology")
    vertices = []
    try:
        resp = requests.get(f"{FLINK_URL}/jobs/{job_id}", timeout=5).json()
        vertices = resp.get('vertices', [])
        check(f"Vertices: {len(vertices)}", len(vertices) >= 3)

        print(f"\n  {'Vertex Name':<40} {'P':>3}  {'Status':<10}")
        print(f"  {'-'*40} {'---':>3}  {'-'*10}")
        for v in vertices:
            print(f"  {v['name']:<40} {v['parallelism']:>3}  {v.get('status', '?'):<10}")

        # 检查 keyword 匹配
        from ds2_controller import Config
        config = Config.from_yaml('ds2_config.yaml')

        print(f"\n  Keyword Matching:")
        for v in vertices:
            op = config.find_operator_config(v['name'])
            if op:
                print(f"    {v['name']:<40} -> {op.arg_name}")
            else:
                print(f"    {v['name']:<40} -> *** NO MATCH ***")
                check(f"Keyword match: {v['name'][:30]}", False,
                      "Add keyword to ds2_config.yaml operator_mapping")

        # 检查边
        plan = resp.get('plan', {})
        edges = []
        for node in plan.get('nodes', []):
            for inp in node.get('inputs', []):
                edges.append((inp['id'], node['id']))
        check(f"Edges: {len(edges)}", len(edges) > 0)

    except ImportError:
        print("  [SKIP] Keyword matching (ds2_controller not importable from here)")
    except Exception as e:
        check("Topology", False, str(e))

    # ========================================
    # 5. 指标可用性
    # ========================================
    print("\n5. Metrics Availability")
    metrics_names = ["numRecordsIn", "numRecordsOut", "accumulateBusyTimeMs"]

    for v in vertices:
        vid = v['id']
        vname = v['name']
        try:
            url = f"{FLINK_URL}/jobs/{job_id}/vertices/{vid}/subtasks/metrics"
            params = f"?get={','.join(metrics_names)}"
            resp = requests.get(url + params, timeout=5)
            data = resp.json()

            if isinstance(data, list) and len(data) > 0:
                # 判断格式
                if 'metrics' in data[0]:
                    fmt = "subtask-level"
                    sample_metrics = {m['id']: m.get('value', '?')
                                      for m in data[0].get('metrics', [])}
                else:
                    fmt = "aggregated"
                    sample_metrics = {m.get('id', '?'): m.get('sum', m.get('value', '?'))
                                      for m in data}

                check(f"{vname[:30]}", True, f"format={fmt}")

                # 打印指标值
                for mn in metrics_names:
                    val = sample_metrics.get(mn, 'N/A')
                    print(f"           {mn}: {val}")
            else:
                check(f"{vname[:30]}", False, f"Empty response: {data}")

        except Exception as e:
            check(f"{vname[:30]}", False, str(e))

    # ========================================
    # 6. 差分测试（验证数据在流动）
    # ========================================
    print("\n6. Data Flow Test (10s diff)")

    if len(vertices) < 2:
        print("  [SKIP] Not enough vertices")
    else:
        test_vertex = vertices[1]  # 通常是第一个处理算子
        vid = test_vertex['id']
        vname = test_vertex['name']
        print(f"  Testing: {vname}")

        def get_agg_metrics(v_id):
            url = f"{FLINK_URL}/jobs/{job_id}/vertices/{v_id}/subtasks/metrics"
            params = f"?get={','.join(metrics_names)}"
            r = requests.get(url + params, timeout=5).json()
            result = {}
            if isinstance(r, list):
                if r and 'metrics' in r[0]:
                    # subtask 格式：汇总所有 subtask
                    totals = {mn: 0.0 for mn in metrics_names}
                    for st in r:
                        for m in st.get('metrics', []):
                            if m['id'] in totals:
                                try:
                                    totals[m['id']] += float(m.get('value', 0))
                                except (ValueError, TypeError):
                                    pass
                    result = totals
                else:
                    for m in r:
                        mid = m.get('id', '')
                        if mid in metrics_names:
                            try:
                                result[mid] = float(m.get('sum', m.get('value', 0)))
                            except (ValueError, TypeError):
                                result[mid] = 0.0
            return result

        try:
            m1 = get_agg_metrics(vid)
            print(f"  Sample 1: {m1}")
            print(f"  Waiting 10 seconds ...")
            time.sleep(10)
            m2 = get_agg_metrics(vid)
            print(f"  Sample 2: {m2}")

            print(f"\n  {'Metric':<30} {'Diff':>12} {'Rate(/s)':>10}")
            print(f"  {'-'*30} {'-'*12} {'-'*10}")

            data_flowing = False
            for mn in metrics_names:
                v1 = m1.get(mn, 0)
                v2 = m2.get(mn, 0)
                diff = v2 - v1
                rate = diff / 10.0
                print(f"  {mn:<30} {diff:>12.0f} {rate:>10.1f}")
                if mn == "numRecordsIn" and diff > 0:
                    data_flowing = True

            check("Data is flowing (numRecordsIn increasing)", data_flowing,
                  "If not flowing, check RocketMQ Producer is running" if not data_flowing else "")

        except Exception as e:
            check("Diff test", False, str(e))

    # ========================================
    # 7. HDFS Savepoint 目录（提示手动检查）
    # ========================================
    print("\n7. Savepoint Directory")
    print("  [INFO] Check on node01:")
    print("    hdfs dfs -ls /flink/savepoints")
    print("    hdfs dfs -mkdir -p /flink/savepoints")

    # ========================================
    # 总结
    # ========================================
    print("\n" + "=" * 60)
    print(f"Results: {PASS_COUNT} PASS, {FAIL_COUNT} FAIL")
    print("=" * 60)

    if FAIL_COUNT == 0:
        print("\nAll checks passed! You can proceed with:")
        print("  python3 ds2_controller.py -c ds2_config.yaml --dry-run")
    else:
        print("\nFix the FAIL items before running DS2.")


if __name__ == "__main__":
    main()



'''
python diagnose_ds2.py
'''