"""
verify_step3_savepoint.py
真正触发一次 Savepoint（不取消作业），验证完整链路
"""

import requests
import time

FLINK_REST = "http://192.168.56.151:8081"
SAVEPOINT_DIR = "hdfs://node01:9000/flink/savepoints"

def main():
    # 找作业
    jobs = requests.get(f"{FLINK_REST}/jobs").json()
    job_id = None
    for j in jobs.get("jobs", []):
        if j["status"] == "RUNNING":
            job_id = j["id"]
            break
    if not job_id:
        print("❌ 没有运行中的作业")
        return

    print(f"作业 ID: {job_id}")

    # ━━━ 触发 Savepoint ━━━
    print("\n=== 触发 Savepoint（不取消作业）===")
    resp = requests.post(
        f"{FLINK_REST}/jobs/{job_id}/savepoints",
        json={"cancel-job": False, "target-directory": SAVEPOINT_DIR}
    )
    print(f"请求状态: {resp.status_code}")
    print(f"响应: {resp.json()}")

    if resp.status_code != 202:
        print("❌ Savepoint 触发失败")
        print("   可能原因:")
        print("   1. HDFS 未启动: 在 node01 执行 start-dfs.sh")
        print("   2. HDFS 目录不存在: hdfs dfs -mkdir -p /flink/savepoints")
        print("   3. 权限问题: hdfs dfs -chmod 777 /flink/savepoints")
        return

    trigger_id = resp.json()["request-id"]

    # ━━━ 等待完成 ━━━
    print(f"\n等待 Savepoint 完成 (trigger_id: {trigger_id})")
    for i in range(30):
        time.sleep(2)
        status_resp = requests.get(
            f"{FLINK_REST}/jobs/{job_id}/savepoints/{trigger_id}"
        ).json()

        status = status_resp.get("status", {}).get("id", "UNKNOWN")
        print(f"  [{i*2}s] 状态: {status}")

        if status == "COMPLETED":
            location = status_resp["operation"]["location"]
            print(f"\n✅ Savepoint 成功!")
            print(f"   路径: {location}")
            print(f"\n   下一步验证: 用这个路径从 savepoint 恢复作业")
            print(f"   命令: bin/flink run -d -s {location} ...")
            return

        if status == "FAILED":
            cause = status_resp.get("operation", {}).get("failure-cause", {})
            print(f"\n❌ Savepoint 失败!")
            print(f"   原因: {cause}")
            print(f"\n   常见原因:")
            print(f"   1. HDFS 未启动")
            print(f"   2. state.backend 配置问题")
            print(f"   3. 算子没有实现 snapshotState")
            return

    print("❌ Savepoint 超时（60秒）")


if __name__ == "__main__":
    main()
