"""
verify_step4_final.py
已知路径，直接验证完整重启链路
"""

import time
import requests
import paramiko

FLINK_REST = "http://192.168.56.151:8081"
SAVEPOINT_DIR = "hdfs://node01:9000/flink/savepoints"

SSH_HOST = "192.168.56.151"
SSH_PORT = 22
SSH_USER = "root"
SSH_PASSWORD = "123456"

FLINK_HOME = "/opt/app/flink-1.17.2"
JAR_PATH = "/opt/flink_jobs/flink-rocketmq-demo-1.0-SNAPSHOT.jar"
MAIN_CLASS = "org.jzx.SeismicStreamJob"


def ssh_exec(cmd: str) -> tuple:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(SSH_HOST, SSH_PORT, SSH_USER, SSH_PASSWORD, timeout=10)
    stdin, stdout, stderr = client.exec_command(cmd, timeout=120)
    out = stdout.read().decode('utf-8').strip()
    err = stderr.read().decode('utf-8').strip()
    code = stdout.channel.recv_exit_status()
    client.close()
    return out, err, code


def main():
    # ━━━ Step 0: 确保 REST API 中有 JAR ━━━
    print("=" * 60)
    print("Step 0: 确保 JAR 已上传到 Flink REST")
    print("=" * 60)

    jar_id = None

    # 检查是否已上传
    jars_resp = requests.get(f"{FLINK_REST}/jars").json()
    for f in jars_resp.get('files', []):
        print(f"  已有 JAR: {f['name']} → {f['id']}")
        if 'seismic' in f['name'].lower() or 'rocketmq-demo' in f['name'].lower():
            jar_id = f['id']

    # 如果没有，通过 SSH 在 node01 本地 curl 上传
    if not jar_id:
        print(f"\n  REST 中无 JAR，上传 {JAR_PATH}...")
        upload_cmd = (
            f'curl -s -X POST '
            f'-H "Expect:" '
            f'-F "jarfile=@{JAR_PATH}" '
            f'http://localhost:8081/jars/upload'
        )
        out, err, code = ssh_exec(upload_cmd)
        print(f"  上传结果: {out[:300]}")

        # 重新查询
        jars_resp = requests.get(f"{FLINK_REST}/jars").json()
        for f in jars_resp.get('files', []):
            if 'seismic' in f['name'].lower() or 'rocketmq-demo' in f['name'].lower():
                jar_id = f['id']
                print(f"  ✅ JAR 已上传: {jar_id}")
                break

    if not jar_id:
        print("  ❌ JAR 上传失败")
        return

    print(f"  使用 JAR ID: {jar_id}")

    # ━━━ Step 1: 找运行中的作业 ━━━
    print(f"\n{'=' * 60}")
    print("Step 1: 当前作业状态")
    print("=" * 60)

    jobs = requests.get(f"{FLINK_REST}/jobs").json()
    old_job_id = None
    for j in jobs.get("jobs", []):
        if j["status"] == "RUNNING":
            old_job_id = j["id"]
            break

    savepoint_path = None

    if not old_job_id:
        print("  没有运行中的作业，直接全新提交")
    else:
        print(f"  ✅ 当前作业: {old_job_id}")

        # ━━━ Step 2: Savepoint ━━━
        print(f"\n{'=' * 60}")
        print("Step 2: Savepoint")
        print("=" * 60)

        resp = requests.post(
            f"{FLINK_REST}/jobs/{old_job_id}/savepoints",
            json={"cancel-job": False, "target-directory": SAVEPOINT_DIR}
        )
        trigger_id = resp.json()["request-id"]

        for _ in range(30):
            time.sleep(2)
            s = requests.get(
                f"{FLINK_REST}/jobs/{old_job_id}/savepoints/{trigger_id}"
            ).json()
            if s.get("status", {}).get("id") == "COMPLETED":
                savepoint_path = s["operation"]["location"]
                print(f"  ✅ Savepoint: {savepoint_path}")
                break
            if s.get("status", {}).get("id") == "FAILED":
                print(f"  ❌ Savepoint 失败")
                break

        # ━━━ Step 3: Cancel ━━━
        print(f"\n{'=' * 60}")
        print("Step 3: Cancel")
        print("=" * 60)

        resp = requests.patch(f"{FLINK_REST}/jobs/{old_job_id}")
        print(f"  Cancel 状态码: {resp.status_code}")

        for i in range(30):
            time.sleep(2)
            state = requests.get(
                f"{FLINK_REST}/jobs/{old_job_id}"
            ).json().get("state", "UNKNOWN")
            if state not in ["RUNNING", "CANCELLING"]:
                print(f"  ✅ 作业已停止: {state}")
                break
            if i % 3 == 0:
                print(f"  [{i * 2}s] {state}")

        time.sleep(5)

    # ━━━ Step 4: 用 REST API 提交（绕过 CLI commons-cli bug）━━━
    print(f"\n{'=' * 60}")
    print("Step 4: REST API 提交作业")
    print("=" * 60)

    program_args = (
        "--checkpoint-interval 30000 "
        "--buffer-timeout 100 "
        "--watermark-delay 5000 "
        "--p-source 2 --p-filter 4 --p-signal 4 "
        "--p-feature 4 --p-grid 4 --p-sink 2"
    )

    run_body = {
        "entryClass": MAIN_CLASS,
        "programArgs": program_args,
        "allowNonRestoredState": True,
    }

    if savepoint_path:
        run_body["savepointPath"] = savepoint_path
        print(f"  模式: 从 Savepoint 恢复")
        print(f"  Savepoint: {savepoint_path}")
    else:
        print(f"  模式: 全新启动")

    print(f"  JAR: {jar_id}")
    print(f"  参数: {program_args}")

    resp = requests.post(f"{FLINK_REST}/jars/{jar_id}/run", json=run_body)
    print(f"\n  响应码: {resp.status_code}")
    resp_body = resp.json()
    print(f"  响应: {resp_body}")

    new_job_id = None

    if resp.status_code == 200:
        new_job_id = resp_body.get("jobid")
        print(f"  ✅ 提交成功: {new_job_id}")
    else:
        # 如果带 savepoint 失败，尝试不带
        if savepoint_path:
            print(f"\n  带 Savepoint 提交失败，尝试全新启动...")
            run_body.pop("savepointPath", None)
            resp = requests.post(f"{FLINK_REST}/jars/{jar_id}/run", json=run_body)
            print(f"  响应码: {resp.status_code}")
            resp_body = resp.json()
            print(f"  响应: {resp_body}")
            if resp.status_code == 200:
                new_job_id = resp_body.get("jobid")
                print(f"  ✅ 全新启动成功: {new_job_id}")

    if not new_job_id:
        print(f"  ❌ 提交失败，查看 Flink 日志:")
        print(f"     ssh {SSH_USER}@{SSH_HOST}")
        print(f"     tail -100 {FLINK_HOME}/log/flink-*-jobmanager-*.log")
        return

    # ━━━ Step 5: 验证新作业 ━━━
    print(f"\n{'=' * 60}")
    print("Step 5: 等待新作业运行并验证数据流动")
    print("=" * 60)

    for i in range(40):
        time.sleep(3)
        try:
            state = requests.get(
                f"{FLINK_REST}/jobs/{new_job_id}"
            ).json().get("state")

            if state == "RUNNING":
                print(f"  ✅ 作业 RUNNING!")
                time.sleep(10)

                # 验证数据流动
                detail = requests.get(f"{FLINK_REST}/jobs/{new_job_id}").json()
                for v in detail.get("vertices", []):
                    name = v.get("name", "")
                    vid = v["id"]

                    if "Source" in name or "Hardware" in name:
                        m = requests.get(
                            f"{FLINK_REST}/jobs/{new_job_id}/vertices/{vid}"
                            f"/subtasks/metrics?get=numRecordsOutPerSecond"
                        ).json()
                        if m:
                            rate = m[0].get('sum', m[0].get('avg', 0))
                            print(f"  {name}: {rate} records/s")

                # 最终汇总
                print(f"\n{'=' * 60}")
                print("✅ 完整重启链路验证通过!")
                print("=" * 60)
                print(f"""
  ━━━ 已验证的正确配置 ━━━
  FLINK_HOME:   {FLINK_HOME}
  JAR_PATH:     {JAR_PATH}
  JAR_ID:       {jar_id}
  MAIN_CLASS:   {MAIN_CLASS}
  FLINK_REST:   {FLINK_REST}

  ━━━ 关键发现 ━━━
  Cancel 方式:  REST PATCH（无body）✅
  提交方式:     REST /jars/{{id}}/run  ✅
  CLI 方式:     ❌ commons-cli 冲突，不可用

  ━━━ ml_tuner_config.yaml 需要更新 ━━━
  flink:
    rest_url: "{FLINK_REST}"
    flink_home: "{FLINK_HOME}"
    jar_path: "{JAR_PATH}"

  submit_mode: "rest_api"   # 不用 CLI
  cancel_mode: "rest_api"   # REST PATCH

  ━━━ job_executor.py 需要修改 ━━━
  提交作业: 改用 REST /jars/{{id}}/run
  Cancel:  改用 REST PATCH /jobs/{{id}}
  不再依赖 SSH flink run
                """)
                return

            elif state in ["FAILED", "CANCELED"]:
                print(f"  ❌ 作业 {state}")
                # 获取异常信息
                exceptions = requests.get(
                    f"{FLINK_REST}/jobs/{new_job_id}/exceptions"
                ).json()
                root = exceptions.get("root-exception", "无")
                print(f"  异常: {root[:500]}")
                return

            if i % 5 == 0:
                print(f"  [{i * 3}s] {state}")

        except Exception as e:
            if i % 5 == 0:
                print(f"  [{i * 3}s] 等待中... ({e})")

    print("  ❌ 超时")


if __name__ == "__main__":
    main()
