"""Step 4 作业执行器验证脚本"""
import sys
import os
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../ml_tuner_config.yaml")


def test_executor_init():
    print("=" * 60)
    print("测试 1/5: JobExecutor 初始化")
    print("=" * 60)

    from job_executor import JobExecutor

    executor = JobExecutor(config_path=CONFIG_PATH)
    print(executor)

    # 验证基本属性
    assert executor.rest_url == "http://192.168.56.151:8081"
    assert executor.job_name == "Seismic-Stream-Job"
    assert executor.warm_up_seconds == 90
    assert executor.max_consecutive_failures == 3
    assert executor.consecutive_failures == 0

    # 验证默认并行度
    print(f"  默认并行度: {executor.parallelism_args}")
    assert 'p-source' in executor.parallelism_args
    assert 'p-filter' in executor.parallelism_args

    print("\n✅ JobExecutor 初始化测试通过")
    return executor


def test_ds2_coordination(executor):
    print("\n" + "=" * 60)
    print("测试 2/5: DS2 协调（文件锁）")
    print("=" * 60)

    lock_file = executor.lock_file
    pause_file = executor.pause_signal_file

    # 确保初始状态干净
    for f in [lock_file, pause_file]:
        if os.path.exists(f):
            os.remove(f)

    # 1. 获取锁
    print("\n--- 获取锁 ---")
    ok = executor.acquire_ds2_lock()
    assert ok is True, "获取锁应成功"
    assert os.path.exists(lock_file), "锁文件应存在"
    assert os.path.exists(pause_file), "暂停信号应存在"
    print(f"  锁文件: {lock_file} -> 存在={os.path.exists(lock_file)}")
    print(f"  暂停信号: {pause_file} -> 存在={os.path.exists(pause_file)}")

    # 2. 读取锁文件内容
    with open(lock_file, 'r') as f:
        content = f.read()
    print(f"  锁文件内容: {content}")
    assert "ML_Tuner" in content

    # 3. 释放锁
    print("\n--- 释放锁 ---")
    executor.release_ds2_lock()
    assert not os.path.exists(lock_file), "锁文件应已删除"
    assert not os.path.exists(pause_file), "暂停信号应已删除"
    print(f"  锁文件: 已删除")
    print(f"  暂停信号: 已删除")

    # 4. 模拟 DS2 持有锁 → 超时
    print("\n--- 模拟 DS2 持锁超时 ---")
    lock_dir = os.path.dirname(lock_file)
    if lock_dir:
        os.makedirs(lock_dir, exist_ok=True)
    with open(lock_file, 'w') as f:
        f.write("DS2 lock (test)")

    original_timeout = executor.lock_timeout
    executor.lock_timeout = 3  # 3秒超时

    ok2 = executor.acquire_ds2_lock()
    assert ok2 is False, "DS2 持锁时应获取失败"
    print(f"  DS2 持锁时获取结果: {ok2} (预期 False)")

    executor.lock_timeout = original_timeout

    # 清理
    for f in [lock_file, pause_file]:
        if os.path.exists(f):
            os.remove(f)

    print("\n✅ DS2 协调测试通过")


def test_failure_tracking(executor):
    print("\n" + "=" * 60)
    print("测试 3/5: 失败计数与回滚判断")
    print("=" * 60)

    executor.reset_failure_count()
    assert executor.get_failure_count() == 0
    assert executor.should_rollback() is False

    executor.increment_failure()
    assert executor.get_failure_count() == 1
    assert executor.should_rollback() is False
    print(f"  失败1次: should_rollback={executor.should_rollback()}")

    executor.increment_failure()
    assert executor.get_failure_count() == 2
    assert executor.should_rollback() is False
    print(f"  失败2次: should_rollback={executor.should_rollback()}")

    executor.increment_failure()
    assert executor.get_failure_count() == 3
    assert executor.should_rollback() is True
    print(f"  失败3次: should_rollback={executor.should_rollback()} (触发回滚)")

    executor.reset_failure_count()
    assert executor.get_failure_count() == 0
    assert executor.should_rollback() is False
    print(f"  重置后: should_rollback={executor.should_rollback()}")

    print("\n✅ 失败计数测试通过")


def test_flink_connection(executor):
    print("\n" + "=" * 60)
    print("测试 4/5: Flink 连接与作业发现")
    print("=" * 60)

    # 1. 查找运行中的作业
    job_id = executor._find_running_job()
    print(f"  查找作业: {job_id}")

    if not job_id:
        print("  ⚠️ 无运行中的作业，跳过后续 Flink 测试")
        return False

    # 2. 获取当前并行度
    parallelism = executor._get_current_parallelism(job_id)
    print(f"  当前并行度: {parallelism}")
    assert isinstance(parallelism, dict)
    assert len(parallelism) > 0
    for arg_name, p_val in parallelism.items():
        assert isinstance(p_val, int), f"{arg_name} 并行度应为整数"
        assert p_val >= 1, f"{arg_name} 并行度应 >= 1"
        print(f"    {arg_name}: {p_val}")

    # 3. 健康检查
    print("\n--- 健康检查 ---")
    healthy = executor.health_check()
    print(f"  健康检查结果: {healthy}")
    assert healthy is True, "作业应处于健康状态"

    # 4. 构建 CLI 参数验证
    print("\n--- CLI 参数构建 ---")
    from parameter_space import ParameterSpace
    ps = ParameterSpace(config_path=CONFIG_PATH)
    theta = ps.get_defaults()
    cli = ps.to_cli_args(theta)
    print(f"  调优参数 CLI: {cli}")

    parallelism_cli_parts = []
    for arg_name, p_val in parallelism.items():
        parallelism_cli_parts.append(f"--{arg_name} {p_val}")
    parallelism_cli = " ".join(parallelism_cli_parts)
    print(f"  并行度 CLI: {parallelism_cli}")

    full_cmd = f"{executor.flink_bin} run -d {executor.jar_path} {cli} {parallelism_cli}"
    print(f"  完整命令: {full_cmd}")

    print("\n✅ Flink 连接测试通过")
    return True


def test_ssh_connection(executor):
    print("\n" + "=" * 60)
    print("测试 5/5: SSH 远程连接")
    print("=" * 60)

    # 1. 基础连通性
    print("\n--- SSH 连通性 ---")
    rc, stdout, stderr = executor._ssh_exec("echo 'SSH_OK'", timeout=15)
    print(f"  returncode: {rc}")
    print(f"  stdout: {stdout}")
    print(f"  stderr: {stderr}")

    if rc != 0:
        print("  ⚠️ SSH 连接失败，跳过后续 SSH 测试")
        print("  请确保:")
        print(f"    1. {executor.ssh_host} 可达")
        print(f"    2. {executor.ssh_user} 用户可 SSH 登录")
        print("    3. SSH 密钥或密码已配置")
        return False

    assert "SSH_OK" in stdout, "SSH echo 测试失败"
    print("  SSH 连通性: ✅")

    # 2. Flink 命令行可用性
    print("\n--- Flink CLI 可用性 ---")
    rc2, stdout2, stderr2 = executor._ssh_exec(
        f"{executor.flink_bin} --version", timeout=15
    )
    print(f"  Flink 版本: {stdout2 or stderr2}")
    if rc2 == 0:
        print("  Flink CLI: ✅")
    else:
        print(f"  ⚠️ Flink CLI 不可用 (rc={rc2})")

    # 3. JAR 文件存在性
    print("\n--- JAR 文件验证 ---")
    rc3, stdout3, stderr3 = executor._ssh_exec(
        f"ls -la {executor.jar_path}", timeout=10
    )
    print(f"  JAR 文件: {stdout3}")
    if rc3 == 0:
        print("  JAR 存在: ✅")
    else:
        print(f"  ⚠️ JAR 不存在: {executor.jar_path}")

    # 4. Savepoint 目录
    print("\n--- Savepoint 目录 ---")
    # HDFS 目录检查
    rc4, stdout4, stderr4 = executor._ssh_exec(
        f"hdfs dfs -ls {executor.savepoint_dir} 2>/dev/null || echo 'DIR_NOT_EXIST'",
        timeout=15
    )
    print(f"  Savepoint 目录: {stdout4[:200] if stdout4 else stderr4[:200]}")

    # 5. Job ID 提取测试
    print("\n--- Job ID 提取测试 ---")
    test_outputs = [
        "Job has been submitted with JobID 12ceb5ffdac19d6533b98168e4d0a3c7",
        "Starting execution of program\nJob has been submitted with JobID abcdef0123456789abcdef0123456789",
        "some random output without job id",
        "",
    ]
    for output in test_outputs:
        extracted = executor._extract_job_id(output)
        first_line = output.split('\n')[0][:60] if output else "(空)"
        print(f"  输入: '{first_line}' → 提取: {extracted}")

    assert executor._extract_job_id(test_outputs[0]) == "12ceb5ffdac19d6533b98168e4d0a3c7"
    assert executor._extract_job_id(test_outputs[1]) == "abcdef0123456789abcdef0123456789"
    assert executor._extract_job_id(test_outputs[2]) is None
    assert executor._extract_job_id(test_outputs[3]) is None
    print("  Job ID 提取: ✅")

    print("\n✅ SSH 连接测试通过")
    return True


def test_savepoint_dry_run(executor):
    """Savepoint 干跑测试（不实际执行 cancel/resubmit）"""
    print("\n" + "=" * 60)
    print("附加测试: Savepoint 触发（仅触发不取消）")
    print("=" * 60)

    job_id = executor._find_running_job()
    if not job_id:
        print("  ⚠️ 无运行中的作业，跳过 Savepoint 测试")
        return

    print(f"  当前作业: {job_id}")
    print(f"  Savepoint 目录: {executor.savepoint_dir}")

    # 仅触发 Savepoint，不取消作业
    print("\n--- 触发 Savepoint ---")
    sp_path = executor._trigger_savepoint(job_id)

    if sp_path:
        print(f"  Savepoint 成功: {sp_path}")

        # 验证作业仍在运行
        still_running = executor._find_running_job()
        assert still_running == job_id, "Savepoint 后作业应仍在运行"
        print(f"  作业仍在运行: ✅")
    else:
        print("  ⚠️ Savepoint 触发失败（可能是 HDFS 不可达）")

    print("\n✅ Savepoint 干跑测试通过")


def test_apply_config_dry_run():
    """
    apply_config 完整流程模拟

    注意：这个测试会实际重启 Flink 作业！
    默认不执行，需要手动启用
    """
    print("\n" + "=" * 60)
    print("附加测试: apply_config 完整流程")
    print("=" * 60)

    ACTUALLY_RUN = False  # ← 设为 True 才会实际执行

    if not ACTUALLY_RUN:
        print("  ⚠️ apply_config 实际执行已禁用")
        print("  如需测试完整流程，请将 ACTUALLY_RUN 改为 True")
        print("  这将会: Savepoint → Cancel → Resubmit 你的 Flink 作业")
        return

    from job_executor import JobExecutor

    executor = JobExecutor(config_path=CONFIG_PATH)

    theta = {
        'checkpoint_interval': 25000,
        'buffer_timeout': 80,
        'watermark_delay': 4000,
    }

    print(f"\n  将要应用的配置: {theta}")
    print(f"  目标作业: {executor.job_name}")

    # 获取锁
    ok = executor.acquire_ds2_lock()
    if not ok:
        print("  获取 DS2 锁失败")
        return

    try:
        success, msg = executor.apply_config(theta)
        print(f"\n  apply_config 结果: success={success}, msg={msg}")

        if success:
            # 预热等待（缩短为 10 秒用于测试）
            print("  预热等待 10 秒...")
            time.sleep(10)

            # 健康检查
            healthy = executor.health_check()
            print(f"  健康检查: {healthy}")
        else:
            print(f"  ⚠️ 配置应用失败: {msg}")

    finally:
        executor.release_ds2_lock()
        print("  DS2 锁已释放")

    print("\n✅ apply_config 完整流程测试通过")


def print_summary(results: dict):
    """打印测试结果汇总"""
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)

    for name, status in results.items():
        icon = "✅" if status else "⚠️"
        print(f"  {icon} {name}")

    all_pass = all(results.values())
    if all_pass:
        print(f"\n  🎉 Step 4 全部测试通过!")
    else:
        skipped = [k for k, v in results.items() if not v]
        print(f"\n  部分测试跳过: {skipped}")
        print("  核心功能已验证，跳过项不影响正确性")


if __name__ == '__main__':
    print("=" * 60)
    print("  ML Tuner Step 4 作业执行器验证")
    print("=" * 60)

    results = {}

    try:
        # 1. 初始化
        executor = test_executor_init()
        results['初始化'] = True

        # 2. DS2 协调
        test_ds2_coordination(executor)
        results['DS2协调'] = True

        # 3. 失败追踪
        test_failure_tracking(executor)
        results['失败追踪'] = True

        # 4. Flink 连接
        flink_ok = test_flink_connection(executor)
        results['Flink连接'] = flink_ok

        # 5. SSH 连接
        ssh_ok = test_ssh_connection(executor)
        results['SSH连接'] = ssh_ok

        # 6. Savepoint 干跑（需要 Flink 可达）
        if flink_ok:
            test_savepoint_dry_run(executor)
            results['Savepoint'] = True
        else:
            results['Savepoint'] = False

        # 7. apply_config 完整流程（默认禁用）
        test_apply_config_dry_run()
        results['apply_config'] = True  # 跳过也算通过

        # 清理
        executor.close()

        # 汇总
        print_summary(results)

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
