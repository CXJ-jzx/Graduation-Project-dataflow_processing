"""Step 5 主循环验证脚本（模拟模式，不实际重启作业）"""
import sys
import os
import time
import logging
import yaml
import numpy as np
from unittest.mock import MagicMock, patch

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "../ml_tuner_config.yaml")


def create_mock_executor(cfg):
    """创建模拟的 JobExecutor"""
    from job_executor import JobExecutor

    executor = JobExecutor(config_dict=cfg)

    # 模拟 apply_config 始终成功
    executor.apply_config = MagicMock(
        return_value=(True, "模拟成功")
    )
    # 模拟预热等待 0 秒
    executor.warm_up_wait = MagicMock()
    # 模拟健康检查始终通过
    executor.health_check = MagicMock(return_value=True)

    return executor


def create_mock_collector(cfg):
    """创建模拟的 ObservationCollector"""
    from observation_collector import ObservationCollector
    from parameter_space import ParameterSpace

    collector = ObservationCollector(config_dict=cfg)

    ps = ParameterSpace(config_dict=cfg)
    ideal_norm = ps.normalize({
        'checkpoint_interval': 30000, 'buffer_timeout': 80,
        'watermark_delay': 5000
    })

    call_count = [0]

    def mock_collect_window(n_windows=10, interval=30):
        """模拟采集：基于距离理想点的距离生成指标"""
        call_count[0] += 1
        noise = np.random.normal(0, 50)

        return {
            'avg_input_rate': 2500 + np.random.normal(0, 100),
            'peak_input_rate': 3000 + np.random.normal(0, 150),
            'input_cv': 0.25 + np.random.normal(0, 0.05),
            'active_node_count': 1500 + np.random.normal(0, 100),
            'avg_backlog': max(0, 50 + noise),
            'avg_busy_ratio': 0.4 + np.random.normal(0, 0.05),
            'max_busy_ratio': 0.6 + np.random.normal(0, 0.05),
            'total_parallelism': 22,
            'heap_usage_ratio': 0.5 + np.random.normal(0, 0.03),
            'latency_p99': max(100, 500 + noise),
            'checkpoint_avg_dur': max(200, 800 + noise),
            'backpressure_ratio': max(0, 0.03 + np.random.normal(0, 0.01)),
            'l2_hit_rate': 0.15 + np.random.normal(0, 0.02),
            'l2_occupancy': 1.0,
            'l3_hit_rate': 0.999 + np.random.normal(0, 0.0001),
            'l3_occupancy': 0.5,
        }

    collector.collect_window = mock_collect_window
    return collector


def test_main_loop_simulated():
    """模拟模式下完整运行 main loop"""
    print("=" * 60)
    print("  Step 5: 主循环模拟测试")
    print("=" * 60)

    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    # 缩短参数以加快测试
    cfg['scheduler']['total_rounds'] = 15
    cfg['scheduler']['warm_up_rounds'] = 5
    cfg['scheduler']['interval_seconds'] = 0
    cfg['safety']['warm_up_after_apply_seconds'] = 0
    cfg['collection']['windows_per_round'] = 1
    cfg['collection']['window_interval_seconds'] = 0

    from main import MLTunerLoop

    loop = MLTunerLoop.__new__(MLTunerLoop)
    loop.cfg = cfg
    loop._running = True
    loop._current_theta = None

    # 初始化组件
    from parameter_space import ParameterSpace
    from feature_engineer import FeatureEngineer
    from observation_store import ObservationStore
    from bayesian_optimizer import BayesianOptimizer

    loop.space = ParameterSpace(config_dict=cfg)
    loop.fe = FeatureEngineer(config_dict=cfg)

    # 使用独立的 CSV 避免污染
    cfg_store = dict(cfg)
    cfg_store['paths'] = dict(cfg.get('paths', {}))
    test_csv = os.path.join(os.path.dirname(CONFIG_PATH),
                            "../data", "test_step5_obs.csv")
    cfg_store['paths']['observation_csv'] = test_csv

    loop.store = ObservationStore(config_dict=cfg_store)
    loop.optimizer = BayesianOptimizer(config_dict=cfg)
    loop.executor = create_mock_executor(cfg)
    loop.collector = create_mock_collector(cfg)

    # 调度参数
    loop.total_rounds = 15
    loop.warm_up_rounds = 5
    loop.interval = 0
    loop.collect_windows = 1
    loop.collect_interval = 0
    loop.rollback_on_violation = True

    # 运行
    print("\n--- 开始模拟运行 (15轮: 5冷启动 + 10优化) ---\n")

    start_time = time.time()

    for round_id in range(loop.total_rounds):
        if not loop._running:
            break

        if round_id < loop.warm_up_rounds:
            loop._run_warmup_round(round_id)
        else:
            loop._run_optimize_round(round_id)

    elapsed = time.time() - start_time

    # 打印结果
    print("\n")
    loop._print_summary()

    # 验证
    total = loop.store.count_total()
    valid = loop.store.count_valid()

    print(f"\n--- 验证 ---")
    print(f"  总记录:   {total}")
    print(f"  有效记录: {valid}")
    print(f"  运行耗时: {elapsed:.1f}s")
    print(f"  apply_config 调用次数: {loop.executor.apply_config.call_count}")
    print(f"  warm_up_wait 调用次数: {loop.executor.warm_up_wait.call_count}")
    print(f"  health_check 调用次数: {loop.executor.health_check.call_count}")

    assert total >= 10, f"应至少有 10 条记录，实际 {total}"
    assert valid >= 8, f"应至少有 8 条有效记录，实际 {valid}"

    j_best, theta_best, best_round = loop.store.get_best()
    print(f"\n  最优 J:    {j_best:.6f}")
    print(f"  最优配置:  {theta_best}")
    print(f"  最优轮次:  {best_round}")

    assert j_best < 1.0, f"J 值应合理 (<1.0)，实际 {j_best}"
    assert theta_best is not None

    # 清理测试文件
    if os.path.exists(test_csv):
        os.remove(test_csv)

    print("\n✅ 主循环模拟测试通过")


def test_signal_handling():
    """测试优雅退出"""
    print("\n" + "=" * 60)
    print("测试: 信号处理（优雅退出）")
    print("=" * 60)

    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    from main import MLTunerLoop

    loop = MLTunerLoop.__new__(MLTunerLoop)
    loop._running = True

    # 模拟信号
    loop._signal_handler(2, None)  # SIGINT

    assert loop._running is False, "信号处理后应停止"
    print("  SIGINT → _running=False ✅")

    print("\n✅ 信号处理测试通过")


def test_interruptible_sleep():
    """测试可中断等待"""
    print("\n" + "=" * 60)
    print("测试: 可中断等待")
    print("=" * 60)

    from main import MLTunerLoop

    loop = MLTunerLoop.__new__(MLTunerLoop)
    loop._running = True

    # 正常等待 2 秒
    t0 = time.time()
    loop._interruptible_sleep(2)
    elapsed = time.time() - t0
    print(f"  正常等待: {elapsed:.1f}s (预期 ~2s)")
    assert 1.5 < elapsed < 3.0

    # 中断等待
    loop._running = False
    t0 = time.time()
    loop._interruptible_sleep(10)
    elapsed = time.time() - t0
    print(f"  中断等待: {elapsed:.1f}s (预期 <2s)")
    assert elapsed < 2.0

    print("\n✅ 可中断等待测试通过")


if __name__ == '__main__':
    print("=" * 60)
    print("  ML Tuner Step 5 主循环验证")
    print("=" * 60)

    try:
        test_signal_handling()
        test_interruptible_sleep()
        test_main_loop_simulated()

        print("\n" + "=" * 60)
        print("  🎉 Step 5 全部测试通过!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
