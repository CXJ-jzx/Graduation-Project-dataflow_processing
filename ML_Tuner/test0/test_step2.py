"""Step 2 基础组件验证脚本"""
import sys
import os
import logging

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../ml_tuner_config.yaml")


def test_parameter_space():
    print("=" * 60)
    print("测试 1/4: ParameterSpace")
    print("=" * 60)

    from parameter_space import ParameterSpace
    ps = ParameterSpace(config_path=CONFIG_PATH)
    print(ps)
    print()

    # 测试默认值
    defaults = ps.get_defaults()
    print(f"默认参数: {defaults}")

    # 测试归一化/反归一化
    norm = ps.normalize(defaults)
    print(f"归一化: {norm}")

    denorm = ps.denormalize(norm)
    print(f"反归一化: {denorm}")

    for name in ps.PARAM_NAMES:
        assert int(denorm[name]) == int(defaults[name]), \
            f"归一化往返不一致! {name}: {denorm[name]} != {defaults[name]}"

    # 测试 LHS 采样
    lhs = ps.latin_hypercube_sample(5, seed=42)
    print(f"\nLHS 5个候选:")
    for i, s in enumerate(lhs):
        print(f"  {i}: {s}")
        print(f"     CLI: {ps.to_cli_args(s)}")

    # 测试边界检查
    for s in lhs:
        for name in ps.PARAM_NAMES:
            p = ps.params[name]
            assert p['min'] <= s[name] <= p['max'], \
                f"越界! {name}={s[name]}, 范围=[{p['min']}, {p['max']}]"

    # 测试邻域采样
    center = ps.normalize(defaults)
    neighbors = ps.neighbor_sample(center, range_pct=0.15, n=3)
    print(f"\n邻域采样 (range=15%):")
    for i, nb in enumerate(neighbors):
        nb_dict = ps.denormalize(nb)
        dist = ps.distance(center, nb)
        print(f"  {i}: {nb_dict}, 距离={dist:.4f}")

    # 测试距离计算
    a = ps.normalize({'checkpoint_interval': 5000, 'buffer_timeout': 1, 'watermark_delay': 1000})
    b = ps.normalize({'checkpoint_interval': 120000, 'buffer_timeout': 200, 'watermark_delay': 30000})
    d = ps.distance(a, b)
    print(f"\n最远两点距离: {d:.4f} (理论最大={3**0.5:.4f})")

    # 测试配置比较
    assert ps.is_same_config(defaults, defaults) is True
    assert ps.is_same_config(defaults, lhs[0]) is False

    print("\n✅ ParameterSpace 全部测试通过")
    return ps


def test_feature_engineer():
    print("\n" + "=" * 60)
    print("测试 2/4: FeatureEngineer")
    print("=" * 60)

    from feature_engineer import FeatureEngineer
    fe = FeatureEngineer(config_path=CONFIG_PATH)

    # 构造模拟特征
    fake_features = {
        'avg_input_rate': 2000, 'peak_input_rate': 3000, 'input_cv': 0.3,
        'active_node_count': 500, 'avg_backlog': 100, 'avg_busy_ratio': 0.6,
        'max_busy_ratio': 0.8, 'total_parallelism': 22, 'heap_usage_ratio': 0.5,
        'latency_p99': 600, 'checkpoint_avg_dur': 300, 'backpressure_ratio': 0.05,
        'l2_hit_rate': 0.03, 'l2_occupancy': 1.0, 'l3_hit_rate': 0.99, 'l3_occupancy': 0.5,
    }

    # 1. 归一化
    norm = fe.normalize_features(fake_features)
    print(f"归一化特征: shape={norm.shape}, min={norm.min():.3f}, max={norm.max():.3f}")
    assert norm.shape == (16,), f"特征维度错误: {norm.shape}"
    assert 0.0 <= norm.min() and norm.max() <= 1.0, "归一化越界"

    # 2. 目标函数
    j, parts = fe.compute_objective(fake_features)
    print(f"目标函数: {fe.format_objective(parts)}")
    assert 0 < j < 10, f"J值异常: {j}"
    assert abs(parts['j_total'] - j) < 0.001

    # 3. 约束检查（正常情况）
    valid, violations = fe.check_constraints(fake_features)
    print(f"约束检查(正常): valid={valid}, violations={violations}")
    assert valid is True

    # 4. 约束检查（违反延迟）
    bad_features = dict(fake_features, latency_p99=8000)
    valid2, violations2 = fe.check_constraints(bad_features)
    print(f"约束检查(超延迟): valid={valid2}, violations={violations2}")
    assert valid2 is False
    assert len(violations2) > 0

    # 5. 约束检查（违反背压）
    bad_features2 = dict(fake_features, backpressure_ratio=0.9)
    valid3, violations3 = fe.check_constraints(bad_features2)
    print(f"约束检查(超背压): valid={valid3}, violations={violations3}")
    assert valid3 is False

    # 6. 多次采样聚合
    samples = [
        dict(fake_features, latency_p99=500),
        dict(fake_features, latency_p99=600),
        dict(fake_features, latency_p99=900),
    ]
    agg = fe.aggregate_median(samples)
    print(f"聚合延迟: {agg['latency_p99']} (期望600)")
    assert agg['latency_p99'] == 600.0, f"中位数聚合错误: {agg['latency_p99']}"

    # 7. 吞吐量基线动态更新
    fe.set_throughput_baseline(0)  # 重置
    j1, _ = fe.compute_objective(dict(fake_features, avg_input_rate=1000))
    j2, _ = fe.compute_objective(dict(fake_features, avg_input_rate=2000))
    j3, _ = fe.compute_objective(dict(fake_features, avg_input_rate=1500))
    print(f"吞吐量基线: {fe.get_throughput_baseline():.0f} (期望2000)")
    assert fe.get_throughput_baseline() == 2000.0

    # 8. 特征格式化
    print(f"\n特征详情:\n{fe.format_features(fake_features)}")

    print("\n✅ FeatureEngineer 全部测试通过")
    return fe


def test_observation_store(ps):
    print("\n" + "=" * 60)
    print("测试 3/4: ObservationStore")
    print("=" * 60)

    from observation_store import ObservationStore

    # 清除旧数据
    store = ObservationStore(config_path=CONFIG_PATH)
    store.records = []
    store.next_round_id = 0

    # 1. 添加记录
    for i in range(5):
        theta = {
            'checkpoint_interval': 10000 + i * 10000,
            'buffer_timeout': 20 + i * 30,
            'watermark_delay': 2000 + i * 2000
        }
        features = {
            'avg_input_rate': 1500 + i * 200,
            'peak_input_rate': 2000 + i * 200,
            'input_cv': 0.2,
            'active_node_count': 400 + i * 50,
            'avg_backlog': 50,
            'avg_busy_ratio': 0.4 + i * 0.1,
            'max_busy_ratio': 0.6 + i * 0.05,
            'total_parallelism': 22,
            'heap_usage_ratio': 0.5,
            'latency_p99': 300 + i * 100,
            'checkpoint_avg_dur': 200 + i * 50,
            'backpressure_ratio': 0.02 + i * 0.01,
            'l2_hit_rate': 0.03,
            'l2_occupancy': 1.0,
            'l3_hit_rate': 0.99,
            'l3_occupancy': 0.5,
        }
        # J 值：让第3条（i=2）最小
        j = 0.6 - 0.1 * (2 - abs(i - 2))
        parts = {
            'f_latency': 0.1, 'f_throughput': 0.2,
            'f_checkpoint': 0.1, 'f_resource': 0.1,
            'j_total': j
        }
        store.add(theta, features, j, parts, is_valid=True)

    print(f"添加5条后: {store}")
    assert store.count_total() == 5
    assert store.count_valid() == 5

    # 2. 最优记录
    j_best, theta_best, best_round = store.get_best()
    print(f"最优: round={best_round}, J={j_best:.4f}, θ={theta_best}")

    # 3. Top-K
    top3 = store.get_top_k(3)
    print(f"Top-3: {top3}")
    assert len(top3) == 3

    # 4. 导出训练数据
    X, y = store.get_training_data(ps)
    print(f"训练数据: X.shape={X.shape}, y.shape={y.shape}")
    assert X.shape == (5, 3)
    assert y.shape == (5,)

    # 5. 标记无效
    store.mark_last_invalid("测试标记")
    assert store.count_valid() == 4
    print(f"标记无效后: {store}")

    # 6. CSV 持久化
    store.save()
    print(f"CSV已保存: {store.csv_path}")

    # 7. CSV 恢复
    store2 = ObservationStore(config_path=CONFIG_PATH)
    store2.load()
    print(f"从CSV恢复: {store2}")
    assert store2.count_total() == store.count_total()
    assert store2.count_valid() == store.count_valid()

    # 8. 吞吐量基线恢复
    baseline = store2.get_throughput_baseline()
    print(f"恢复吞吐量基线: {baseline:.0f}")

    # 9. 最近一条记录
    last = store2.get_last()
    print(f"最近记录: round={last.round_id}, valid={last.is_valid}")

    print("\n✅ ObservationStore 全部测试通过")


def test_observation_collector():
    print("\n" + "=" * 60)
    print("测试 4/4: ObservationCollector（实际连接Flink）")
    print("=" * 60)

    from observation_collector import ObservationCollector

    collector = ObservationCollector(config_path=CONFIG_PATH)
    print(collector)

    # 1. 发现 Job 和 Vertex
    ok = collector.discover()
    print(f"发现结果: {ok}")

    if not ok:
        print("⚠️ 无法连接 Flink 或作业未运行，跳过实际采集测试")
        print("   请确保 Flink 作业正在运行后重试")
        collector.close()
        return

    # 2. 检查作业状态
    running = collector.is_job_running()
    print(f"作业运行中: {running}")
    assert running is True

    # 3. 单次采集
    print("\n--- 单次采集 ---")
    features = collector.collect_once()
    if features:
        print(f"采集到 {len(features)} 个特征:")
        for k in sorted(features.keys()):
            v = features[k]
            print(f"  {k:25s}: {v:.4f}")

        # 验证关键特征不为空
        assert features.get('avg_input_rate', 0) > 0, "avg_input_rate 应大于0"
        assert features.get('total_parallelism', 0) > 0, "total_parallelism 应大于0"
        assert features.get('latency_p99', -1) >= 0, "latency_p99 应非负"
        assert features.get('l2_occupancy', -1) >= 0, "l2_occupancy 应非负"
        assert features.get('l3_hit_rate', -1) >= 0, "l3_hit_rate 应非负"

        print("\n关键指标验证:")
        print(f"  吞吐量: {features['avg_input_rate']:.0f} records/s")
        print(f"  端到端P99延迟: {features['latency_p99']:.1f} ms")
        print(f"  平均繁忙率: {features['avg_busy_ratio']:.4f}")
        print(f"  背压比例: {features['backpressure_ratio']:.4f}")
        print(f"  L2命中率: {features['l2_hit_rate']:.4f}")
        print(f"  L3命中率: {features['l3_hit_rate']:.4f}")
        print(f"  Checkpoint耗时: {features['checkpoint_avg_dur']:.1f} ms")
        print(f"  堆内存使用率: {features['heap_usage_ratio']:.4f}")
    else:
        print("⚠️ 单次采集返回空")

    # 4. 不执行完整窗口采集（太耗时），仅验证接口
    print("\n--- 窗口采集接口验证 ---")
    print(f"  窗口配置: {collector.sample_count}次, 每{collector.interval_seconds}s")
    print(f"  总时长: {collector.window_seconds}s")
    print("  (跳过实际窗口采集，可手动调用 collector.collect_window())")

    # 5. 缓存重置测试
    collector.reset_cache()
    assert collector.get_job_id() is None
    ok2 = collector.discover()
    assert ok2 is True
    print(f"\n缓存重置后重新发现: {ok2}")

    collector.close()
    print("\n✅ ObservationCollector 全部测试通过")


def test_integration():
    """端到端集成测试"""
    print("\n" + "=" * 60)
    print("集成测试: 完整流程")
    print("=" * 60)

    from parameter_space import ParameterSpace
    from feature_engineer import FeatureEngineer
    from observation_store import ObservationStore
    from observation_collector import ObservationCollector

    ps = ParameterSpace(config_path=CONFIG_PATH)
    fe = FeatureEngineer(config_path=CONFIG_PATH)
    store = ObservationStore(config_path=CONFIG_PATH)
    collector = ObservationCollector(config_path=CONFIG_PATH)

    # 模拟一轮完整流程
    print("\n--- 模拟调优轮次 ---")

    # 1. 生成候选参数
    theta = ps.get_defaults()
    print(f"候选参数: {theta}")
    print(f"CLI参数: {ps.to_cli_args(theta)}")

    # 2. 采集指标（如果Flink可达则实际采集，否则用模拟数据）
    if collector.discover():
        features_raw = collector.collect_once()
        if not features_raw:
            features_raw = _mock_features()
            print("(使用模拟特征)")
    else:
        features_raw = _mock_features()
        print("(Flink不可达，使用模拟特征)")

    # 3. 归一化
    x_norm = fe.normalize_features(features_raw)
    print(f"归一化特征: shape={x_norm.shape}")

    # 4. 计算目标函数
    j, parts = fe.compute_objective(features_raw)
    print(f"目标函数: {fe.format_objective(parts)}")

    # 5. 约束检查
    valid, violations = fe.check_constraints(features_raw)
    print(f"约束: valid={valid}")

    # 6. 存储
    store.records = []  # 清空旧数据
    store.next_round_id = 0
    store.add(theta, features_raw, j, parts, valid, violations)
    print(f"存储: {store}")

    # 7. 参数空间归一化
    theta_norm = ps.normalize(theta)
    print(f"参数归一化: {theta_norm}")

    collector.close()
    print("\n✅ 集成测试通过")


def _mock_features():
    """模拟特征数据"""
    return {
        'avg_input_rate': 1800, 'peak_input_rate': 2500, 'input_cv': 0.25,
        'active_node_count': 450, 'avg_backlog': 80, 'avg_busy_ratio': 0.55,
        'max_busy_ratio': 0.75, 'total_parallelism': 22, 'heap_usage_ratio': 0.45,
        'latency_p99': 500, 'checkpoint_avg_dur': 250, 'backpressure_ratio': 0.03,
        'l2_hit_rate': 0.035, 'l2_occupancy': 1.0, 'l3_hit_rate': 0.999, 'l3_occupancy': 0.5,
    }


if __name__ == '__main__':
    print("=" * 60)
    print("  ML Tuner Step 2 基础组件验证")
    print("=" * 60)

    try:
        ps = test_parameter_space()
        fe = test_feature_engineer()
        test_observation_store(ps)
        test_observation_collector()
        test_integration()

        print("\n" + "=" * 60)
        print("  🎉 Step 2 全部测试通过!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
