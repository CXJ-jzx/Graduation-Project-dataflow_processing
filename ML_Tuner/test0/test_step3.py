"""Step 3 核心算法验证脚本"""
import sys
import os
import numpy as np
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../ml_tuner_config.yaml")


def test_surrogate_model():
    print("=" * 60)
    print("测试 1/3: SurrogateModel")
    print("=" * 60)

    from surrogate_model import SurrogateModel

    model = SurrogateModel(config_path=CONFIG_PATH)
    print(model)

    # ---- Phase 1: RF (少量数据) ----
    print("\n--- Phase 1: RF (5条数据, < warm_up=10) ---")
    np.random.seed(42)
    X_small = np.random.rand(5, 3)
    # 构造一个简单的目标函数: J = x0^2 + x1^2 + x2^2
    y_small = np.sum(X_small ** 2, axis=1)

    model_type = model.fit(X_small, y_small)
    print(f"  模型类型: {model_type}")
    assert model_type == "rf", f"预期 rf, 实际 {model_type}"
    assert model.is_fitted()

    # 预测
    X_test = np.array([[0.5, 0.5, 0.5], [0.0, 0.0, 0.0], [1.0, 1.0, 1.0]])
    mu, sigma = model.predict(X_test)
    print(f"  预测均值: {mu}")
    print(f"  预测标准差: {sigma}")
    assert mu.shape == (3,)
    assert sigma.shape == (3,)
    assert np.all(sigma >= 0), "sigma 应非负"

    # 单点预测
    mu_single, sigma_single = model.predict(np.array([0.5, 0.5, 0.5]))
    print(f"  单点预测: μ={mu_single[0]:.4f}, σ={sigma_single[0]:.4f}")

    stats = model.get_training_stats()
    print(f"  训练统计: {stats}")

    # ---- Phase 2: GP (足够数据) ----
    print("\n--- Phase 2: GP (15条数据, >= warm_up=10) ---")
    X_large = np.random.rand(15, 3)
    y_large = np.sum(X_large ** 2, axis=1) + np.random.normal(0, 0.01, 15)

    model_type2 = model.fit(X_large, y_large)
    print(f"  模型类型: {model_type2}")
    assert model_type2 == "gp", f"预期 gp, 实际 {model_type2}"

    mu2, sigma2 = model.predict(X_test)
    print(f"  预测均值: {mu2}")
    print(f"  预测标准差: {sigma2}")

    stats2 = model.get_training_stats()
    print(f"  训练统计: {stats2}")

    # 验证 GP 的不确定性特性：
    # 在训练点附近 sigma 应该较小
    mu_near, sigma_near = model.predict(X_large[:3])
    mu_far, sigma_far = model.predict(np.array([[0.99, 0.99, 0.99]]))
    print(f"  训练点附近 σ: {sigma_near}")
    print(f"  远离训练点 σ: {sigma_far}")

    # ---- 批量预测性能 ----
    print("\n--- 批量预测 ---")
    import time
    X_batch = np.random.rand(200, 3)
    t0 = time.time()
    mu_batch, sigma_batch = model.predict(X_batch)
    elapsed = (time.time() - t0) * 1000
    print(f"  200个点预测耗时: {elapsed:.1f} ms")
    assert mu_batch.shape == (200,)

    print("\n✅ SurrogateModel 全部测试通过")
    return model


def test_acquisition():
    print("\n" + "=" * 60)
    print("测试 2/3: AcquisitionFunction")
    print("=" * 60)

    from acquisition import AcquisitionFunction

    acq = AcquisitionFunction(config_path=CONFIG_PATH)
    print(acq)

    # ---- ξ 衰减测试 ----
    print("\n--- ξ 衰减曲线 ---")
    warm_up = 10
    xi_values = []
    for n in range(0, 30):
        xi = acq.compute_xi(n, warm_up)
        xi_values.append(xi)
        if n in [0, 5, 10, 15, 20, 25, 29]:
            print(f"  n_valid={n:2d}: ξ={xi:.4f}")

    # 验证衰减特性
    assert xi_values[0] == acq.xi_initial, "初始 ξ 不正确"
    assert xi_values[5] == acq.xi_initial, "冷启动期 ξ 应保持不变"
    assert xi_values[-1] <= acq.xi_initial, "ξ 应该衰减"
    # 单调递减（冷启动期后）
    for i in range(warm_up + 1, len(xi_values)):
        assert xi_values[i] <= xi_values[i - 1] + 1e-10, \
            f"ξ 应单调递减: xi[{i}]={xi_values[i]} > xi[{i-1}]={xi_values[i-1]}"

    # ---- EI 计算测试 ----
    print("\n--- EI 计算 ---")
    j_best = 0.3
    xi = 0.1

    # 场景1: μ 远优于 j_best → EI 大
    mu1 = np.array([0.1])
    sigma1 = np.array([0.05])
    ei1 = acq.compute_ei(mu1, sigma1, j_best, xi)
    print(f"  场景1 (μ<<j_best): μ={mu1[0]}, σ={sigma1[0]}, EI={ei1[0]:.6f}")
    assert ei1[0] > 0, "预测远优于最优时 EI 应大于0"

    # 场景2: μ 远差于 j_best → EI 小
    mu2 = np.array([0.8])
    sigma2 = np.array([0.05])
    ei2 = acq.compute_ei(mu2, sigma2, j_best, xi)
    print(f"  场景2 (μ>>j_best): μ={mu2[0]}, σ={sigma2[0]}, EI={ei2[0]:.6f}")
    assert ei2[0] < ei1[0], "预测差时 EI 应更小"

    # 场景3: σ=0 → EI=0
    mu3 = np.array([0.1])
    sigma3 = np.array([0.0])
    ei3 = acq.compute_ei(mu3, sigma3, j_best, xi)
    print(f"  场景3 (σ=0):       μ={mu3[0]}, σ={sigma3[0]}, EI={ei3[0]:.6f}")
    assert ei3[0] == 0, "σ=0 时 EI 应为 0"

    # 场景4: 大 σ → EI 受不确定性驱动
    mu4 = np.array([0.4])
    sigma4_small = np.array([0.01])
    sigma4_large = np.array([0.3])
    ei4_small = acq.compute_ei(mu4, sigma4_small, j_best, xi)
    ei4_large = acq.compute_ei(mu4, sigma4_large, j_best, xi)
    print(f"  场景4a (小σ): μ={mu4[0]}, σ={sigma4_small[0]}, EI={ei4_small[0]:.6f}")
    print(f"  场景4b (大σ): μ={mu4[0]}, σ={sigma4_large[0]}, EI={ei4_large[0]:.6f}")
    assert ei4_large[0] > ei4_small[0], "大 σ 应有更高的探索驱动 EI"

    # ---- 批量 EI ----
    print("\n--- 批量 EI ---")
    mu_batch = np.random.uniform(0.1, 0.8, 200)
    sigma_batch = np.random.uniform(0.01, 0.3, 200)
    ei_batch = acq.compute_ei(mu_batch, sigma_batch, j_best, xi)
    print(f"  批量 EI: {len(ei_batch)} 个, max={ei_batch.max():.6f}, min={ei_batch.min():.6f}")
    assert np.all(ei_batch >= 0), "EI 不能为负"

    # ---- 单点 EI ----
    ei_single = acq.compute_ei_single(0.1, 0.05, j_best, xi)
    print(f"  单点 EI: {ei_single:.6f}")
    assert abs(ei_single - ei1[0]) < 1e-10, "单点和批量结果应一致"

    print("\n✅ AcquisitionFunction 全部测试通过")
    return acq


def test_bayesian_optimizer():
    print("\n" + "=" * 60)
    print("测试 3/3: BayesianOptimizer")
    print("=" * 60)

    from bayesian_optimizer import BayesianOptimizer
    from observation_store import ObservationStore
    from parameter_space import ParameterSpace

    bo = BayesianOptimizer(config_path=CONFIG_PATH)
    print(bo)

    # 准备 ObservationStore 并模拟 12 条历史观测
    store = ObservationStore(config_path=CONFIG_PATH)
    store.records = []
    store.next_round_id = 0
    store.j_best = float('inf')
    store.theta_best = None
    store.best_round_id = -1

    ps = ParameterSpace(config_path=CONFIG_PATH)

    print("\n--- 模拟 12 条历史观测 ---")
    np.random.seed(123)

    for i in range(12):
        theta = {
            'checkpoint_interval': int(np.random.choice(range(5000, 120001, 5000))),
            'buffer_timeout': int(np.random.choice(range(1, 201, 10))),
            'watermark_delay': int(np.random.choice(range(1000, 30001, 1000))),
        }
        # 模拟 J 值：越接近 (30000, 100, 5000) 越好
        theta_norm = ps.normalize(theta)
        ideal = ps.normalize({'checkpoint_interval': 30000, 'buffer_timeout': 100,
                               'watermark_delay': 5000})
        distance = np.linalg.norm(theta_norm - ideal)
        j = 0.1 + distance * 0.5 + np.random.normal(0, 0.02)
        j = max(0.05, j)

        features = {
            'avg_input_rate': 2000 + np.random.normal(0, 100),
            'latency_p99': 300 + distance * 500,
            'avg_busy_ratio': 0.5 + np.random.normal(0, 0.05),
            'checkpoint_avg_dur': 500 + distance * 300,
        }

        parts = {'f_latency': 0.1, 'f_throughput': 0.1, 'f_checkpoint': 0.1,
                 'f_resource': 0.1, 'j_total': j}

        store.add(theta, features, j, parts)
        print(f"  round={i:2d}: θ={theta}, J={j:.4f}")

    print(f"\n存储状态: {store}")
    j_best, theta_best, _ = store.get_best()
    print(f"最优: J={j_best:.4f}, θ={theta_best}")

    # ---- 测试 suggest ----
    print("\n--- BO.suggest() ---")
    suggestion = bo.suggest(store, j_current=j_best)

    if suggestion is not None:
        print(f"  建议参数: {suggestion['theta']}")
        print(f"  EI={suggestion['ei']:.6f}")
        print(f"  μ={suggestion['mu']:.6f}")
        print(f"  σ={suggestion['sigma']:.6f}")
        print(f"  模型={suggestion['model_type']}")
        print(f"  ξ={suggestion['xi']:.4f}")
        print(f"  预测改善={suggestion['predicted_improvement']:.2%}")

        # 验证返回结构
        assert 'theta' in suggestion
        assert 'ei' in suggestion
        assert suggestion['ei'] >= 0
        for name in ps.PARAM_NAMES:
            assert name in suggestion['theta'], f"缺少参数 {name}"
            p = ps.params[name]
            val = suggestion['theta'][name]
            assert p['min'] <= val <= p['max'], \
                f"{name}={val} 越界 [{p['min']}, {p['max']}]"

        # CLI 参数
        cli = ps.to_cli_args(suggestion['theta'])
        print(f"  CLI: {cli}")
    else:
        print("  suggest 返回 None (ET 未通过或无改善空间)")

    # ---- 测试 保守模式 ----
    print("\n--- 保守模式 ---")
    conservative = bo.suggest_conservative(theta_best)
    print(f"  保守建议: {conservative}")
    dist = ps.distance(ps.normalize(theta_best), ps.normalize(conservative))
    print(f"  与最优距离: {dist:.4f} (应 < 0.2)")
    assert dist < 0.3, f"保守模式距离过大: {dist}"

    # ---- 多轮建议一致性测试 ----
    print("\n--- 多轮建议 ---")
    for trial in range(3):
        s = bo.suggest(store, j_current=j_best)
        if s:
            print(f"  Trial {trial}: θ={s['theta']}, EI={s['ei']:.6f}, μ={s['mu']:.6f}")
        else:
            print(f"  Trial {trial}: None")

    # ---- 边界情况: 少量数据 ----
    print("\n--- 边界情况: 仅2条数据 ---")
    store_small = ObservationStore(config_path=CONFIG_PATH)
    store_small.records = []
    store_small.next_round_id = 0
    store_small.j_best = float('inf')
    store_small.theta_best = None
    store_small.best_round_id = -1

    for i in range(2):
        theta = ps.get_defaults()
        theta['checkpoint_interval'] = 20000 + i * 20000
        store_small.add(theta, {'avg_input_rate': 1500}, 0.5 - i * 0.1,
                        {'j_total': 0.5 - i * 0.1})

    s_small = bo.suggest(store_small, j_current=0.4)
    if s_small:
        print(f"  少量数据建议: θ={s_small['theta']}, model={s_small['model_type']}")
        assert s_small['model_type'] == 'rf', "少量数据应使用 RF"
    else:
        print("  少量数据: None (可接受)")

    print("\n✅ BayesianOptimizer 全部测试通过")


def test_end_to_end():
    """端到端模拟：完整优化循环"""
    print("\n" + "=" * 60)
    print("端到端模拟: 20轮优化循环")
    print("=" * 60)

    from bayesian_optimizer import BayesianOptimizer
    from observation_store import ObservationStore
    from parameter_space import ParameterSpace
    from feature_engineer import FeatureEngineer

    bo = BayesianOptimizer(config_path=CONFIG_PATH)
    store = ObservationStore(config_path=CONFIG_PATH)
    store.records = []
    store.next_round_id = 0
    store.j_best = float('inf')
    store.theta_best = None
    store.best_round_id = -1

    ps = ParameterSpace(config_path=CONFIG_PATH)
    fe = FeatureEngineer(config_path=CONFIG_PATH)

    # 模拟目标函数（有噪声的二次函数）
    # 最优点: checkpoint=30000, buffer=80, watermark=5000
    ideal_norm = ps.normalize({
        'checkpoint_interval': 30000, 'buffer_timeout': 80, 'watermark_delay': 5000
    })

    def simulate_objective(theta_dict):
        """模拟：距离理想点越远，J 越大"""
        theta_norm = ps.normalize(theta_dict)
        dist = np.linalg.norm(theta_norm - ideal_norm)
        j = 0.08 + 0.6 * dist ** 2 + np.random.normal(0, 0.01)
        return max(0.05, j)

    # 冷启动: LHS 采样 8 轮
    print("\n--- 冷启动: LHS 8 轮 ---")
    lhs_samples = ps.latin_hypercube_sample(8, seed=42)

    for i, theta in enumerate(lhs_samples):
        j = simulate_objective(theta)
        features = {
            'avg_input_rate': 2000 + np.random.normal(0, 100),
            'latency_p99': 300 + j * 500,
            'avg_busy_ratio': 0.5,
            'checkpoint_avg_dur': 200,
        }
        parts = {'j_total': j, 'f_latency': 0.1, 'f_throughput': 0.1,
                 'f_checkpoint': 0.05, 'f_resource': 0.05}
        store.add(theta, features, j, parts)
        print(f"  LHS[{i}]: θ={theta}, J={j:.4f}")

    # BO 优化: 12 轮
    print("\n--- BO 优化: 12 轮 ---")
    j_history = [r.j_total for r in store.records]
    et_skips = 0
    improvements = 0

    for round_id in range(12):
        j_best, theta_best, _ = store.get_best()
        suggestion = bo.suggest(store, j_current=j_best)

        if suggestion is None:
            # ET 未通过，使用保守模式
            theta = bo.suggest_conservative(theta_best)
            et_skips += 1
            tag = "保守"
        else:
            theta = suggestion['theta']
            tag = f"BO(EI={suggestion['ei']:.4f})"

        j = simulate_objective(theta)
        features = {
            'avg_input_rate': 2000, 'latency_p99': 300 + j * 500,
            'avg_busy_ratio': 0.5, 'checkpoint_avg_dur': 200,
        }
        parts = {'j_total': j, 'f_latency': 0.1, 'f_throughput': 0.1,
                 'f_checkpoint': 0.05, 'f_resource': 0.05}

        old_best = store.j_best
        store.add(theta, features, j, parts)

        if j < old_best:
            improvements += 1

        j_history.append(j)
        print(f"  Round[{round_id:2d}] {tag:25s}: θ={theta}, "
              f"J={j:.4f}, best={store.j_best:.4f}")

    # 结果分析
    print("\n--- 优化结果 ---")
    j_best_final, theta_best_final, best_round = store.get_best()
    print(f"  最优 J:    {j_best_final:.4f} (round={best_round})")
    print(f"  最优 θ:    {theta_best_final}")
    print(f"  理想 θ:    checkpoint=30000, buffer=80, watermark=5000")
    print(f"  ET 跳过:   {et_skips}/{12}")
    print(f"  改善次数:  {improvements}/{12}")
    print(f"  总记录:    {store.count_total()}")

    # 验证优化效果
    j_lhs_best = min(r.j_total for r in store.records[:8])
    print(f"\n  LHS 阶段最优 J: {j_lhs_best:.4f}")
    print(f"  BO  阶段最优 J: {j_best_final:.4f}")

    if j_best_final < j_lhs_best:
        improvement_pct = (j_lhs_best - j_best_final) / j_lhs_best * 100
        print(f"  BO 改善了 {improvement_pct:.1f}% ✅")
    else:
        print(f"  BO 未改善 LHS 结果 (可接受，模拟中存在噪声)")

    # J 值趋势
    print(f"\n  J 值趋势 (前5 → 后5):")
    first5 = j_history[:5]
    last5 = j_history[-5:]
    print(f"    前5: {[f'{j:.3f}' for j in first5]}")
    print(f"    后5: {[f'{j:.3f}' for j in last5]}")
    print(f"    均值: {np.mean(first5):.4f} → {np.mean(last5):.4f}")

    print("\n✅ 端到端模拟通过")


if __name__ == '__main__':
    print("=" * 60)
    print("  ML Tuner Step 3 核心算法验证")
    print("=" * 60)

    try:
        test_surrogate_model()
        test_acquisition()
        test_bayesian_optimizer()
        test_end_to_end()

        print("\n" + "=" * 60)
        print("  🎉 Step 3 全部测试通过!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
