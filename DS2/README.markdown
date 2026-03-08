# DS2 弹性调度算法设计与实现文档

---

## 1. 设计背景与核心目标

在地震数据流处理场景中，由于上游传感器数据写入速率可能发生突变，或流处理系统资源分配不合理，极易导致 RocketMQ 消息积压与端到端延迟飙升。

本设计基于 OSDI '18 论文 *"Three Steps is All You Need: Fast, Accurate, Automatic Scaling Decisions for Distributed Streaming Dataflows"*，实现了一个针对 Flink 1.17 的自动化弹性调度控制器。

**核心目标：**

摒弃传统基于 CPU/内存利用率的滞后预测模型，直接利用 Flink 算子当前的**真实处理能力**（数据驱动），实时且精确地推算并调整各算子的最佳并行度，从而在无积压的前提下最大化资源利用率。

---

## 2. 核心算法理论模型

DS2 算法的决策完全依赖于对流处理拓扑图的数学建模。模型包含三个核心指标，通过 Flink REST API 采集。

### 2.1 基础测量指标

| 指标 | 符号 | 含义 | 数据来源 |
|------|------|------|----------|
| 输入速率 | `in_rate` | 算子每秒接收的记录数 | `numRecordsIn` 差分 |
| 输出速率 | `out_rate` | 算子每秒发出的记录数 | `numRecordsOut` 差分 |
| 忙碌度 | `busy_ratio` | 算子处于处理状态的时间占比（0~1） | `accumulateBusyTimeMs` 差分 |

### 2.2 推导指标（核心公式）

根据上述基础指标，推导出两个关键模型量：

**真实处理能力（True Processing Rate, TPR）**

假设算子 100% 满负荷运行时的处理速率：

```
TPR = in_rate / busy_ratio
```

> 示例：算子当前 in_rate = 1000 records/s，busy_ratio = 0.4（40%忙碌），
> 则 TPR = 1000 / 0.4 = 2500 records/s，意味着满负荷可处理 2500 条/秒。

**选择率（Selectivity）**

算子对数据的过滤或膨胀比例：

```
Selectivity = out_rate / in_rate
```

| 算子 | 典型选择率 | 说明 |
|------|-----------|------|
| HardwareFilterFunction | 0.90 ~ 1.0 | 过滤少量硬件异常数据 |
| SignalExtractionFunction | 1.0 | 一条输入对应一条输出 |
| FeatureExtractionFunction | 1.0 | 一条输入对应一条输出 |
| GridAggregationFunction | ~0.05 | 20 个节点事件聚合为 1 个网格摘要 |

### 2.3 目标并行度计算公式

按拓扑序（Source → Sink），利用上游传递的目标流量，计算当前算子所需的最优并行度：

```
CapPerInstance    = TPR / CurrentParallelism
TargetParallelism = ceil( TargetInputRate / CapPerInstance × SafetyMargin )
```

**完整数值示例：**

```
FeatureExtractionFunction 当前状态：
  CurrentParallelism = 4
  in_rate            = 1000 records/s
  busy_ratio         = 0.80

推导：
  TPR             = 1000 / 0.80 = 1250 records/s
  CapPerInstance  = 1250 / 4    = 312.5 records/s/实例

假设上游目标输出 = 3000 records/s，SafetyMargin = 1.5：
  TargetParallelism = ceil(3000 / 312.5 × 1.5) = ceil(14.4) = 15

结论：需要从 parallelism=4 扩容到 parallelism=15
```

---

## 3. 模块化架构设计

系统采用 Python 独立进程实现，通过 REST API 与 Flink JobManager 交互。

### 3.1 模块组成

```
ds2_controller.py
│
├── Config              配置管理（从 ds2_config.yaml 加载）
├── FlinkClient         Flink REST API 交互客户端
├── MetricsCollector    指标采集与差分计算
├── DS2Model            核心算法决策逻辑
├── ScalingExecutor     扩缩容执行器
└── DS2Controller       主控制循环
```

### 3.2 模块交互流程

```
Flink REST API
      │
      │ numRecordsIn / numRecordsOut / accumulateBusyTimeMs
      ▼
MetricsCollector（差分计算）
      │
      │ in_rate / out_rate / busy_ratio
      │ true_processing_rate / selectivity
      ▼
DS2Model.compute_plan()（拓扑排序 + 公式计算）
      │
      │ ScalingDecision { vertex_id, current_p, target_p }
      ▼
DS2Model.should_apply()（Activation Window 稳定性检查）
      │
      │ True / False
      ▼
ScalingExecutor.execute()
      │
      ├── trigger_savepoint()  → hdfs://node01:9000/flink/savepoints/
      ├── cancel_job()
      └── run_job(new_parallelism, savepoint_path)
```

### 3.3 各模块详细说明

#### 3.3.1 Config（配置管理模块）

统一管理 Flink 集群信息与 DS2 策略参数，支持从 YAML 文件加载：

```yaml
flink:
  rest_url: "http://192.168.56.151:8081"
  job_name: "Seismic-Cache-Optimized-Job"
  savepoint_dir: "hdfs://node01:9000/flink/savepoints"

ds2:
  policy_interval: 15
  warm_up_time: 30
  activation_window: 3
  target_rate_ratio: 1.5
  min_busy_ratio: 0.05
  max_parallelism: 16
  min_parallelism: 1
  change_threshold: 0
  target_jar_name: "flink-rocketmq-demo-1.0-SNAPSHOT"
```

| 参数 | 默认值 | 建议范围 | 说明 |
|------|--------|---------|------|
| policy_interval | 15s | 10~30s | 过小导致决策抖动，过大响应慢 |
| activation_window | 3 | 2~5 | 越大越稳定，但响应越慢 |
| target_rate_ratio | 1.5 | 1.1~1.5 | 越大越保守，资源利用率越低 |
| warm_up_time | 30s | 30~60s | 需大于 Flink 作业冷启动时间 |
| change_threshold | 0 | 0~2 | 设为 1 可过滤 ±1 的微小抖动 |

#### 3.3.2 FlinkClient（API 交互模块）

封装所有对 Flink REST API 的 HTTP 请求：

| 方法 | API 端点 | 功能 |
|------|---------|------|
| `get_running_job_id()` | `/jobs/overview` | 查找正在运行的目标作业 |
| `get_job_topology()` | `/jobs/{job_id}` | 获取作业 DAG 拓扑（算子+边） |
| `get_vertex_metrics()` | `/jobs/{job_id}/vertices/{vid}/subtasks/metrics` | 获取算子累积指标 |
| `trigger_savepoint()` | `/jobs/{job_id}/savepoints` | 触发状态快照 |
| `cancel_job()` | `/jobs/{job_id}?mode=cancel` | 停止作业 |
| `get_jar_id()` | `/jars` | 查找已上传的 JAR 包 |
| `run_job()` | `/jars/{jar_id}/run` | 以新并行度从 Savepoint 恢复提交 |

**API 数据格式兼容处理：**

Flink 1.17 在不同并行度下返回的 Metrics 结构不同：

```
并行度 > 1（格式A）：
[
  {"subtask": 0, "metrics": [{"id": "numRecordsIn", "value": "500"}]},
  {"subtask": 1, "metrics": [{"id": "numRecordsIn", "value": "600"}]}
]

并行度 = 1 或聚合模式（格式B）：
[
  {"id": "numRecordsIn", "min": 500, "max": 600, "sum": 1100, "avg": 550}
]
```

`FlinkClient` 内部自动探测格式，统一提取 `sum` 值。

#### 3.3.3 MetricsCollector（指标采集与差分计算模块）

**设计核心：累积量差分法**

彻底弃用 Flink 不稳定的 `*PerSecond` 瞬时指标，改用累积总量差分计算：

```
上一次采集：numRecordsIn = 15000，时间 = t1
本次采集：  numRecordsIn = 30000，时间 = t2

in_rate = (30000 - 15000) / (t2 - t1) = 1000 records/s
```

**忙碌度差分计算：**

```
上一次采集：avg_busy_ms = 3000ms，时间 = t1
本次采集：  avg_busy_ms = 9000ms，时间 = t2
采集间隔：  duration = t2 - t1 = 15s

busy_delta = 9000 - 3000 = 6000ms
busy_ratio = 6000 / (15 × 1000) = 0.40（40% 忙碌）
```

**缓存结构：**

```python
# 每个算子维护一条历史记录
self.last_metrics[vertex_id] = (timestamp, total_in, total_out, avg_busy_ms)
```

#### 3.3.4 DS2Model（调度决策模块）

**拓扑排序 + 流量传递：**

```
Source(p=4)
  │ target_output = 2000/s
  ▼
HardwareFilter(p=4)      selectivity = 0.95
  │ target_output = 2000 × 0.95 = 1900/s
  ▼
SignalExtraction(p=4)     selectivity = 1.0
  │ target_output = 1900/s
  ▼
FeatureExtraction(p=4)    selectivity = 1.0
  │ target_output = 1900/s
  ▼
GridAggregation(p=4)      selectivity = 0.05
  │ target_output = 1900 × 0.05 = 95/s
  ▼
Sink
```

**拓扑排序必要性：**

下游的目标输入依赖于上游的目标输出，必须按 Source → Sink 方向依次计算。代码使用入度 BFS 算法实现拓扑排序：

```python
def topological_sort(self) -> List[str]:
    in_degree = defaultdict(int)
    for _, target in self.edges:
        in_degree[target] += 1

    queue = [v for v in self.vertices if in_degree[v] == 0]
    result = []

    while queue:
        node = queue.pop(0)
        result.append(node)
        for downstream in self.get_downstream(node):
            in_degree[downstream] -= 1
            if in_degree[downstream] == 0:
                queue.append(downstream)

    return result
```

**Activation Window 机制：**

为防止指标抖动导致频繁扩缩容，模型维护一个决策历史队列：

```
第 15s：决策 p=8，窗口 = [{p:8}]             → 等待 (1/3)
第 30s：决策 p=8，窗口 = [{p:8},{p:8}]       → 等待 (2/3)
第 45s：决策 p=9，窗口 = [{p:8},{p:8},{p:9}] → 不一致，继续等待
第 60s：决策 p=9，窗口 = [{p:8},{p:9},{p:9}] → 不一致，继续等待
第 75s：决策 p=9，窗口 = [{p:9},{p:9},{p:9}] → 3 次一致，执行扩容！
```

对应代码：

```python
def should_apply(self, decisions):
    current_snapshot = {vid: d.target_parallelism for vid, d in decisions.items()}
    self.decision_history.append(current_snapshot)

    if len(self.decision_history) < self.config.activation_window:
        return False, f"Stabilizing... ({len(self.decision_history)}/{self.config.activation_window})"

    first = self.decision_history[0]
    is_stable = all(snap == first for snap in self.decision_history)

    if is_stable:
        return True, "Decision stable, APPLYING scaling"
    else:
        return False, "Decisions fluctuating, waiting..."
```

#### 3.3.5 ScalingExecutor（扩缩容执行模块）

执行 Flink 作业的无损平滑扩缩容，分四步完成：

```
Step 1: trigger_savepoint()
        → 将 L2/L3 缓存状态写入 HDFS
        → 等待异步完成（最长 120 秒）

Step 2: cancel_job()
        → 停止当前作业
        → 轮询等待作业状态变为 CANCELED

Step 3: run_job(new_parallelism, savepoint_path)
        → 以新并行度从 Savepoint 恢复提交
        → L2/L3 缓存状态自动恢复

Step 4: 进入 warm_up_time 冷却期
        → 等待系统在新并行度下稳定
        → 冷却期内不做任何决策
```

对应代码：

```python
def execute(self, job_id, decisions, dry_run):
    new_max_parallelism = max(d.target_parallelism for d in decisions.values())

    if dry_run:
        return True

    # 1. 找到 JAR 包
    jar_id = self.client.get_jar_id(self.config.target_jar_name)

    # 2. 触发 Savepoint
    savepoint_path = self.client.trigger_savepoint(job_id)

    # 3. 停止作业
    self.client.cancel_job(job_id)
    self.client.wait_for_job_cancel(job_id)

    # 4. 以新并行度从 Savepoint 恢复
    success = self.client.run_job(
        jar_id=jar_id,
        parallelism=new_max_parallelism,
        savepoint_path=savepoint_path
    )

    if success:
        self.last_scale_time = time.time()
    return success
```

---

## 4. 关键执行流程（Three Steps）

控制器主体 `DS2Controller` 运行在持续循环中：

```
┌───────────────────────────────────────────────────────────────┐
│                  DS2 决策循环（每 15 秒一轮）                   │
│                                                                 │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  Step 1: Measure（测量）                               │     │
│  │                                                        │     │
│  │  Collector 遍历拓扑中每个算子                           │     │
│  │  → 拉取 numRecordsIn / numRecordsOut /                 │     │
│  │    accumulateBusyTimeMs 累积指标                        │     │
│  │  → 差分计算 in_rate / out_rate / busy_ratio            │     │
│  │  → 推导 true_processing_rate / selectivity             │     │
│  └───────────────────────────────────────────────────────┘     │
│                             ↓                                   │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  Step 2: Plan（规划）                                  │     │
│  │                                                        │     │
│  │  按拓扑排序（Source → Sink）依次处理每个算子：           │     │
│  │  ① target_input = 上游目标输出之和                     │     │
│  │  ② cap_per_inst = TPR / current_parallelism           │     │
│  │  ③ target_p = ceil(target_input / cap × margin)       │     │
│  │  ④ 传递 target_output = target_input × selectivity    │     │
│  └───────────────────────────────────────────────────────┘     │
│                             ↓                                   │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  Step 3: Execute（执行）                               │     │
│  │                                                        │     │
│  │  Activation Window 检查：                               │     │
│  │  → 连续 3 次决策一致？                                  │     │
│  │     否 → 记录历史，等待下一轮                           │     │
│  │     是 → Savepoint → Cancel → Run（新并行度）          │     │
│  │         → 进入 warm_up 冷却期                          │     │
│  └───────────────────────────────────────────────────────┘     │
│                                                                 │
└───────────────────────────────────────────────────────────────┘
```

对应代码：

```python
def run(self):
    while self.running:
        loop_start = time.time()

        job_id = self.client.get_running_job_id()

        if not job_id:
            self.logger.warning("Job not running.")

        elif self.executor.is_warming_up():
            self.logger.info(f"Cooling down... {self.executor.get_warmup_remaining():.0f}s")

        else:
            topo = self.client.get_job_topology(job_id)
            if topo:
                # Step 1: Measure
                topo = self.collector.collect_all(topo)

                # Step 2: Plan
                decisions = self.model.compute_plan(topo)

                # Step 3: Execute
                should_apply, reason = self.model.should_apply(decisions)
                if should_apply:
                    self.executor.execute(job_id, decisions, self.dry_run)
                    self.model.reset_history()

        elapsed = time.time() - loop_start
        sleep_time = max(1, self.config.policy_interval - elapsed)
        time.sleep(sleep_time)
```

---

## 5. 一次完整扩缩容时间线

```
T = 0s      DS2 启动，第一次采集，存入基准缓存（无速率数据）
              → 无法决策，跳过

T = 15s     第二次采集，执行差分，获得第一批有效速率
              → compute_plan() 计算目标并行度
              → 假设 FeatureExtraction 需从 p=4 扩到 p=8
              → decision_history = [{p:8}]，窗口 1/3，等待

T = 30s     第三次采集，再次决策 p=8
              → decision_history = [{p:8},{p:8}]，窗口 2/3，等待

T = 45s     第四次采集，再次决策 p=8，三次一致
              → should_apply() 返回 True
              → 开始执行扩缩容：

              T+45s   触发 Savepoint（约 30s）
              T+75s   Savepoint 完成，路径写入 HDFS
              T+75s   Cancel 当前作业（约 5s）
              T+80s   作业停止确认
              T+80s   以 p=8 从 Savepoint 重新提交
              T+90s   新作业启动完成

T = 90s     进入 warm_up_time 冷却期（30s）
              → 冷却期内不做任何决策
              → L2/L3 缓存已从 Savepoint 恢复

T = 120s    冷却结束，DS2 恢复正常监控循环
```

**关键结论：从负载变化到完成扩缩容，总耗时约 75~120 秒。**

---

## 6. 针对本场景的核心工程适配

在将学术论文落地到 Flink 1.17 环境时，本设计做了以下关键适配：

### 6.1 API 数据格式双向兼容

Flink 1.17 在不同并行度下返回的 Metrics JSON 结构不同（Subtask 数组 vs 聚合型 JSON）。`FlinkClient` 内部增加了解析探测逻辑，统一提取 `sum` 值，避免差分计算时速率被异常放大。

### 6.2 繁忙度跨并行度修正

在执行扩缩容后，算子的 Subtask 数量发生改变。`MetricsCollector` 中强制存储**全局总忙碌毫秒数**而非**平均忙碌毫秒数**，确保扩容前后差分时间窗口内的计算不失真。

### 6.3 Source 算子自适应拉伸

原版论文往往假定 Source 速率被动接受。本设计加入针对 Source 的独立评估逻辑：若 Source 的 `busy_ratio > 0.8`，说明摄入端已达到瓶颈，主动调大其并行度以加快 RocketMQ 积压消费。

### 6.4 HDFS 超时容错机制

由于大规模状态数据的 Savepoint 写入 HDFS 需要较长时间，将异步操作的轮询等待阈值从标准的 60 秒延长至 120 秒，保障快照的完整性。

---

## 7. DS2 与三级缓存的协同

DS2 扩缩容与缓存系统的交互是本设计的核心价值之一。

### 7.1 无 DS2 时的退化场景

```
负载增加 → 积压增长 → 延迟增加
       → L2 缓存中的历史数据因超时被淘汰
       → 缓存命中率持续下降
       → 异常检测准确率降低
       → 系统进入恶性循环
```

### 7.2 有 DS2 时的保障机制

```
负载增加 → DS2 检测到 busy_ratio 上升
       → 计算目标并行度
       → Savepoint 保存 L2/L3 缓存状态至 HDFS
       → 新并行度启动后从 Savepoint 恢复缓存
       → 缓存命中率无损恢复
       → 系统在新并行度下稳定运行
```

### 7.3 各级缓存在扩缩容中的行为

| 缓存级别 | 存储位置 | 扩缩容行为 | 恢复方式 |
|---------|---------|-----------|---------|
| L1 缓存 | JVM 堆内存 | ❌ 丢失 | 重启后立即重建，影响极小 |
| L2 缓存 | Flink MapState | ✅ 保留 | 通过 Savepoint 恢复 |
| L3 缓存 | Flink MapState | ✅ 保留 | 通过 Savepoint 恢复 |

---

## 8. 控制台输出说明

### 8.1 正常运行输出

```
09:15:00 | DS2.Controller  | INFO  | DS2 Controller Started (DryRun=True)
09:15:00 | DS2.FlinkClient | INFO  | Connected to Flink 1.17.2

------------------------------------------------------------
Current Status:
[Source: RocketMQ]       p=4 | in= 2000/s | busy=38.2% 🟢
[HardwareFilter]         p=4 | in= 1998/s | busy=41.5% 🟢
[SignalExtraction]       p=4 | in= 1976/s | busy=79.3% 🟡
[FeatureExtraction]      p=4 | in= 1976/s | busy=82.1% 🔴
[GridAggregation]        p=4 | in= 1976/s | busy=15.3% 🟢
------------------------------------------------------------
```

### 8.2 触发扩缩容输出

```
09:15:15 | DS2.Controller | INFO | Decision: Stabilizing... (1/3)
09:15:30 | DS2.Controller | INFO | Decision: Stabilizing... (2/3)
09:15:45 | DS2.Controller | INFO | Decision: Decision stable, APPLYING

============================================================
 >>> EXECUTING SCALING PLAN (Target Global P=8) <<<
  FeatureExtraction: 4 -> 8 ↑
  SignalExtraction:  4 -> 6 ↑
  HardwareFilter:    4 -> 4
============================================================
```

### 8.3 状态图标含义

| 图标 | 忙碌度范围 | 说明 |
|------|----------|------|
| 🟢 | < 50% | 资源充足 |
| 🟡 | 50% ~ 80% | 负载适中 |
| 🔴 | > 80% | 接近瓶颈，可能触发扩容 |

---

## 9. 使用方式

### 9.1 环境准备

```bash
# 安装 Python 依赖
pip install requests pyyaml

# 确认 Flink 作业正在运行
curl http://192.168.56.151:8081/jobs/overview

# 确认 JAR 包已上传到 Flink
curl http://192.168.56.151:8081/jars
```

### 9.2 启动命令

```bash
# 干跑模式（推荐先用此模式验证决策合理性）
python ds2_controller.py --dry-run -v

# 正常模式（实际执行扩缩容）
python ds2_controller.py

# 指定配置文件
python ds2_controller.py -c /path/to/ds2_config.yaml --dry-run -v
```

### 9.3 停止方式

```bash
# Ctrl+C 优雅停止（已注册 SIGINT 信号处理）
```

---

## 10. 已知限制

| 限制 | 说明 | 影响 |
|------|------|------|
| 线性假设 | 假设 busy_ratio 与处理能力成线性关系 | IO 密集型算子可能不准确 |
| 重启开销 | 每次扩缩容需 Savepoint + 重启，约 75~120s | 扩缩容期间服务短暂不可用 |
| 全局并行度 | 当前实现以最大值统一设置全局并行度 | 轻量算子资源轻微浪费 |
| 分区限制 | Source 并行度不能超过 RocketMQ Topic 分区数 | 限制 Source 最大扩容能力 |
| 冷启动延迟 | 首次有效决策需等待 2 个 policy_interval | DS2 启动后约 30s 内无决策 |

---

## 11. 文件结构

```
ds2/
├── ds2_controller.py       # DS2 控制器主程序（Python）
└── ds2_config.yaml         # 配置文件

---

**✨系统提示：**

**检测到当前聊天的对话轮数较多，提示您注意适时创建新聊天。**

（只是一个小提醒。本提醒不影响模型表现）

> 此为 ChatGPT 网页前端自身渲染特性所致。对话过长可能导致浏览器卡顿、响应变慢，从而影响交互使用体验。

---

