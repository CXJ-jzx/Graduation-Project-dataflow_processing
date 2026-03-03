# Graduation-Project-dataflow_processing
毕设终版
# 面向地震勘探场景的大数据流处理平台 —— 全流程设计方案

---

## 一、整体流程概览

```
.seis文件 → Producer(坐标硬编码) → RocketMQ(Topic: seismic-raw)
    → Flink Source → 元数据解析 → 硬件指标过滤
    → 波形拆分(100ms窗口) → 信号检测(L1) → 特征提取(L2)
    → 网格聚合(L3) → Sink → RocketMQ(Topic: seismic-events)
```

---

## 二、缓存层设计

### 2.1 L1缓存 —— 节点信号状态缓存

**存储位置**：Flink KeyedState（HeapStateBackend，按 nodeid 键控，纯内存）

**缓存内容**：

```
NodeSignalState {
  double  prev_window_rms;        // 上一个100ms窗口的RMS能量值
  boolean is_event_active;        // 当前节点是否处于地震事件中
  long    event_start_ms;         // 本次事件起始时刻（ms精度）
  int     event_peak_amplitude;   // 本次事件当前峰值
  int     continuous_noise_cnt;   // 连续静默窗口计数
}
```

**淘汰策略**：不主动淘汰，使用**超时状态重置**机制。若某节点超过 `T=5s` 无新数据到达，通过 Flink `onTimer()` 定时器将该节点 L1 状态重置为初始值，防止节点离线后状态永久停留在 `is_event_active=true`。

**命中率定义**：信号检测算子访问 L1 时，能取到有效的上一窗口状态（非空、非过期）的比例。正常运行时应接近 100%，仅节点冷启动的第一个窗口会出现缺失。

---

### 2.2 L2缓存 —— 单节点多秒特征聚合缓存

**存储位置**：Flink KeyedState（按 nodeid 键控）

**缓存内容**：

```
NodeEventHistory {
  List<EventFeature> recent_events;   // 保留最近 K=5 条事件记录

  EventFeature {
    long   onset_ms;               // 起震时刻（ms精度）
    long   duration_ms;            // 持续时长
    int    peak_amplitude;         // 峰值振幅
    double rms_energy;             // 全段RMS能量
    int    signal_sample_count;    // 有效地震样本点数
  }

  // 基于 recent_events 实时维护的聚合统计量
  double avg_peak_amplitude;       // 近K次峰值均值（用于异常检测基线）
}
```

**淘汰策略**：以**节点槽位**为淘汰单位，采用 **LRU + TTL 双重机制**：

- 槽位容量上限：`CAPACITY = 1500`（覆盖约 75% 的节点）
- 写入新事件时，若总槽位数超过 CAPACITY，淘汰 LRU 队列末尾的节点槽位
- 某节点超过 `TTL = 300s` 无任何读写操作，直接淘汰其槽位（应对节点离线场景）
- 每个节点槽位内部，`recent_events` 列表最多保留 K=5 条，超出时淘汰最旧的一条（FIFO）

**命中率定义**：查询某节点历史特征时，该节点槽位在 L2 中存在的比例。节点首次触发事件时必然缺失（冷启动），从第二次事件起命中率应逐步稳定。

---

### 2.3 L3缓存 —— 空间网格区域聚合缓存

**存储位置**：JVM 进程内 HashMap，分静态与动态两部分

**缓存内容**：

```
// 静态部分（只读，系统启动时初始化，永不淘汰）
Map<Integer, String>  nodeid_to_grid;    // nodeid → grid_id 映射
Map<String, List<Integer>> grid_to_nodes; // grid_id → nodeid 列表

// 动态部分（随数据流更新）
GridSummary {
  String grid_id;
  int    active_node_count;     // 当前窗口内触发事件的节点数
  int    total_node_count;      // 该网格总节点数
  double avg_peak_amplitude;    // 网格内节点峰值振幅均值
  double trigger_rate;          // 触发率 = active_node_count / total_node_count
  long   last_update_ts;        // 最后更新时间戳
}
```

**淘汰策略**：
- 静态部分：永不淘汰，全量常驻内存（约 16KB，可忽略）
- 动态部分：**LRU(M=50) + TTL(600s) 双重机制**
  - 动态条目数超过 M=50 时，淘汰最久未访问的网格条目
  - 每隔 60s 定时扫描，清理超过 TTL=600s 未更新的网格条目

**命中率定义**：查询某 grid_id 的 GridSummary 时，缓存中存在且未过期的比例。

---

## 三、分步骤全流程设计

### Step 1：系统初始化 —— 坐标分配与网格构建

**目标**：在 Producer 启动前，为 2000 个节点分配固定虚拟坐标，并构建空间网格映射关系。

**操作**：

1. 扫描 `dataset` 目录下全部 2000 个文件夹，提取 nodeid 列表，按 nodeid 升序排列
2. 按排名分配固定坐标（spacing=100）：
   - `fixed_x = (rank % 50) * spacing`
   - `fixed_y = (rank / 50) * spacing`
3. 按 `10×10` 划分网格（共 200 个网格，每格约 10 个节点），计算每个节点所属网格：
   - `grid_col = rank % 50 / 5`
   - `grid_row = rank / 50 / 4`
4. 将映射关系持久化到本地配置文件（JSON格式），供 Producer 和 Flink 作业启动时加载：
   - 正向：`nodeid → {fixed_x, fixed_y, grid_row, grid_col}`
   - 反向：`grid_id → List<nodeid>`

---

### Step 2：Producer —— 读取 .seis 文件写入 RocketMQ

**目标**：并发读取 2000 个 .seis 文件，解析二进制格式，修正坐标后发送至 RocketMQ。

**操作**：

1. 加载 Step1 生成的坐标映射配置文件
2. 启动线程池，每个线程负责一个 .seis 文件，执行以下逻辑：
   - 跳过文件头 1024 字节
   - 循环按 3048 字节切块读取，不足 3048 字节的尾部直接丢弃
   - 解析前 48 字节元数据（**小端序**），提取各字段
   - **用映射表将元数据中随机的 x、y 字段替换为 fixed_x、fixed_y**
   - 将完整 3048 字节（修正后元数据 + 3000 字节波形）作为消息体
   - 以 nodeid 为 MessageKey 发送至 RocketMQ Topic：`seismic-raw`
3. 发送速率控制：每个节点每秒发送 1 条消息，模拟真实采集节奏

**元数据字段偏移速查表**：

| 字段 | 偏移 | 类型 | 说明 |
|------|------|------|------|
| size | 0–1 | short | 固定 3048 |
| nSamples | 2–3 | short | 固定 1000 |
| sampleRate | 4–5 | short | 固定 1000 |
| nodeid | 12–15 | int | 节点ID |
| x | 20–23 | int | **替换为 fixed_x** |
| y | 24–27 | int | **替换为 fixed_y** |
| timeStamp | 32–35 | int | 起始时间戳（秒） |
| batVoltage | 40–41 | short | 电池电压 |
| batTemp | 42–43 | short | 电池温度 |

---

### Step 3：Flink Source —— 从 RocketMQ 消费数据

**目标**：Flink 作业订阅 `seismic-raw`，将二进制消息反序列化为结构化对象。

**操作**：

1. 使用 RocketMQ Flink Connector 订阅 Topic `seismic-raw`，配置消费者组
2. 自定义反序列化器，将 3048 字节消息体解析为 `SeismicRecord` 对象：

```
SeismicRecord {
  int     nodeid
  int     fixed_x
  int     fixed_y
  long    timestamp        // 秒级时间戳
  short   batVoltage
  short   batTemp
  int[]   samples          // 1000个采样点（每点3字节有符号整数，小端序）
}
```

3. 对数据流执行 `keyBy(nodeid)`，保证同一节点的数据进入同一处理链路

---

### Step 4：硬件指标过滤算子

**目标**：基于元数据中的硬件状态字段，剔除明显异常的数据包，避免无效数据污染后续处理。

**过滤规则**：

| 字段 | 过滤条件 | 处理方式 |
|------|---------|---------|
| batVoltage | < 10 | 丢弃，输出到 SideOutput |
| batTemp | > 80 | 丢弃，输出到 SideOutput |
| timestamp | 与上一条时间差 < 0 或 > 10s | 标记可疑，输出到 SideOutput |

**操作**：

1. 通过 Flink `ProcessFunction` 实现，使用 `KeyedState` 记录每个节点上一条数据的 timestamp，用于时间差检查
2. 异常数据通过 `SideOutput` 单独输出，不阻断主流程
3. 通过过滤的合格数据继续流向 Step 5

---

### Step 5：波形拆分 —— 切分 100ms 窗口

**目标**：将每条 `SeismicRecord` 的 1000 个采样点切分为 10 个 100ms 子窗口，并计算每个窗口的 RMS 能量。

**操作**：

1. 对 1000 个采样点按每 100 个点一组切分：
   - `window[i] = samples[i*100 .. i*100+99]`，i = 0, 1, ..., 9
2. 每段构造一个 `WindowRecord`：

```
WindowRecord {
  int     nodeid
  int     fixed_x
  int     fixed_y
  int     window_index          // 0~9
  long    window_start_ms       // timestamp * 1000 + window_index * 100
  int[]   samples               // 100个采样点
  double  rms                   // sqrt(sum(s_i^2) / 100)
}
```

3. 每条 `SeismicRecord` 扁平化输出 10 条 `WindowRecord`，依次流向下游

---

### Step 6：信号检测算子（读写 L1 缓存）

**目标**：判断每个 100ms 窗口归属（噪音 or 地震信号），检测事件起止边界，输出完整地震事件。

**L1 读写逻辑**：

每个 `WindowRecord` 到达时：

1. **读 L1**：获取该 nodeid 的 `NodeSignalState`（首次访问则初始化为默认值）
2. **判断当前窗口归属**：

```
// 事件开始判断
if (current_rms / prev_window_rms > 100) AND (current_rms > 1000):
    is_event_active = true
    event_start_ms = window_start_ms
    event_peak_amplitude = max(samples)
    continuous_noise_cnt = 0

// 事件持续中
elif is_event_active AND current_rms >= 100:
    event_peak_amplitude = max(event_peak_amplitude, max(samples))
    continuous_noise_cnt = 0

// 事件可能结束（连续静默判断）
elif is_event_active AND current_rms < 100:
    continuous_noise_cnt += 1
    if continuous_noise_cnt >= 3:
        → 触发事件结束，输出 EventRecord
        → 重置 L1 状态

// 纯噪音
else:
    → 丢弃当前窗口
```

3. **更新 L1**：将 `current_rms` 写入 `prev_window_rms`，更新其余状态字段
4. **L1 超时重置**：每次收到节点数据时刷新 Flink 定时器，若超过 5s 无数据则触发 `onTimer()` 重置该节点 L1 状态

**输出**：

- 噪音窗口 → 丢弃
- 事件结束时 → 输出 `EventRecord`：

```
EventRecord {
  int     nodeid
  int     fixed_x
  int     fixed_y
  long    event_start_ms
  long    event_end_ms
  long    duration_ms
  int     peak_amplitude
  int[]   all_signal_samples   // 事件期间所有地震信号窗口的采样点合并
}
```

---

### Step 7：特征提取算子（读写 L2 缓存）

**目标**：对完整 `EventRecord` 提取关键特征，写入 L2 缓存，并基于历史数据做异常判断。

**操作**：

1. 对 `EventRecord` 计算特征：
   - `peak_amplitude`：所有信号采样点绝对值的最大值
   - `rms_energy`：全段 RMS
   - `duration_ms`：事件持续时长
   - `signal_sample_count`：有效地震采样点总数

2. **读 L2**：查询该 nodeid 的节点槽位：
   - **L2 命中**：取 `avg_peak_amplitude` 与当前 `peak_amplitude` 对比；若偏差超过 50%，标记 `is_anomaly = true`
   - **L2 缺失（冷启动）**：跳过异常检测，直接写入

3. **写 L2**：
   - 将当前 `EventFeature` 追加到 `recent_events` 列表
   - 若列表长度 > K=5，淘汰最旧的一条（FIFO）
   - 更新 `avg_peak_amplitude`（滑动均值）
   - 若总槽位数 > CAPACITY=1500，按 LRU 淘汰最久未访问的节点槽位
   - 更新该节点槽位的最近访问时间（用于 LRU 和 TTL 计算）

4. 输出 `EventFeature`：

```
EventFeature {
  int     nodeid
  int     fixed_x
  int     fixed_y
  long    onset_ms
  long    duration_ms
  int     peak_amplitude
  double  rms_energy
  int     signal_sample_count
  boolean is_anomaly
}
```

---

### Step 8：网格聚合算子（读写 L3 缓存）

**目标**：以网格为粒度，汇聚同一网格内各节点的事件特征，更新网格级统计指标。

**操作**：

1. **读 L3 静态索引**：根据 nodeid 查 `nodeid_to_grid` 映射，确认所属 `grid_id`
2. **读 L3 动态缓存**：查询对应 `grid_id` 的 `GridSummary`：
   - **L3 命中**：在现有 GridSummary 基础上更新字段：
     - `active_node_count += 1`
     - `avg_peak_amplitude` 重新计算（滑动均值）
     - `trigger_rate = active_node_count / total_node_count`
     - `last_update_ts = current_ts`
   - **L3 缺失**：新建 GridSummary 条目，从 L3 静态索引查 `total_node_count`，写入 L3
3. **L3 淘汰检查**：
   - 写入后若动态条目数 > M=50，淘汰 LRU 末尾的网格条目
   - 每隔 60s 定时扫描，清理 `last_update_ts` 超过 TTL=600s 的条目
4. 输出 `GridSummary` 对象，进入 Sink

---

### Step 9：Sink —— 结果写回 RocketMQ

**目标**：将处理结果写回 RocketMQ，输出两类结构化数据供下游消费。

**Topic 1：`seismic-node-events`（单节点事件特征）**

- 触发时机：每当 Step 7 完成一次特征提取
- 消息内容：`nodeid`、`fixed_x`、`fixed_y`、`onset_ms`、`duration_ms`、`peak_amplitude`、`rms_energy`、`is_anomaly`
- MessageKey：nodeid

**Topic 2：`seismic-grid-summary`（网格区域聚合报告）**

- 触发时机：每当 Step 8 完成一次网格统计更新
- 消息内容：`grid_id`、`active_node_count`、`total_node_count`、`trigger_rate`、`avg_peak_amplitude`、`last_update_ts`
- MessageKey：grid_id

---

## 四、数据流向全景图

```
[Step 1] 系统初始化
    坐标映射表、网格映射表（持久化到本地配置文件）
         ↓ 加载
[Step 2] Producer（并发读取2000个.seis文件）
    修正x/y坐标 → RocketMQ Topic: seismic-raw
         ↓
[Step 3] Flink Source
    反序列化 → SeismicRecord → keyBy(nodeid)
         ↓
[Step 4] 硬件指标过滤
    异常数据 → SideOutput（丢弃/记录）
    合格数据 ↓
[Step 5] 100ms 窗口拆分
    1条SeismicRecord → 10条WindowRecord（含RMS）
         ↓
[Step 6] 信号检测（读写 L1 缓存）
    噪音窗口 → 丢弃
    事件结束 → EventRecord ↓
[Step 7] 特征提取（读写 L2 缓存）
    → EventFeature（含异常标记）↓
[Step 8] 网格聚合（读写 L3 缓存）
    → GridSummary ↓
[Step 9] Sink
    → RocketMQ Topic: seismic-node-events
    → RocketMQ Topic: seismic-grid-summary
```

---

## 五、各层缓存作用总结

| 缓存层 | 容量 | 淘汰策略 | 核心作用 |
|--------|------|---------|---------|
| L1 | 固定 2000 条，~200KB | 超时重置（T=5s） | 维护节点信号的跨秒连续性，保证事件边界检测准确 |
| L2 | 最多 1500 个节点槽位 | LRU(1500) + TTL(300s) | 积累节点近期事件特征，支持异常检测与节点健康评估 |
| L3-静态 | 固定 2000 条，~16KB | 永不淘汰 | 节点坐标与网格归属的快速查询 |
| L3-动态 | 最多 50 个网格条目 | LRU(50) + TTL(600s) | 汇聚网格内节点统计量，输出区域级信号质量指标 |
