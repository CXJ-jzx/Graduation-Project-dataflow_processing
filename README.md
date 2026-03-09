# 面向地震勘探场景的大数据流处理平台
## 全流程设计方案（Protobuf 版）

---

## 一、整体架构概览

```
┌──────────────────────────────────────────────────────────────┐
│                    .seis 文件（2000个节点）                    │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│  Step 1：系统初始化                                           │
│  输出：config/node_mapping.pb  config/grid_mapping.pb        │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│  Step 2：Producer                                            │
│  读取 .seis → 解析二进制 → 修正坐标 → Protobuf 序列化         │
│  输出：RocketMQ Topic: seismic-raw                           │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│  Step 3：Flink Source + Watermark                            │
│  消费 RocketMQ → Protobuf 反序列化 → 分配事件时间 → keyBy    │
│  输出：DataStream<SeismicRecord>                             │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│  Step 4：硬件指标过滤算子                                     │
│  输出：主流（正常数据） + SideOutput（异常数据）               │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│  Step 5：地震信号提取算子                                     │
│  分析1000个采样点 → 提取地震段 → 丢弃纯噪音                   │
│  输出：DataStream<SeismicEvent>                              │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│  Step 6：特征提取与异常检测算子（L2 缓存）                    │
│  读写 L2 缓存 → 异常检测 → 特征聚合                          │
│  输出：DataStream<EventFeature>                              │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│  Step 7：网格聚合算子（L3 缓存）                              │
│  读写 L3 缓存 → 网格统计                                     │
│  输出：DataStream<GridSummary>                               │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│  Step 8：Sink                                                │
│  EventFeature  → RocketMQ: seismic-node-events               │
│  GridSummary   → RocketMQ: seismic-grid-summary              │
└──────────────────────────────────────────────────────────────┘
```

### 各步骤一览

| 阶段 | 步骤 | 输入 | 输出格式 |
|------|------|------|----------|
| 初始化 | Step 1 坐标分配与网格构建 | dataset/ 目录 | Protobuf 二进制配置文件 |
| 数据采集 | Step 2 Producer | .seis 文件 + 坐标映射 | RocketMQ seismic-raw（Protobuf） |
| 流处理入口 | Step 3 Flink Source | RocketMQ seismic-raw | DataStream\<SeismicRecord\> |
| 过滤 | Step 4 硬件指标过滤 | DataStream\<SeismicRecord\> | 主流 + SideOutput |
| 信号提取 | Step 5 地震信号提取 | DataStream\<SeismicRecord\> | DataStream\<SeismicEvent\> |
| 特征检测 | Step 6 特征提取与异常检测 | DataStream\<SeismicEvent\> | DataStream\<EventFeature\> |
| 空间聚合 | Step 7 网格聚合 | DataStream\<EventFeature\> | DataStream\<GridSummary\> |
| 输出 | Step 8 Sink | EventFeature + GridSummary | RocketMQ 两个 Topic |

---

## 二、Protobuf 消息定义（.proto 文件）

所有跨模块传输的数据结构统一采用 Protocol Buffers（proto3）定义，替代 JSON。全部 `.proto` 文件放置于项目 `src/main/proto/` 目录，编译后生成对应的 Java 类供各模块使用。

### 为什么选择 Protobuf 而非 JSON

| 对比维度 | JSON | Protobuf（本方案） |
|----------|------|-------------------|
| 消息体大小（SeismicRecord 含1000采样点） | 约 6–8 KB | 约 1–2 KB（压缩率 60–80%） |
| 序列化速度 | 较慢（文本解析） | 快 3–10 倍（二进制编解码） |
| 类型安全 | 弱（解析方自行推断） | 强（.proto 强类型定义） |
| 2000节点并发吞吐 | 网络带宽压力大 | 大幅降低带宽占用 |
| 适配野外受限网络 | 差 | 好 |

> ⚠ 在野外网络带宽受限场景下，Protobuf 的体积优势可直接降低网络拥塞风险，这是本设计选择 Protobuf 的核心原因。

---

### 2.1 `seismic_record.proto` —— RocketMQ 原始消息

Producer 写入 RocketMQ 的消息体，对应从 `.seis` 文件解析出的一个 3048 字节数据块（1 秒数据）。

```protobuf
syntax = "proto3";
package seismic;

option java_package = "com.seismic.proto";
option java_outer_classname = "SeismicRecordProto";

// 对应一个 3048 字节数据块（1秒 / 1000个采样点）
message SeismicRecord {
  int32  nodeid      = 1;   // 节点ID
  int32  fixed_x     = 2;   // 硬编码 X 坐标（已替换原始随机值）
  int32  fixed_y     = 3;   // 硬编码 Y 坐标
  string grid_id     = 4;   // 所属网格ID，如 "3_7"
  int64  timestamp   = 5;   // 秒级时间戳
  int32  bat_voltage = 6;   // 电池电压
  int32  bat_temp    = 7;   // 电池温度
  repeated int32 samples = 8; // 1000个采样点（24位有符号整型）
}
```

---

### 2.2 `seismic_event.proto` —— 信号提取结果

Step 5（地震信号提取算子）输出的结果，仅包含检测到地震信号的数据块。

```protobuf
syntax = "proto3";
package seismic;

option java_package = "com.seismic.proto";
option java_outer_classname = "SeismicEventProto";

message SeismicEvent {
  int32  nodeid           = 1;   // 节点ID
  int32  fixed_x          = 2;   // X 坐标
  int32  fixed_y          = 3;   // Y 坐标
  string grid_id          = 4;   // 网格ID
  int64  timestamp        = 5;   // 秒级时间戳
  int32  signal_start_idx = 6;   // 信号起始位置（0–999）
  int32  signal_end_idx   = 7;   // 信号结束位置
  int32  signal_length    = 8;   // 信号持续采样点数
  int32  peak_amplitude   = 9;   // 峰值振幅
  double rms_energy       = 10;  // RMS 能量
  repeated int32 signal_samples = 11; // 信号段原始采样点（可选保留）
}
```

---

### 2.3 `event_feature.proto` —— 特征与异常检测结果

Step 6（特征提取与异常检测算子）基于 L2 缓存输出的特征记录，是 Sink 输出到 `seismic-node-events` 的消息体。

```protobuf
syntax = "proto3";
package seismic;

option java_package = "com.seismic.proto";
option java_outer_classname = "EventFeatureProto";

message EventFeature {
  int32  nodeid             = 1;   // 节点ID
  int32  fixed_x            = 2;   // X 坐标
  int32  fixed_y            = 3;   // Y 坐标
  string grid_id            = 4;   // 网格ID
  int64  timestamp          = 5;   // 秒级时间戳
  int32  peak_amplitude     = 6;   // 峰值振幅
  double rms_energy         = 7;   // RMS 能量
  int32  signal_length      = 8;   // 信号持续采样点数
  bool   is_anomaly         = 9;   // 是否异常（与历史均值偏差 > 50%）
  int32  recent_event_count = 10;  // 近 60 秒内触发次数
  double avg_peak_amplitude = 11;  // L2 缓存中的历史平均振幅
}
```

---

### 2.4 `grid_summary.proto` —— 网格聚合报告

Step 7（网格聚合算子）基于 L3 缓存输出的网格级统计，是 Sink 输出到 `seismic-grid-summary` 的消息体。

```protobuf
syntax = "proto3";
package seismic;

option java_package = "com.seismic.proto";
option java_outer_classname = "GridSummaryProto";

message GridSummary {
  string grid_id               = 1;  // 网格ID，如 "3_7"
  int32  active_node_count     = 2;  // 当前窗口内触发事件的节点数
  int32  total_node_count      = 3;  // 该网格总节点数（固定约 20）
  double avg_peak_amplitude    = 4;  // 网格内节点平均峰值振幅
  double trigger_rate          = 5;  // 触发率 = active / total
  int64  last_update_timestamp = 6;  // 最近一次更新的时间戳（秒）
}
```

---

### 2.5 `node_config.proto` —— 坐标配置文件

Step 1 生成的坐标映射配置，以 Protobuf 格式持久化，供 Producer 和 Flink 算子加载，替代 JSON 配置文件。

```protobuf
syntax = "proto3";
package seismic;

option java_package = "com.seismic.proto";
option java_outer_classname = "NodeConfigProto";

// 单个节点的坐标与网格信息
message NodeInfo {
  int32  nodeid   = 1;
  int32  fixed_x  = 2;
  int32  fixed_y  = 3;
  string grid_id  = 4;
}

// 全量节点映射（序列化为 config/node_mapping.pb）
message NodeMapping {
  repeated NodeInfo nodes = 1;  // 2000 个节点信息
}

// 单个网格的包含节点列表
message GridMapping {
  string grid_id          = 1;
  repeated int32 node_ids = 2;  // 该网格包含的 nodeid 列表
  int32  total_node_count = 3;
}

// 全量网格映射（序列化为 config/grid_mapping.pb）
message GridMappingList {
  repeated GridMapping grids = 1;  // 100 个网格信息
}
```

> ⚠ `config/node_mapping.pb` 和 `config/grid_mapping.pb` 分别对应 `NodeMapping` 和 `GridMappingList` 的序列化文件，在项目启动时由 Producer 和 Flink 算子各自加载到内存。

---

## 三、分步骤详细设计

---

### Step 1：系统初始化 —— 坐标分配与网格构建

#### 目标
为 2000 个节点分配固定虚拟坐标，构建空间网格映射关系，持久化为 Protobuf 配置文件。

#### 输入 / 输出

| 类型 | 内容 | 格式 |
|------|------|------|
| 输入 | `dataset/` 目录下的 2000 个文件夹（文件夹名即 nodeid） | 文件系统 |
| 输出 | `config/node_mapping.pb`（NodeMapping） | Protobuf 二进制 |
| 输出 | `config/grid_mapping.pb`（GridMappingList） | Protobuf 二进制 |

#### 坐标分配规则

| 参数 | 公式 / 值 | 说明 |
|------|-----------|------|
| 总节点数 | 2000 | 2000 个检波器 |
| 空间布局 | 50列 × 40行 | 排列基准 |
| fixed_x | `(rank % 50) × 100` | rank 为按 nodeid 字典序升序后的序号（0–1999） |
| fixed_y | `(rank / 50) × 100` | 整除 |
| grid_col | `(rank % 50) / 5` | 整除，范围 0–9 |
| grid_row | `(rank / 50) / 4` | 整除，范围 0–9 |
| grid_id | `"row_col"`（如 `"3_7"`） | 字符串拼接 |
| 网格总数 | 10×10 = 100 个 | 每格约 20 节点 |
| 坐标范围 | x ∈ [0, 4900], y ∈ [0, 3900] | — |

#### 实现流程

1. 扫描 `dataset/` 目录，提取所有文件夹名作为 nodeid，按字典序升序排列（rank 0→1999）
2. 按照坐标公式为每个节点计算 `fixed_x`、`fixed_y`、`grid_col`、`grid_row`、`grid_id`
3. 构建 `NodeMapping`：将 2000 条 `NodeInfo` 写入 `repeated nodes` 字段
4. 构建 `GridMappingList`：按 `grid_id` 分组，统计每格的 `node_ids` 和 `total_node_count`
5. 使用 Protobuf 序列化写出 `node_mapping.pb` 和 `grid_mapping.pb` 到 `config/` 目录

#### 验证检查

- [ ] 确认生成 2000 条 `NodeInfo` 记录
- [ ] 确认生成恰好 100 个 `GridMapping`（10×10）
- [ ] 确认每个网格包含约 20 个节点
- [ ] 确认 `node_mapping.pb` 可被正确反序列化

---

### Step 2：Producer —— 读取 .seis 文件写入 RocketMQ

#### 目标
并发读取 2000 个 `.seis` 文件，解析二进制格式，修正坐标，将数据序列化为 `SeismicRecord` Protobuf 消息后发送至 RocketMQ。

#### 输入 / 输出

| 类型 | 内容 | 说明 |
|------|------|------|
| 输入 | `dataset/` 目录下的 `.seis` 文件 | 每个节点一个文件 |
| 输入 | `config/node_mapping.pb` | 坐标映射配置 |
| 输出 | RocketMQ Topic: `seismic-raw` | 消息体为 `SeismicRecord` Protobuf |

#### 元数据字段偏移表（小端序 LITTLE_ENDIAN）

| 字节范围 | 长度 | 类型 | 字段名 | 说明 |
|---------|------|------|--------|------|
| 0–1 | 2 | short | size | 固定 3048 |
| 2–3 | 2 | short | nSamples | 固定 1000 |
| 12–15 | 4 | int | nodeid | 节点ID |
| 20–23 | 4 | int | x | **需修正为 fixed_x** |
| 24–27 | 4 | int | y | **需修正为 fixed_y** |
| 32–35 | 4 | int | timestamp | 起始时间戳（秒级） |
| 40–41 | 2 | short | batVoltage | 电池电压 |
| 42–43 | 2 | short | batTemp | 电池温度 |
| 48–3047 | 3000 | byte[] | samples | 1000 个 24 位有符号整型采样点 |

#### 实现流程

1. 启动时反序列化 `config/node_mapping.pb`，构建内存 `Map<Integer, NodeInfo>`（nodeid → NodeInfo）
2. 初始化 RocketMQ Producer，配置 NameServer 地址和 ProducerGroup
3. 创建固定大小线程池（建议 50 个线程），每个线程处理若干 `.seis` 文件
4. 对每个 `.seis` 文件：
   - 跳过文件头 1024 字节
   - 循环读取 3048 字节数据块，不足 3048 字节的尾部直接丢弃
5. 解析前 48 字节元数据（LITTLE_ENDIAN）：提取 `nodeid`、`timestamp`、`batVoltage`、`batTemp`
6. 查询 `NodeInfo`，获取 `fixed_x`、`fixed_y`、`grid_id`（不修改原始字节，直接在 Protobuf 构造时填入正确值）
7. 解析后 3000 字节波形数据：按 3 字节一组解析为 24 位有符号整数（注意符号扩展），填充 `samples` 数组
8. 构造 `SeismicRecord` Protobuf 对象，调用 `toByteArray()` 序列化
9. 构造 RocketMQ `Message`（`MessageKey = nodeid`），调用 `producer.send(message)`
10. 每条消息发送后 `Thread.sleep(1000)` 控制发送速率（每节点每秒 1 条）

#### 24 位整数解析说明

每个采样点占 3 字节（小端序），解析为有符号 24 位整数：

```
b0 = buffer[offset]     & 0xFF
b1 = buffer[offset + 1] & 0xFF
b2 = buffer[offset + 2]          // 最高字节，保留符号
value = (b2 << 16) | (b1 << 8) | b0
// 符号扩展：若第 23 位为 1，将高 8 位置为 0xFF
if ((value & 0x800000) != 0) value |= 0xFF000000;
```

#### 关键注意点

- **字节序**：全部元数据字段使用 `LITTLE_ENDIAN` 解析
- **坐标修正**：不修改原始字节，在构造 Protobuf 对象时直接填入 `fixed_x`、`fixed_y`
- **MessageKey**：设置为 nodeid 字符串，保证同一节点消息有序消费
- **速率控制**：每节点每秒 1 条消息，2000 节点并发约 2000 条/秒
- **消息体格式**：`SeismicRecord.toByteArray()`，不使用 JSON

#### 验证检查

- [ ] 确认消息成功发送到 `seismic-raw` Topic
- [ ] 确认消息体可被 `SeismicRecord.parseFrom()` 正确反序列化
- [ ] 确认 `fixed_x`、`fixed_y`、`grid_id` 字段已正确填入
- [ ] 确认发送速率约 2000 条/秒

---

### Step 3：Flink Source —— 从 RocketMQ 消费数据

#### 目标
Flink 作业订阅 RocketMQ，将 Protobuf 消息反序列化为 `SeismicRecord` 对象，配置事件时间 Watermark，按 nodeid 分区。

#### 输入 / 输出

| 类型 | 内容 | 格式 |
|------|------|------|
| 输入 | RocketMQ Topic: `seismic-raw` | SeismicRecord Protobuf |
| 输出 | `DataStream<SeismicRecord>`（keyBy nodeid，带 Watermark） | Java 对象流 |

#### 关键配置参数

| 配置项 | 值 | 说明 |
|--------|-----|------|
| Consumer Group | `seismic-consumer-group` | — |
| Topic | `seismic-raw` | — |
| 消费起始位置 | `CONSUME_FROM_LATEST` | — |
| 事件时间提取 | `record.timestamp × 1000L` | 秒→毫秒 |
| 乱序容忍度 | `forBoundedOutOfOrderness(5s)` | 5 秒容忍 |
| 空闲超时 | `withIdleness(30s)` | 防止某分区无数据时阻塞 Watermark 推进 |
| 分区键 | `keyBy(record.nodeid)` | 保证同节点数据顺序处理 |

#### 反序列化逻辑

- 实现 `DeserializationSchema<SeismicRecord>`
- 在 `deserialize(byte[] message)` 方法中调用 `SeismicRecord.parseFrom(message)`
- 直接得到结构化对象，无需手动字节解析（Protobuf 自动处理）
- 与 Producer 的编码对称：Producer 调用 `toByteArray()`，Source 调用 `parseFrom()`

#### 验证检查

- [ ] 确认成功消费 RocketMQ 消息
- [ ] 确认 `SeismicRecord` 对象各字段正确解析
- [ ] 确认 `samples` 数组包含 1000 个采样点
- [ ] 确认 Watermark 正常推进（无长时间阻塞）
- [ ] 确认按 nodeid 正确分区

---

### Step 4：硬件指标过滤算子

#### 目标
基于元数据中的硬件状态字段，过滤明显异常的采集数据，异常数据通过 SideOutput 单独输出，不阻断主流程。

#### 输入 / 输出

| 类型 | 内容 |
|------|------|
| 输入 | `DataStream<SeismicRecord>`（keyBy nodeid） |
| 主流输出 | `DataStream<SeismicRecord>`（正常数据） |
| 侧输出流 | `DataStream<SeismicRecord>`（异常数据，Tag: `"filtered-data"`） |

#### 过滤规则

| 检查项 | 过滤条件 | 处理方式 |
|--------|----------|----------|
| 电池电压 | `batVoltage < 10` | 输出到侧输出流，主流丢弃 |
| 电池温度 | `batTemp > 80` | 输出到侧输出流，主流丢弃 |
| 时间戳间隔 | 与上条数据时间差 < 0 或 > 10 秒 | 标记可疑，输出到侧输出流 |

#### 算子设计

- 继承 `KeyedProcessFunction<Integer, SeismicRecord, SeismicRecord>`（需要 Key 上下文维护状态）
- 使用 `ValueState<Long>` 存储该节点上一条数据的 `timestamp`，用于时间戳间隔检查
- 定义 `OutputTag<SeismicRecord>("filtered-data")` 作为侧输出标签
- 异常数据通过 `ctx.output(sideOutputTag, record)` 输出
- 正常数据通过 `out.collect(record)` 向下游传递
- 主流程通过 `.getSideOutput(filteredDataTag)` 获取异常数据流，可选写入日志 Topic

#### 验证检查

- [ ] 确认异常数据进入侧输出流，主流不受阻断
- [ ] 确认 `ValueState` 正确维护每个节点的 `lastTimestamp`
- [ ] 确认三条过滤规则均生效

---

### Step 5：地震信号提取算子

#### 目标
分析 1000 个采样点，检测地震信号段，丢弃纯噪音数据，计算初步波形特征。

#### 输入 / 输出

| 类型 | 内容 |
|------|------|
| 输入 | `DataStream<SeismicRecord>`（keyBy nodeid） |
| 输出 | `DataStream<SeismicEvent>`（仅包含检测到地震信号的数据） |

#### 信号检测逻辑

| 步骤 | 说明 |
|------|------|
| 设置噪音阈值 | `NOISE_THRESHOLD = 100`（前半段噪音为个位数，信号段为数十万级，阈值区分明确） |
| 检测 signal_start_idx | 从前往后扫描，找第一个 `|sample[i]| > NOISE_THRESHOLD` 的位置 |
| 无信号处理 | 若 `signal_start_idx == -1`，说明该秒为纯噪音，直接 `return`，不输出任何事件 |
| 检测 signal_end_idx | 从 `signal_start_idx` 继续向后扫描，找第一个回落至噪音水平的位置，或取 `999` |
| 提取信号段 | `signal_samples = samples[signal_start_idx : signal_end_idx]` |
| 计算峰值振幅 | `peak_amplitude = max(|signal_samples[i]|)` |
| 计算 RMS 能量 | `rms_energy = sqrt( Σ(sample²) / signal_length )` |
| 构造输出 | 构造 `SeismicEvent` 对象，通过 `out.collect(event)` 输出 |

#### SeismicEvent 字段说明

| 字段 | 来源 | 说明 |
|------|------|------|
| nodeid / fixed_x / fixed_y / grid_id / timestamp | 直接继承自 SeismicRecord | — |
| signal_start_idx | 本算子计算 | 信号起始下标（0–999） |
| signal_end_idx | 本算子计算 | 信号结束下标 |
| signal_length | `signal_end_idx - signal_start_idx` | 信号持续采样点数 |
| peak_amplitude | 本算子计算 | 峰值振幅（绝对值最大值） |
| rms_energy | 本算子计算 | 均方根能量 |
| signal_samples | 信号段原始数据 | 可选保留，用于下游异常检测 |

> ⚠ 纯噪音数据在此步骤被丢弃，显著降低下游算子的处理压力。

#### 验证检查

- [ ] 确认纯噪音数据不产生输出
- [ ] 确认信号段下标范围合理（`signal_start_idx < signal_end_idx`）
- [ ] 确认 `peak_amplitude` 和 `rms_energy` 计算正确

---

### Step 6：特征提取与异常检测算子（L2 缓存）

#### 目标
基于 L2 缓存维护的节点历史事件，进行异常检测，输出带标注的特征记录。

#### 输入 / 输出

| 类型 | 内容 |
|------|------|
| 输入 | `DataStream<SeismicEvent>`（keyBy nodeid） |
| 输出 | `DataStream<EventFeature>` |

#### L2 缓存设计

| 设计维度 | 参数 / 值 | 说明 |
|----------|-----------|------|
| 存储位置 | 算子内 `LinkedHashMap` | 非 Flink 托管状态，存于算子 JVM 堆内存，读写速度为纯内存 |
| 容量 | 1500 个节点槽位 | 超出时 LRU 淘汰最久未访问的节点 |
| 时间窗口 | 最近 60 秒 | 每次处理时清理超过 60s 的历史事件（`removeIf`） |
| TTL | 300 秒 | 超过 300 秒未访问的节点槽位定期清理 |
| 访问顺序 | `accessOrder = true` | `LinkedHashMap` 按访问顺序排列，天然支持 LRU |
| LRU-K | K = 2 | 需被访问 2 次后才视为热点，避免偶发事件污染缓存 |

#### NodeHistory 数据结构

| 字段 | 类型 | 说明 |
|------|------|------|
| recent_events | `List<SeismicEvent>` | 该节点最近 60 秒内的地震事件列表 |
| avg_peak_amplitude | double | 历史平均峰值振幅（异常检测基准） |
| avg_rms_energy | double | 历史平均 RMS 能量 |
| event_count | int | 近期事件总数 |
| last_update_timestamp | long | 最后更新时间戳（用于 TTL 判断） |

#### 处理流程

1. 读取 L2 缓存：`l2Cache.get(nodeid)`，判断是否命中（L2 Hit / Miss）
2. **L2 命中**：清理该节点历史中超过 60 秒的旧事件（`removeIf`）
3. **L2 缺失**：创建新的 `NodeHistory` 对象（冷启动，跳过本次异常检测）
4. **异常检测**（仅 L2 命中且 `event_count > 0` 时执行）：
   - `deviation = |current.peak_amplitude - history.avg_peak_amplitude| / history.avg_peak_amplitude`
   - 若 `deviation > 0.5`，标记 `is_anomaly = true`
5. 更新缓存：将当前事件加入 `recent_events`，重新计算统计量（`avg_peak_amplitude`、`avg_rms_energy`、`event_count`），更新 `last_update_timestamp`
6. 写回缓存：`l2Cache.put(nodeid, history)`，`LinkedHashMap` 自动执行 LRU 淘汰（容量 > 1500 时）
7. 构造 `EventFeature` 并 `out.collect(feature)`

#### EventFeature 字段说明

| 字段 | 来源 | 说明 |
|------|------|------|
| nodeid / fixed_x / fixed_y / grid_id / timestamp | 继承自 SeismicEvent | — |
| peak_amplitude / rms_energy / signal_length | 继承自 SeismicEvent | — |
| is_anomaly | 本算子计算 | 是否异常 |
| recent_event_count | L2 缓存读取 | 近 60 秒触发次数 |
| avg_peak_amplitude | L2 缓存读取 | 历史平均振幅 |

#### 验证检查

- [ ] 确认异常标注逻辑正确（偏差 > 50% 才标记）
- [ ] 确认冷启动（L2 缺失）时不输出 `is_anomaly = true`
- [ ] 确认 L2 缓存不超过 1500 条（LRU 淘汰生效）
- [ ] 确认时间窗口清理正常（超过 60 秒的事件被移除）

---

### Step 7：网格聚合算子（L3 缓存）

#### 目标
以空间网格为粒度，汇聚来自不同节点的特征事件，输出网格级统计报告。

#### 输入 / 输出

| 类型 | 内容 |
|------|------|
| 输入 | `DataStream<EventFeature>`（keyBy grid_id） |
| 输出 | `DataStream<GridSummary>` |

> ⚠ 此算子应对 `EventFeature` 按 `grid_id` 进行 `keyBy`，每个 grid_id 由独立的算子子任务处理，避免全局 ProcessFunction 单并行度的性能瓶颈。

#### L3 缓存设计

| 设计维度 | 参数 / 值 | 说明 |
|----------|-----------|------|
| 静态部分 | `grid_id → total_node_count` 映射（永不淘汰） | 启动时从 `grid_mapping.pb` 加载，全量持有 |
| 动态部分 | `grid_id → GridSummary` 统计对象 | 实时更新的网格聚合结果 |
| 动态容量 | 50 个网格条目 | 超出时 LRU 淘汰最久未活跃的网格 |
| TTL | 600 秒 | 超过 600 秒未更新的网格条目定期清理 |
| 聚合指标 | active_node_count / avg_peak_amplitude / trigger_rate | 见 GridSummary proto 定义 |

#### GridSummary 更新逻辑

1. 根据 `feature.grid_id` 查找 L3 动态缓存
2. **L3 动态缺失**：从静态映射获取 `total_node_count`，初始化新的 `GridSummary` 对象
3. 更新字段：`active_node_count` 自增 1
4. 滑动均值更新：`avg_peak_amplitude = (old_avg × (count-1) + current) / count`
5. 计算触发率：`trigger_rate = active_node_count / total_node_count`
6. 更新 `last_update_timestamp`，写回 L3 动态缓存
7. 每次更新后通过 `out.collect(summary)` 输出最新 `GridSummary`

#### 验证检查

- [ ] 确认 `trigger_rate` 计算正确
- [ ] 确认 `total_node_count` 与配置文件一致（约 20）
- [ ] 确认 L3 动态缓存不超过 50 条（LRU 淘汰生效）
- [ ] 确认 `avg_peak_amplitude` 滑动均值更新正确

---

### Step 8：Sink —— 结果写回 RocketMQ

#### 目标
将处理结果以 Protobuf 格式写回 RocketMQ，输出两类 Topic 的消息。

#### 输出配置

| 数据流 | Topic | MessageKey | 消息体格式 |
|--------|-------|------------|-----------|
| `DataStream<EventFeature>` | `seismic-node-events` | `feature.nodeid`（字符串） | `EventFeature.toByteArray()` |
| `DataStream<GridSummary>` | `seismic-grid-summary` | `summary.grid_id` | `GridSummary.toByteArray()` |

#### 序列化方式

- 实现自定义 `SerializationSchema<EventFeature>` 和 `SerializationSchema<GridSummary>`
- 在 `serialize()` 方法中调用 `object.toByteArray()` 返回 Protobuf 二进制字节数组
- 不使用 JSON 序列化，消息体体积约为同等 JSON 的 1/5 至 1/3

#### 发送参数

| 参数 | 值 | 说明 |
|------|-----|------|
| Producer Group | `seismic-sink-producer-group` | — |
| 批量发送大小 | 32 条/批 | 减少网络往返次数 |
| 发送超时 | 3000 ms | — |

#### 验证检查

- [ ] 确认 `seismic-node-events` Topic 可消费到 `EventFeature` 消息
- [ ] 确认 `seismic-grid-summary` Topic 可消费到 `GridSummary` 消息
- [ ] 确认消息体可被对应 Protobuf 类正确反序列化

---

## 四、缓存层设计汇总

| 缓存层 | 作用域 | 容量 | 时间窗口 | 淘汰策略 | 核心功能 |
|--------|--------|------|----------|----------|----------|
| **L2** | 单节点 | 1500 节点 | 60 秒 | LRU(1500) + TTL(300s) | 异常检测、节点趋势分析（Step 6） |
| **L3 静态** | 全局 | 永不淘汰 | 永久 | 无淘汰 | nodeid → grid_id 映射（Step 7 启动加载） |
| **L3 动态** | 空间网格 | 50 网格 | 600 秒 | LRU(50) + TTL(600s) | 网格聚合统计（Step 7） |

---

## 五、推荐实现顺序与验证要点

| 顺序 | 实现内容 | 核心验证点 | 依赖前置 |
|------|----------|------------|----------|
| 1 | Step 1：坐标初始化脚本 | `node_mapping.pb` 可反序列化，100 个网格，每格约 20 节点 | 无 |
| 2 | `.proto` 文件编译 | 生成 Java 类，确认 `toByteArray()` / `parseFrom()` 可用 | Step 1 完成 |
| 3 | Step 2：Producer | 消息发送成功，`fixed_x/y/grid_id` 正确 | Step 1 + 编译完成 |
| 4 | Step 3：Flink Source | 成功反序列化消息，Watermark 正常推进 | Step 2 完成 |
| 5 | Step 4：硬件过滤 | 异常数据进入侧输出流，主流不受阻断 | Step 3 完成 |
| 6 | Step 5：信号提取 | 纯噪音数据被丢弃，信号段正确提取 | Step 4 完成 |
| 7 | Step 6：L2 缓存 + 异常检测 | 异常标注正确，L2 命中率 > 70% | Step 5 完成 |
| 8 | Step 7：L3 缓存 + 网格聚合 | `trigger_rate` 计算正确，L3 命中率 > 70% | Step 6 完成 |
| 9 | Step 8：Sink | 两个 Topic 均可消费到正确格式的 Protobuf 消息 | Step 7 完成 |

---

## 六、项目目录结构建议

```
seismic-platform/
├── config/
│   ├── node_mapping.pb          # NodeMapping（Step 1 生成）
│   └── grid_mapping.pb          # GridMappingList（Step 1 生成）
│
├── src/main/proto/
│   ├── seismic_record.proto     # RocketMQ 原始消息
│   ├── seismic_event.proto      # 信号提取结果
│   ├── event_feature.proto      # 特征与异常检测结果
│   ├── grid_summary.proto       # 网格聚合报告
│   └── node_config.proto        # 坐标配置文件
│
├── src/main/java/com/seismic/
│   ├── init/
│   │   └── NodeMappingInitializer.java   # Step 1
│   ├── producer/
│   │   └── SeismicProducer.java          # Step 2
│   ├── flink/
│   │   ├── source/
│   │   │   └── SeismicRecordDeserializer.java   # Step 3
│   │   ├── operator/
│   │   │   ├── HardwareFilterFunction.java      # Step 4
│   │   │   ├── SignalExtractionFunction.java     # Step 5
│   │   │   ├── FeatureExtractionFunction.java   # Step 6（L2缓存）
│   │   │   └── GridAggregationFunction.java     # Step 7（L3缓存）
│   │   └── sink/
│   │       ├── EventFeatureSerializer.java      # Step 8
│   │       └── GridSummarySerializer.java       # Step 8
│   └── SeismicStreamJob.java     # Flink 主作业入口
│
└── dataset/
    ├── 45050001/
    │   └── 45050001-20260203-172809.seis
    ├── 45050002/
    │   └── 45050002-20260203-172809.seis
    └── ...（共 2000 个节点目录）
```


---
## 完整数据流拓扑
```text
                                  seismic-raw (RocketMQ)
                                         │
                                    ┌────┴────┐
                                    │  Source  │ (并行度=2)
                                    └────┬────┘
                                         │ SeismicRecord
                                    ┌────┴────┐
                                    │Watermark │
                                    └────┬────┘
                                         │
                                  keyBy(nodeid)
                                         │
                                    ┌────┴────┐
                                    │Hardware  │ (并行度=4)
                                    │ Filter   │
                                    └──┬───┬──┘
                          正常数据 ←──┘   └──→ SideOutput(异常数据)
                                    │
                             keyBy(nodeid)
                                    │
                                ┌───┴───┐
                                │Signal │ (并行度=4)
                                │Extract│
                                └───┬───┘
                                    │ SeismicEvent
                             keyBy(nodeid)
                                    │
                              ┌─────┴─────┐
                              │ Feature   │ (并行度=4, L2 缓存)
                              │ Extraction│
                              └─────┬─────┘
                                    │ EventFeature
                         ┌──────────┼──────────┐
                         │                     │
                  keyBy(gridId)                │
                         │                     │
                   ┌─────┴─────┐        ┌──────┴──────┐
                   │   Grid    │        │ EventFeature │ (并行度=2)
                   │Aggregation│        │    Sink      │
                   │ (L3 缓存) │        └──────┬──────┘
                   └─────┬─────┘               │
                         │ GridSummary    seismic-node-events
                   ┌─────┴─────┐          (RocketMQ)
                   │GridSummary│ (并行度=2)
                   │   Sink    │
                   └─────┬─────┘
                         │
                  seismic-grid-summary
                     (RocketMQ)

```

---
# 网址
```text

HDFS:
http://192.168.56.151:9870/dfshealth.html#tab-overview

Fink:
http://192.168.56.151:8081/#/overview

Rocketmq:
http://192.168.56.1:8080/#/topic
```
---
# 启动程序
```text
cd /opt/app/flink-1.17.2

/opt/app/flink-1.17.2/bin/stop-cluster.sh
/opt/app/flink-1.17.2/bin/start-cluster.sh
```

```text
格式化hdfs （node01）：
hdfs namenode -format

启动hdfs：
stop-dfs.sh
start-dfs.sh

1️⃣ HDFS 创建 savepoint
hdfs dfs -mkdir -p /flink/savepoints

2️⃣ 给 Flink/DS2 写权限
hdfs dfs -chmod 777 /flink/savepoints

验证：
hdfs dfs -ls /flink
```


```text
【启动nameserver，带.conf】
nohup  /usr/local/rocketmq/bin/mqnamesrv &

【带.conf启动broker】
nohup sh /usr/local/rocketmq/bin/mqbroker -c /usr/local/rocketmq/conf/dledger/broker.conf &
```


---
# flink集群相关指令
```text
【上传 JAR 到 Flink】
# 在 node01 上执行
curl -X POST http://localhost:8081/jars/upload \
  -F "jarfile=@/opt/flink_jobs/flink-rocketmq-demo-1.0-SNAPSHOT.jar"

【查看 JAR ID】
# 在 node01 上执行
curl -s http://localhost:8081/jars

【提交作业】
curl -X POST http://localhost:8081/jars/JOB_ID/run \
  -H "Content-Type: application/json" \
  -d '{
    "parallelism": 2,
    "programArgs": "--p-source 1 --p-filter 2 --p-signal 2 --p-feature 2 --p-grid 2 --p-sink 1"
  }'
```
---
# ☠问题：

V2：⚠️ 功能正常，但发现一个严重性能问题

```text
功能层面全部通过：
反序列化失败 = 0 ✅
坐标不匹配 = 0 ✅
采样点异常 = 0 ✅
但有一个严重问题需要重视：
平均消息体大小: 5415 字节
原始数据块大小: 3048 字节
膨胀比率: 约 178%
消息体大小分布:
  > 5KB:  9995  （99.99% 的消息都超过 5KB）
这直接印证了之前提到的 int32 编码负数问题。每条消息携带约 460 个负数采样点，每个负数用 int32 编码占 10 字节，导致消息体是原始数据的 1.78 倍。这个数据可以直接写进论文作为"改进前"的对比基线。
```


```text
根本原因：L2 缓存容量不足

L2_MAX_CAPACITY = 200（当前代码中的值）
每个 Flink 子任务处理的节点数 = 2000 / 4 = 500
缓存能覆盖的节点比例 = 200 / 500 = 40%


但实际命中率只有 20%，比理论上限还低，说明 LRU 淘汰非常频繁，节点数据很快就被挤出缓存。
这正好对应之前建议中提到的改进点，需要将 L2_MAX_CAPACITY 从 200 提高到至少 500。
```

```text
实现完整性层面（需补充）

异常侧输出流未接 Sink

HardwareFilterFunction 父类规范问题
```


```text
sink算子有待优化，会产生背压
```
