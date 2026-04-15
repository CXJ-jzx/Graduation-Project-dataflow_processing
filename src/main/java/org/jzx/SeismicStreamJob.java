package org.jzx;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jzx.flink.operator.FeatureExtractionFunction;
import org.jzx.flink.operator.GridAggregationFunction;
import org.jzx.flink.operator.HardwareFilterFunction;
import org.jzx.flink.operator.SignalExtractionFunction;
import org.jzx.flink.sink.EventFeatureSink;
import org.jzx.flink.sink.GridSummarySink;
import org.jzx.flink.source.ProtobufSerializerRegistrar;
import org.jzx.flink.source.RocketMQSeismicSource;
import org.jzx.proto.EventFeatureProto.EventFeature;
import org.jzx.proto.GridSummaryProto.GridSummary;
import org.jzx.proto.SeismicEventProto.SeismicEvent;
import org.jzx.proto.SeismicRecordProto.SeismicRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Flink 主作业入口
 *
 * 支持通过命令行参数动态设置各算子并行度，配合 DS2 弹性调度使用：
 *
 *   --p-source   Source 并行度（默认 2）
 *   --p-filter   硬件过滤算子并行度（默认 4）
 *   --p-signal   信号提取算子并行度（默认 4）
 *   --p-feature  特征提取算子并行度（默认 4）
 *   --p-grid     网格聚合算子并行度（默认 4）
 *   --p-sink     Sink 并行度（默认 2）
 *
 * ML Tuner 参数：
 *   --checkpoint-interval     Checkpoint 间隔毫秒（默认 30000）
 *   --buffer-timeout          网络 buffer 超时毫秒（默认 100）
 *   --watermark-delay         Watermark 乱序容忍毫秒（默认 5000）
 *   --consumer-group-suffix   Consumer Group 后缀（默认 ""，由 ML Tuner 自动生成）
 *   --consume-from-latest     是否从最新位置消费（默认 false）
 *
 * 示例：
 *   flink run -c org.jzx.SeismicStreamJob myjar.jar \
 *       --p-source 2 --p-filter 6 --p-signal 6 \
 *       --checkpoint-interval 20000 --buffer-timeout 50 \
 *       --consumer-group-suffix _a3f7b2c1 --consume-from-latest true
 */
public class SeismicStreamJob {
    private static final Logger LOG = LoggerFactory.getLogger(SeismicStreamJob.class);

    // RocketMQ 配置
    private static final String NAMESRV_ADDR = "192.168.56.151:9876";
    private static final String SOURCE_TOPIC = "seismic-raw";
    private static final String BASE_CONSUMER_GROUP = "seismic-flink-consumer-group";

    // 作业名（必须与 ds2_config.yaml 中的 job_name 完全一致）
    private static final String JOB_NAME = "Seismic-Stream-Job";

    // 各算子默认并行度
    private static final int DEFAULT_SOURCE_P  = 2;
    private static final int DEFAULT_FILTER_P  = 4;
    private static final int DEFAULT_SIGNAL_P  = 4;
    private static final int DEFAULT_FEATURE_P = 4;
    private static final int DEFAULT_GRID_P    = 4;
    private static final int DEFAULT_SINK_P    = 2;

    // 引擎参数默认值
    private static final int DEFAULT_CHECKPOINT_INTERVAL = 30000;
    private static final int DEFAULT_BUFFER_TIMEOUT      = 100;
    private static final int DEFAULT_WATERMARK_DELAY     = 5000;

    public static void main(String[] args) throws Exception {
        LOG.info("========== 启动 {} ==========", JOB_NAME);

        // ============================
        // 0. 解析命令行参数
        // ============================
        ParameterTool params = ParameterTool.fromArgs(args);

        // 并行度参数
        int sourceP  = params.getInt("p-source",  DEFAULT_SOURCE_P);
        int filterP  = params.getInt("p-filter",  DEFAULT_FILTER_P);
        int signalP  = params.getInt("p-signal",  DEFAULT_SIGNAL_P);
        int featureP = params.getInt("p-feature", DEFAULT_FEATURE_P);
        int gridP    = params.getInt("p-grid",    DEFAULT_GRID_P);
        int sinkP    = params.getInt("p-sink",    DEFAULT_SINK_P);

        // 引擎参数
        int checkpointInterval = params.getInt("checkpoint-interval",
                DEFAULT_CHECKPOINT_INTERVAL);
        int bufferTimeout = params.getInt("buffer-timeout",
                DEFAULT_BUFFER_TIMEOUT);
        int watermarkDelay = params.getInt("watermark-delay",
                DEFAULT_WATERMARK_DELAY);

        // ★ 新增：Consumer Group 动态后缀
        String consumerGroupSuffix = params.get("consumer-group-suffix", "");
        String consumerGroup = BASE_CONSUMER_GROUP + consumerGroupSuffix;

        // ★ 新增：消费起始位置
        boolean consumeFromLatest = params.getBoolean("consume-from-latest", false);

        LOG.info("========================================");
        LOG.info("并行度: source={}, filter={}, signal={}, feature={}, grid={}, sink={}",
                sourceP, filterP, signalP, featureP, gridP, sinkP);
        LOG.info("引擎参数: checkpoint={}ms, buffer={}ms, watermark={}ms",
                checkpointInterval, bufferTimeout, watermarkDelay);
        LOG.info("Consumer Group: {}", consumerGroup);
        LOG.info("消费起点: {}", consumeFromLatest ? "LATEST (跳过积压)" : "FIRST_OFFSET (从头/续点)");
        LOG.info("========================================");

        // ============================
        // 1. Flink 执行环境配置
        // ============================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        int globalP = Math.max(Math.max(Math.max(sourceP, filterP),
                Math.max(signalP, featureP)), Math.max(gridP, sinkP));
        env.setParallelism(globalP);

        // 注册 Protobuf 序列化
        ProtobufSerializerRegistrar.registerAll(env);

        // Checkpoint 配置
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.disableOperatorChaining();

        // 网络 buffer 超时
        env.setBufferTimeout(bufferTimeout);

        // 延迟追踪
        env.getConfig().setLatencyTrackingInterval(5000);

        // 全局参数
        env.getConfig().setGlobalJobParameters(params);

        // ============================
        // 2. RocketMQ Source ★ 使用动态 Consumer Group
        // ============================
        DataStream<SeismicRecord> sourceStream = env
                .addSource(new RocketMQSeismicSource(
                        NAMESRV_ADDR,
                        SOURCE_TOPIC,
                        consumerGroup,         // ★ 动态 Consumer Group
                        consumeFromLatest      // ★ 消费起始位置
                ))
                .setParallelism(sourceP)
                .name("RocketMQ-Source");

        // ============================
        // 3. Watermark 分配
        // ============================
        DataStream<SeismicRecord> streamWithWatermark = sourceStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<SeismicRecord>forBoundedOutOfOrderness(
                                        Duration.ofMillis(watermarkDelay))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<SeismicRecord>)
                                                (record, ts) ->
                                                        record.getTimestamp() * 1000L
                                )
                                .withIdleness(Duration.ofSeconds(30))
                )
                .name("Watermark-Assigner");

        // ============================
        // 4. 按 nodeid 分区 → 硬件过滤
        // ============================
        SingleOutputStreamOperator<SeismicRecord> filteredStream =
                streamWithWatermark
                        .keyBy(SeismicRecord::getNodeid)
                        .process(new HardwareFilterFunction())
                        .setParallelism(filterP)
                        .name("Hardware-Filter");

        DataStream<SeismicRecord> abnormalStream = filteredStream
                .getSideOutput(HardwareFilterFunction.FILTERED_DATA_TAG);

        // ============================
        // 5. 按 nodeid 分区 → 信号提取
        // ============================
        DataStream<SeismicEvent> eventStream = filteredStream
                .keyBy(SeismicRecord::getNodeid)
                .process(new SignalExtractionFunction())
                .setParallelism(signalP)
                .name("Signal-Extraction");

        // ============================
        // 6. 按 nodeid 分区 → 特征提取与异常检测（L2 缓存）
        // ============================
        DataStream<EventFeature> featureStream = eventStream
                .keyBy(SeismicEvent::getNodeid)
                .process(new FeatureExtractionFunction())
                .setParallelism(featureP)
                .name("Feature-Extraction");

        // ============================
        // 7. 按 grid_id 重新分区 → 网格聚合（L3 缓存）
        // ============================
        DataStream<GridSummary> gridSummaryStream = featureStream
                .keyBy(EventFeature::getGridId)
                .process(new GridAggregationFunction())
                .setParallelism(gridP)
                .name("Grid-Aggregation");

        // ============================
        // 8. Sink：EventFeature → seismic-node-events
        // ============================
        featureStream
                .addSink(new EventFeatureSink(NAMESRV_ADDR))
                .setParallelism(sinkP)
                .name("EventFeature-Sink");

        // ============================
        // 9. Sink：GridSummary → seismic-grid-summary
        // ============================
        gridSummaryStream
                .addSink(new GridSummarySink(NAMESRV_ADDR))
                .setParallelism(sinkP)
                .name("GridSummary-Sink");

        // ============================
        // 10. 启动作业
        // ============================
        LOG.info("作业拓扑构建完成，启动执行...");
        LOG.info("各算子并行度: Source={}, Filter={}, Signal={}, Feature={}, Grid={}, Sink={}",
                sourceP, filterP, signalP, featureP, gridP, sinkP);
        LOG.info("引擎参数: checkpoint={}ms, buffer={}ms, watermark={}ms",
                checkpointInterval, bufferTimeout, watermarkDelay);
        LOG.info("Consumer Group: {} (consumeFromLatest={})",
                consumerGroup, consumeFromLatest);

        env.execute(JOB_NAME);
    }
}
