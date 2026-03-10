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
 * 示例：
 *   flink run -c org.jzx.SeismicStreamJob myjar.jar \
 *       --p-source 2 --p-filter 6 --p-signal 6 --p-feature 8 --p-grid 4 --p-sink 2
 */
public class SeismicStreamJob {
    private static final Logger LOG = LoggerFactory.getLogger(SeismicStreamJob.class);

    // RocketMQ 配置
    private static final String NAMESRV_ADDR = "192.168.56.151:9876";
    private static final String SOURCE_TOPIC = "seismic-raw";
    private static final String CONSUMER_GROUP = "seismic-flink-consumer-group";

    // 作业名（必须与 ds2_config.yaml 中的 job_name 完全一致）
    private static final String JOB_NAME = "Seismic-Stream-Job";

    // 各算子默认并行度
    private static final int DEFAULT_SOURCE_P  = 2;
    private static final int DEFAULT_FILTER_P  = 4;
    private static final int DEFAULT_SIGNAL_P  = 4;
    private static final int DEFAULT_FEATURE_P = 4;
    private static final int DEFAULT_GRID_P    = 4;
    private static final int DEFAULT_SINK_P    = 2;

    public static void main(String[] args) throws Exception {
        LOG.info("========== 启动 {} ==========", JOB_NAME);

        // ============================
        // 0. 解析命令行参数
        // ============================
        ParameterTool params = ParameterTool.fromArgs(args);

        int sourceP  = params.getInt("p-source",  DEFAULT_SOURCE_P);
        int filterP  = params.getInt("p-filter",  DEFAULT_FILTER_P);
        int signalP  = params.getInt("p-signal",  DEFAULT_SIGNAL_P);
        int featureP = params.getInt("p-feature", DEFAULT_FEATURE_P);
        int gridP    = params.getInt("p-grid",    DEFAULT_GRID_P);
        int sinkP    = params.getInt("p-sink",    DEFAULT_SINK_P);

        LOG.info("并行度配置: source={}, filter={}, signal={}, feature={}, grid={}, sink={}",
                sourceP, filterP, signalP, featureP, gridP, sinkP);

        // ============================
        // 1. Flink 执行环境配置
        // ============================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 全局并行度设为各算子最大值（作为兜底，实际各算子会覆盖）
        int globalP = Math.max(Math.max(Math.max(sourceP, filterP),
                Math.max(signalP, featureP)), Math.max(gridP, sinkP));
        env.setParallelism(globalP);

        // 注册 Protobuf 序列化
        ProtobufSerializerRegistrar.registerAll(env);

        // Checkpoint 配置
        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 将参数注册到全局配置（算子内可通过 getRuntimeContext 获取）
        env.getConfig().setGlobalJobParameters(params);

        // ============================
        // 2. RocketMQ Source
        // ============================
        DataStream<SeismicRecord> sourceStream = env
                .addSource(new RocketMQSeismicSource(NAMESRV_ADDR, SOURCE_TOPIC, CONSUMER_GROUP))
                .setParallelism(sourceP)
                .name("RocketMQ-Source");

        // ============================
        // 3. Watermark 分配
        // ============================
        DataStream<SeismicRecord> streamWithWatermark = sourceStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<SeismicRecord>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<SeismicRecord>)
                                                (record, ts) -> record.getTimestamp() * 1000L
                                )
                                .withIdleness(Duration.ofSeconds(30))
                )
                .name("Watermark-Assigner");

        // ============================
        // 4. 按 nodeid 分区 → 硬件过滤
        // ============================
        SingleOutputStreamOperator<SeismicRecord> filteredStream = streamWithWatermark
                .keyBy(SeismicRecord::getNodeid)
                .process(new HardwareFilterFunction())
                .setParallelism(filterP)
                .name("Hardware-Filter");

        // 侧输出流（异常数据）
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

        env.execute(JOB_NAME);
    }
}
