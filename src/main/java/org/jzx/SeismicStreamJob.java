package org.jzx;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 */
public class SeismicStreamJob {
    private static final Logger LOG = LoggerFactory.getLogger(SeismicStreamJob.class);

    // RocketMQ 配置
    private static final String NAMESRV_ADDR = "192.168.56.151:9876";
    private static final String SOURCE_TOPIC = "seismic-raw";
    private static final String CONSUMER_GROUP = "seismic-flink-consumer-group";

    public static void main(String[] args) throws Exception {
        LOG.info("========== 启动 SeismicStreamJob ==========");

        // ============================
        // 1. Flink 执行环境配置
        // ============================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // SeismicStreamJob.java 中添加：


        // ============================
        // Checkpoint 配置
        // ============================
        env.enableCheckpointing(30000);  // 30秒间隔，默认就是 EXACTLY_ONCE
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);


        // ============================
        // 2. RocketMQ Source
        // ============================
        DataStream<SeismicRecord> sourceStream = env
                .addSource(new RocketMQSeismicSource(NAMESRV_ADDR, SOURCE_TOPIC, CONSUMER_GROUP))
                .setParallelism(2)
                .name("RocketMQ Source");

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
                .name("Assign Watermarks");

        // ============================
        // 4. 按 nodeid 分区 → 硬件过滤
        // ============================
        SingleOutputStreamOperator<SeismicRecord> filteredStream = streamWithWatermark
                .keyBy(record -> record.getNodeid())
                .process(new HardwareFilterFunction())
                .name("Hardware Filter");

        DataStream<SeismicRecord> abnormalStream = filteredStream
                .getSideOutput(HardwareFilterFunction.FILTERED_DATA_TAG);

        // ============================
        // 5. 按 nodeid 分区 → 信号提取
        // ============================
        DataStream<SeismicEvent> eventStream = filteredStream
                .keyBy(record -> record.getNodeid())
                .process(new SignalExtractionFunction())
                .name("Signal Extraction");

        // ============================
        // 6. 按 nodeid 分区 → 特征提取与异常检测（L2 缓存）
        // ============================
        DataStream<EventFeature> featureStream = eventStream
                .keyBy(event -> event.getNodeid())
                .process(new FeatureExtractionFunction())
                .name("Feature Extraction");

        // ============================
        // 7. 按 grid_id 重新分区 → 网格聚合（L3 缓存）
        // ============================
        DataStream<GridSummary> gridSummaryStream = featureStream
                .keyBy(feature -> feature.getGridId())
                .process(new GridAggregationFunction())
                .name("Grid Aggregation");

        // ============================
        // 8. Sink：EventFeature → seismic-node-events
        // ============================
        featureStream
                .addSink(new EventFeatureSink(NAMESRV_ADDR))
                .setParallelism(2)
                .name("EventFeature Sink");

        // ============================
        // 9. Sink：GridSummary → seismic-grid-summary
        // ============================
        gridSummaryStream
                .addSink(new GridSummarySink(NAMESRV_ADDR))
                .setParallelism(2)
                .name("GridSummary Sink");

        // ============================
        // 10. 启动作业
        // ============================
        LOG.info("作业拓扑构建完成，启动执行...");
        env.execute("Seismic Stream Processing Job");
    }
}
