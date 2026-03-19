package org.jzx.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.jzx.proto.EventFeatureProto.EventFeature;

import java.util.Arrays;

/**
 * EventFeature Sink
 * 将节点级特征数据序列化为 Protobuf 写入 RocketMQ
 *
 * Topic: seismic-node-events
 * MessageKey: nodeid
 *
 * ML Tuner 暴露的 Gauge 指标：
 * - latency_p99: Sink 端处理耗时 P99（微秒），反映 Sink 的实际处理负荷
 *
 * 主延迟指标依赖 Flink 原生 LatencyMarker 机制：
 * - 在 SeismicStreamJob 中通过 setLatencyTrackingInterval(5000) 启用
 * - Flink 自动在 Source 注入时间标记，Sink 端计算 Source→Sink 的真实处理延迟
 * - ML Tuner 通过 REST API 查询 Flink 原生的 latency 指标
 */
public class EventFeatureSink extends AbstractRocketMQSink<EventFeature> {

    // >>>>>> ML-TUNER 新增：处理耗时追踪
    /** 耗时采样缓冲区大小 */
    private static final int LATENCY_BUFFER_SIZE = 1000;

    /** 每隔多少条记录重新计算一次 P99 */
    private static final int LATENCY_CALC_INTERVAL = 100;

    /** 环形缓冲区：存储最近 LATENCY_BUFFER_SIZE 条记录的 Sink 处理耗时（微秒） */
    private transient long[] latencyBuffer;

    /** 环形缓冲区写入索引 */
    private transient int latencyWriteIdx;

    /** 已写入的总记录数 */
    private transient long latencyTotalCount;

    /** 当前 P99 处理耗时（微秒），供 Gauge 查询 */
    private transient volatile double latencyP99;
    // <<<<<< ML-TUNER 新增

    public EventFeatureSink(String namesrvAddr) {
        super(namesrvAddr, "seismic-node-events", "event-feature-sink-group");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // >>>>>> ML-TUNER 新增：初始化处理耗时追踪
        latencyBuffer = new long[LATENCY_BUFFER_SIZE];
        latencyWriteIdx = 0;
        latencyTotalCount = 0;
        latencyP99 = 0.0;

        getRuntimeContext().getMetricGroup()
                .gauge("latency_p99", () -> latencyP99);
        // <<<<<< ML-TUNER 新增
    }

    @Override
    public void invoke(EventFeature value, Context context) throws Exception {
        // >>>>>> ML-TUNER 新增：记录 Sink 处理起始时间
        long startNano = System.nanoTime();
        // <<<<<< ML-TUNER 新增

        // 调用父类的 invoke（批量缓冲 + 异步发送）
        super.invoke(value, context);

        // >>>>>> ML-TUNER 新增：计算本条记录在 Sink 内的处理耗时
        long elapsedMicros = (System.nanoTime() - startNano) / 1_000;  // 纳秒→微秒

        latencyBuffer[latencyWriteIdx] = elapsedMicros;
        latencyWriteIdx = (latencyWriteIdx + 1) % LATENCY_BUFFER_SIZE;
        latencyTotalCount++;

        // 每 LATENCY_CALC_INTERVAL 条重新计算 P99
        if (latencyTotalCount % LATENCY_CALC_INTERVAL == 0) {
            int sampleSize = (int) Math.min(latencyTotalCount, LATENCY_BUFFER_SIZE);
            long[] sorted = new long[sampleSize];
            System.arraycopy(latencyBuffer, 0, sorted, 0, sampleSize);
            Arrays.sort(sorted);
            int p99Index = (int) Math.ceil(sampleSize * 0.99) - 1;
            p99Index = Math.max(0, Math.min(p99Index, sampleSize - 1));
            latencyP99 = sorted[p99Index];
        }
        // <<<<<< ML-TUNER 新增
    }

    @Override
    protected byte[] serialize(EventFeature value) {
        return value.toByteArray();
    }

    @Override
    protected String extractKey(EventFeature value) {
        return String.valueOf(value.getNodeid());
    }
}
