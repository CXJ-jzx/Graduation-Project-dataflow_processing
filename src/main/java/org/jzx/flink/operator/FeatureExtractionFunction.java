package org.jzx.flink.operator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.jzx.proto.EventFeatureProto.EventFeature;
import org.jzx.proto.SeismicEventProto.SeismicEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 特征提取与异常检测算子（L2 缓存）
 *
 * 功能：
 * 1. 维护 L2 缓存（每个节点最近 60 秒的历史事件）
 * 2. 异常检测：当前事件与历史均值偏差 > 50% 标记为异常
 * 3. 输出带标注的 EventFeature
 *
 * L2 缓存配置：
 * - 容量：1500 个节点
 * - 时间窗口：60 秒
 * - TTL：300 秒
 * - LRU-K：K=2
 */
public class FeatureExtractionFunction
        extends KeyedProcessFunction<Integer, SeismicEvent, EventFeature> {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureExtractionFunction.class);

    // L2 缓存参数
    private static final int L2_MAX_CAPACITY = 200;
    private static final int L2_TIME_WINDOW_SECONDS = 60;
    private static final int L2_TTL_SECONDS = 30;
    private static final int L2_LRU_K = 2;

    // 异常检测阈值
    private static final double ANOMALY_DEVIATION_THRESHOLD = 0.5;  // 50%

    // TTL 清理间隔（每处理 1000 条触发一次）
    private static final int TTL_CLEANUP_INTERVAL = 1000;

    // L2 缓存实例（非 Flink 托管状态，存于算子 JVM 堆内存）
    private transient L2Cache l2Cache;

    // 统计计数器
    private transient long totalEvents;
    private transient long anomalyCount;
    private transient long coldStartCount;
    private transient long processedCount;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        l2Cache = new L2Cache(L2_MAX_CAPACITY, L2_TIME_WINDOW_SECONDS, L2_TTL_SECONDS, L2_LRU_K);
        totalEvents = 0;
        anomalyCount = 0;
        coldStartCount = 0;
        processedCount = 0;

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        LOG.info("FeatureExtractionFunction subtask {} 已启动, L2缓存配置: 容量={}, 窗口={}s, TTL={}s, K={}",
                subtaskIndex, L2_MAX_CAPACITY, L2_TIME_WINDOW_SECONDS, L2_TTL_SECONDS, L2_LRU_K);
    }

    @Override
    public void processElement(
            SeismicEvent event,
            Context ctx,
            Collector<EventFeature> out) throws Exception {

        totalEvents++;
        int nodeid = event.getNodeid();
        long currentTimestamp = event.getTimestamp();

        // ============================
        // 1. 读取 L2 缓存
        // ============================
        NodeHistory history = l2Cache.get(nodeid);

        boolean isAbnormal = false;
        double avgPeakAmplitude = 0;
        int recentEventCount = 0;

        if (history == null) {
            // ============================
            // 2a. L2 缺失（冷启动）
            // ============================
            history = new NodeHistory();
            coldStartCount++;

            // 冷启动：不做异常检测，直接标记为正常

        } else {
            // ============================
            // 2b. L2 命中
            // ============================

            // 清理超过 60 秒的旧事件
            l2Cache.evictExpiredEvents(nodeid, currentTimestamp);

            // 获取历史统计
            avgPeakAmplitude = history.getAvgPeakAmplitude();
            recentEventCount = history.getEventCount();

            // ============================
            // 3. 异常检测（仅 L2 命中且有历史数据时执行）
            // ============================
            if (recentEventCount > 0 && avgPeakAmplitude > 0) {
                double deviation = Math.abs(event.getPeakAmplitude() - avgPeakAmplitude) / avgPeakAmplitude;

                if (deviation > ANOMALY_DEVIATION_THRESHOLD) {
                    isAbnormal = true;
                    anomalyCount++;
                }
            }
        }

        // ============================
        // 4. 更新缓存（将当前事件加入历史）
        // ============================
        history.addEvent(currentTimestamp, event.getPeakAmplitude(), event.getRmsEnergy());
        l2Cache.put(nodeid, history);

        // 更新后重新获取统计量（包含 history.getEventCount();
        avgPeakAmplitude = history.getAvgPeakAmplitude();

        // ============================
        // 5. 构造 EventFeature 输出
        // ============================
        EventFeature feature = EventFeature.newBuilder()
                .setNodeid(nodeid)
                .setFixedX(event.getFixedX())
                .setFixedY(event.getFixedY())
                .setGridId(event.getGridId())
                .setTimestamp(currentTimestamp)
                .setPeakAmplitude(event.getPeakAmplitude())
                .setRmsEnergy(event.getRmsEnergy())
                .setSignalLength(event.getSignalLength())
                .setIsAnomaly(isAbnormal)
                .setRecentEventCount(recentEventCount)
                .setAvgPeakAmplitude(avgPeakAmplitude)
                .build();

        out.collect(feature);
        processedCount++;

        // ============================
        // 6. 定期 TTL 清理 + 日志统计
        // ============================
        if (processedCount % TTL_CLEANUP_INTERVAL == 0) {
            l2Cache.ttlCleanup(currentTimestamp);

            LOG.info("L2缓存统计: {}, 异常数={}, 冷启动数={}",
                    l2Cache.getStats(), anomalyCount, coldStartCount);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("FeatureExtractionFunction 关闭, 最终统计: 总事件={}, 异常={}, 冷启动={}, L2缓存: {}",
                totalEvents, anomalyCount, coldStartCount,
                l2Cache != null ? l2Cache.getStats() : "null");
        super.close();
    }
}
