package org.jzx.flink.operator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.jzx.proto.EventFeatureProto.EventFeature;
import org.jzx.proto.GridSummaryProto.GridSummary;
import org.jzx.proto.NodeConfigProto.GridInfo;
import org.jzx.proto.NodeConfigProto.GridMappingList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * 网格聚合算子（L3 缓存）
 *
 * 输出策略：
 * 1. 每个网格每 EMIT_INTERVAL_SECONDS 秒输出一次聚合结果
 * 2. 使用 Flink Timer 机制实现定时触发
 *
 * ML Tuner 暴露的 Gauge 指标：
 * - l3_hit_rate:   缓存命中率 [0.0, 1.0]
 * - l3_occupancy:  缓存占用率 [0.0, 1.0]
 */
public class GridAggregationFunction
        extends KeyedProcessFunction<String, EventFeature, GridSummary> {

    private static final Logger LOG = LoggerFactory.getLogger(GridAggregationFunction.class);

    // L3 缓存参数
    private static final int L3_MAX_CAPACITY = 50;
    private static final int L3_TIME_WINDOW_SECONDS = 60;
    private static final int L3_TTL_SECONDS = 600;

    // 输出间隔（秒）：每个网格每 1 秒输出一次聚合结果
    private static final int EMIT_INTERVAL_SECONDS = 1;

    // TTL 清理间隔
    private static final int TTL_CLEANUP_INTERVAL = 500;

    // L3 缓存实例
    private transient L3Cache l3Cache;

    // 记录每个网格是否已注册 Timer
    private transient Map<String, Boolean> timerRegistered;

    // 统计计数器
    private transient long totalEvents;
    private transient long processedCount;
    private transient long emitCount;

    // >>>>>> ML-TUNER 新增：Gauge 指标值
    private transient volatile double l3HitRate;
    private transient volatile double l3Occupancy;
    // <<<<<< ML-TUNER 新增

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        l3Cache = new L3Cache(L3_MAX_CAPACITY, L3_TIME_WINDOW_SECONDS, L3_TTL_SECONDS);
        timerRegistered = new HashMap<>();
        totalEvents = 0;
        processedCount = 0;
        emitCount = 0;

        // >>>>>> ML-TUNER 新增：初始化并注册 Gauge 指标
        l3HitRate = 0.0;
        l3Occupancy = 0.0;

        getRuntimeContext().getMetricGroup()
                .gauge("l3_hit_rate", () -> l3HitRate);
        getRuntimeContext().getMetricGroup()
                .gauge("l3_occupancy", () -> l3Occupancy);
        // <<<<<< ML-TUNER 新增

        // 加载静态网格映射
        loadGridMapping();

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        LOG.info("GridAggregationFunction subtask {} 已启动, 输出间隔={}s, L3配置: 容量={}, 窗口={}s, TTL={}s",
                subtaskIndex, EMIT_INTERVAL_SECONDS, L3_MAX_CAPACITY, L3_TIME_WINDOW_SECONDS, L3_TTL_SECONDS);
    }

    /**
     * 加载静态网格映射
     */
    private void loadGridMapping() throws Exception {
        File configFile = new File("config/grid_mapping.pb");
        if (!configFile.exists()) {
            LOG.warn("grid_mapping.pb 不存在，使用默认每网格 20 节点");
            return;
        }

        try (FileInputStream fis = new FileInputStream(configFile)) {
            GridMappingList gridMappingList = GridMappingList.parseFrom(fis);
            for (GridInfo gridInfo : gridMappingList.getGridsList()) {
                l3Cache.loadStaticMapping(gridInfo.getGridId(), gridInfo.getTotalNodeCount());
            }
            LOG.info("加载静态网格映射完成，共 {} 个网格", gridMappingList.getGridsCount());
        }
    }

    @Override
    public void processElement(
            EventFeature feature,
            Context ctx,
            Collector<GridSummary> out) throws Exception {

        totalEvents++;
        String gridId = feature.getGridId();
        long currentTimestamp = feature.getTimestamp();

        // ============================
        // 1. 获取或创建网格状态
        // ============================
        GridState gridState = l3Cache.getOrCreate(gridId);

        // ============================
        // 2. 清理过期节点事件
        // ============================
        l3Cache.evictExpiredEvents(gridId, currentTimestamp);

        // ============================
        // 3. 更新当前节点事件（只更新状态，不输出）
        // ============================
        gridState.updateNode(
                feature.getNodeid(),
                feature.getPeakAmplitude(),
                feature.getRmsEnergy(),
                currentTimestamp
        );

        // ============================
        // 4. 注册定时器（每个网格只注册一次，后续由 Timer 自动续期）
        // ============================
        if (!timerRegistered.containsKey(gridId)) {
            // 将事件时间戳转为毫秒，对齐到 EMIT_INTERVAL_SECONDS 的整数倍
            long eventTimeMs = currentTimestamp * 1000L;
            long nextEmitMs = eventTimeMs + EMIT_INTERVAL_SECONDS * 1000L;
            ctx.timerService().registerEventTimeTimer(nextEmitMs);
            timerRegistered.put(gridId, true);
        }

        processedCount++;

        // 定期 TTL 清理 + 日志统计 + Gauge 更新
        if (processedCount % TTL_CLEANUP_INTERVAL == 0) {
            l3Cache.ttlCleanup(currentTimestamp);

            // >>>>>> ML-TUNER 新增：更新 Gauge 指标值
            l3HitRate = l3Cache.getHitRate();
            l3Occupancy = l3Cache.getOccupancy();
            // <<<<<< ML-TUNER 新增

            LOG.info("L3缓存统计: {}, 已输出 {} 条 GridSummary", l3Cache.getStats(), emitCount);
        }
    }

    /**
     * 定时触发：输出网格聚合结果
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<GridSummary> out) throws Exception {
        String gridId = ctx.getCurrentKey();

        // 获取网格状态
        GridState gridState = l3Cache.get(gridId);
        if (gridState == null) {
            timerRegistered.remove(gridId);
            return;
        }

        // 清理过期节点
        long currentTimestampSec = timestamp / 1000L;
        gridState.evictExpired(currentTimestampSec, L3_TIME_WINDOW_SECONDS);

        // 只有存在活跃节点时才输出
        if (gridState.getActiveNodeCount() > 0) {
            GridSummary summary = GridSummary.newBuilder()
                    .setGridId(gridId)
                    .setActiveNodeCount(gridState.getActiveNodeCount())
                    .setTotalNodeCount(gridState.getTotalNodeCount())
                    .setAvgPeakAmplitude(gridState.getAvgPeakAmplitude())
                    .setTriggerRate(gridState.getTriggerRate())
                    .setLastUpdateTimestamp(currentTimestampSec)
                    .build();

            out.collect(summary);
            emitCount++;

            // 注册下一次 Timer（续期）
            ctx.timerService().registerEventTimeTimer(timestamp + EMIT_INTERVAL_SECONDS * 1000L);
        } else {
            // 没有活跃节点，不再续期
            timerRegistered.remove(gridId);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("GridAggregationFunction 关闭, 最终统计: 总事件={}, 输出GridSummary={}, L3缓存: {}",
                totalEvents, emitCount, l3Cache != null ? l3Cache.getStats() : "null");
        super.close();
    }
}
