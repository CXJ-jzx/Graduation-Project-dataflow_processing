package org.jzx.flink.operator;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.jzx.proto.EventFeatureProto.EventFeature;
import org.jzx.proto.NodeConfigProto.NodeMapping;
import org.jzx.proto.NodeConfigProto.NodeInfo;
import org.jzx.proto.SeismicEventProto.SeismicEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;

public class FeatureExtractionFunction
        extends KeyedProcessFunction<Integer, SeismicEvent, EventFeature> {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureExtractionFunction.class);

    // ==================== L2 缓存参数 ====================
    private static final int L2_MAX_CAPACITY = 120;
    private static final int L2_TIME_WINDOW_SECONDS = 60;
    private static final int L2_TTL_SECONDS = 30;
    private static final int L2_LRU_K = 2;

    private static final double FA_LRU_CANDIDATE_RATIO = 0.2;
    private static final long FA_LRU_DECAY_INTERVAL_MS = 30000;

    private static final double ANOMALY_DEVIATION_THRESHOLD = 0.5;

    private static final int TTL_CLEANUP_INTERVAL = 1000;
    private static final int TIER_LOG_INTERVAL = 5000;

    // ==================== 分层配置（普通模式）====================
    private static final int TIER_INIT_THRESHOLD = 200;
    private static final double TIER1_UPPER = 0.10;
    private static final double TIER2_UPPER = 0.30;
    private static final double TIER3_UPPER = 0.60;

    // ==================== 僵化实验全局边界（与 StagnationProducer 完全一致）====================
    // StagnationProducer 对全局 2000 个节点排序后的绝对索引:
    //   T1a: [0, 100)         → Phase2 变低频（僵尸）
    //   T1b: [100, 200)       → Phase2 保持高频（活跃对照组）
    //   T2:  [200, 600)
    //   T3:  [600, 1200)
    //   T4a: [1200, 1300)     → Phase2 变高频（新热点）
    //   T4b: [1300, n)        → Phase2 保持低频
    private static final int GLOBAL_T1A_END = 100;
    private static final int GLOBAL_T1B_END = 200;
    private static final int GLOBAL_T2_END  = 600;
    private static final int GLOBAL_T3_END  = 1200;
    private static final int GLOBAL_T4A_END = 1300;

    // classpath 中的全局节点配置
    private static final String NODE_MAPPING_RESOURCE = "config/node_mapping.pb";

    // ==================== 运行时状态 ====================
    private transient IL2Cache l2Cache;
    private transient String cacheStrategyName;

    private transient boolean stagnationMode;

    private transient long totalEvents;
    private transient long anomalyCount;
    private transient long coldStartCount;
    private transient long processedCount;

    // 普通模式：4层统计 [T1, T2, T3, T4]
    private transient long[] tierHitCount;
    private transient long[] tierMissCount;

    // 僵化模式：6组统计 [T1a, T1b, T2, T3, T4a, T4b]
    private transient long[] stagHitCount;
    private transient long[] stagMissCount;

    // 普通模式使用：本地初始化
    private transient Set<Integer> seenNodeIds;
    private transient boolean tierInitialized;

    // 排序后的节点 ID 列表
    // 僵化模式：全局列表（2000个）从 node_mapping.pb 加载
    // 普通模式：本地列表（~200个）从 processElement 收集
    private transient List<Integer> sortedNodeIds;

    // 普通模式边界（本地排序后索引）
    private transient int tier1EndIdx;
    private transient int tier2EndIdx;
    private transient int tier3EndIdx;

    // 僵化模式边界（全局排序后绝对索引）
    private transient int stagT1aEnd;
    private transient int stagT1bEnd;
    private transient int stagT2End;
    private transient int stagT3End;
    private transient int stagT4aEnd;

    private transient volatile double l2HitRate;
    private transient volatile double l2Occupancy;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        String strategy = "lru";
        stagnationMode = false;

        try {
            ParameterTool params = (ParameterTool)
                    getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            if (params != null) {
                strategy = params.get("cache-strategy", "lru");
                stagnationMode = params.getBoolean("stagnation-mode", false);
            }
        } catch (Exception e) {
            LOG.warn("无法获取全局参数，使用默认缓存策略 LRU");
        }

        switch (strategy.toLowerCase()) {
            case "fifo":
                l2Cache = new FIFOCache(L2_MAX_CAPACITY, L2_TIME_WINDOW_SECONDS,
                        L2_TTL_SECONDS, L2_LRU_K);
                cacheStrategyName = "FIFO";
                break;
            case "lru":
                l2Cache = new L2Cache(L2_MAX_CAPACITY, L2_TIME_WINDOW_SECONDS,
                        L2_TTL_SECONDS, L2_LRU_K);
                cacheStrategyName = "LRU";
                break;
            case "lru-k":
                l2Cache = new LRUKCache(L2_MAX_CAPACITY, L2_TIME_WINDOW_SECONDS,
                        L2_TTL_SECONDS, L2_LRU_K);
                cacheStrategyName = "LRU-K";
                break;
            case "lfu":
                l2Cache = new LFUCache(L2_MAX_CAPACITY, L2_TIME_WINDOW_SECONDS,
                        L2_TTL_SECONDS, L2_LRU_K);
                cacheStrategyName = "LFU";
                break;
            case "w-tinylfu":
                l2Cache = new WTinyLFUCache(L2_MAX_CAPACITY, L2_TIME_WINDOW_SECONDS,
                        L2_TTL_SECONDS, L2_LRU_K);
                cacheStrategyName = "W-TinyLFU";
                break;
            default:
                l2Cache = new L2Cache(L2_MAX_CAPACITY, L2_TIME_WINDOW_SECONDS,
                        L2_TTL_SECONDS, L2_LRU_K);
                cacheStrategyName = "LRU";
                break;
        }

        totalEvents    = 0;
        anomalyCount   = 0;
        coldStartCount = 0;
        processedCount = 0;

        tierHitCount  = new long[4];
        tierMissCount = new long[4];
        stagHitCount  = new long[6];
        stagMissCount = new long[6];

        seenNodeIds     = new HashSet<>();
        tierInitialized = false;
        sortedNodeIds   = null;

        tier1EndIdx = tier2EndIdx = tier3EndIdx = 0;
        stagT1aEnd = stagT1bEnd = stagT2End = stagT3End = stagT4aEnd = 0;

        l2HitRate   = 0.0;
        l2Occupancy = 0.0;
        getRuntimeContext().getMetricGroup().gauge("l2_hit_rate",  () -> l2HitRate);
        getRuntimeContext().getMetricGroup().gauge("l2_occupancy", () -> l2Occupancy);

        int subtask = getRuntimeContext().getIndexOfThisSubtask();

        // ===== 僵化模式：从 classpath 加载全局节点列表 =====
        if (stagnationMode) {
            boolean loaded = initGlobalBounds();
            if (loaded) {
                LOG.info("subtask {} 启动 | 策略={}, 容量={}, stagnationMode=true, " +
                                "全局分层=已加载（{}个节点）",
                        subtask, cacheStrategyName, L2_MAX_CAPACITY,
                        sortedNodeIds.size());
            } else {
                LOG.warn("subtask {} 启动 | 策略={}, stagnationMode=true, " +
                                "全局分层=加载失败，回退到本地初始化（结果可能不准确）",
                        subtask, cacheStrategyName);
            }
        } else {
            LOG.info("subtask {} 启动 | 策略={}, 容量={}, 窗口={}s, TTL={}s, 分层门槛={}",
                    subtask, cacheStrategyName, L2_MAX_CAPACITY,
                    L2_TIME_WINDOW_SECONDS, L2_TTL_SECONDS, TIER_INIT_THRESHOLD);
        }
    }

    /**
     * 从 classpath 加载全局节点列表，初始化僵化模式分层边界
     *
     * 关键：每个 subtask 都加载完整的 2000 个节点列表（很小，几 KB）
     *       用全局排序位置判断分层，避免 keyBy 哈希分区导致的分层错乱
     *
     * @return true=加载成功，false=加载失败
     */
    private boolean initGlobalBounds() {
        int subtask = getRuntimeContext().getIndexOfThisSubtask();

        try (InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(NODE_MAPPING_RESOURCE)) {

            if (is == null) {
                LOG.error("subtask {} classpath 中未找到 {}", subtask, NODE_MAPPING_RESOURCE);
                LOG.error("  请确认: cp config/node_mapping.pb src/main/resources/config/node_mapping.pb");
                return false;
            }

            NodeMapping mapping = NodeMapping.parseFrom(is);
            sortedNodeIds = new ArrayList<>();
            for (NodeInfo node : mapping.getNodesList()) {
                sortedNodeIds.add(node.getNodeid());
            }
            Collections.sort(sortedNodeIds);

            int n = sortedNodeIds.size();
            LOG.info("subtask {} 加载全局节点列表: {} 个节点", subtask, n);

            if (n < GLOBAL_T4A_END) {
                LOG.warn("subtask {} 全局节点数 {} < {}，部分分层可能为空",
                        subtask, n, GLOBAL_T4A_END);
            }

            // 使用与 StagnationProducer 完全相同的绝对索引
            stagT1aEnd = Math.min(GLOBAL_T1A_END, n);   // [0, 100)
            stagT1bEnd = Math.min(GLOBAL_T1B_END, n);   // [100, 200)
            stagT2End  = Math.min(GLOBAL_T2_END,  n);   // [200, 600)
            stagT3End  = Math.min(GLOBAL_T3_END,  n);   // [600, 1200)
            stagT4aEnd = Math.min(GLOBAL_T4A_END, n);   // [1200, 1300)
            // T4b: [1300, n)

            // 普通模式边界也一并初始化
            tier1EndIdx = Math.max((int)(n * TIER1_UPPER), 1);
            tier2EndIdx = Math.max((int)(n * TIER2_UPPER), tier1EndIdx + 1);
            tier3EndIdx = Math.max((int)(n * TIER3_UPPER), tier2EndIdx + 1);

            tierInitialized = true;

            // 释放本地收集集合（不再需要）
            if (seenNodeIds != null) {
                seenNodeIds.clear();
                seenNodeIds = null;
            }

            // 打印边界信息
            LOG.info("  T1a(僵尸):   idx [0, {}), {} 个, nodeId [{}, {}]",
                    stagT1aEnd, stagT1aEnd,
                    sortedNodeIds.get(0),
                    sortedNodeIds.get(Math.max(stagT1aEnd - 1, 0)));
            LOG.info("  T1b(活跃):   idx [{}, {}), {} 个, nodeId [{}, {}]",
                    stagT1aEnd, stagT1bEnd, stagT1bEnd - stagT1aEnd,
                    sortedNodeIds.get(stagT1aEnd),
                    sortedNodeIds.get(Math.max(stagT1bEnd - 1, stagT1aEnd)));
            LOG.info("  T2:          idx [{}, {}), {} 个",
                    stagT1bEnd, stagT2End, stagT2End - stagT1bEnd);
            LOG.info("  T3:          idx [{}, {}), {} 个",
                    stagT2End, stagT3End, stagT3End - stagT2End);

            if (stagT3End < n) {
                LOG.info("  T4a(新热点): idx [{}, {}), {} 个, nodeId [{}, {}]",
                        stagT3End, stagT4aEnd, stagT4aEnd - stagT3End,
                        sortedNodeIds.get(stagT3End),
                        sortedNodeIds.get(Math.max(stagT4aEnd - 1, stagT3End)));
                LOG.info("  T4b(冷节点): idx [{}, {}), {} 个",
                        stagT4aEnd, n, n - stagT4aEnd);
            }

            return true;

        } catch (Exception e) {
            LOG.error("subtask {} 加载全局节点列表异常: {}", subtask, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public void processElement(
            SeismicEvent event,
            Context ctx,
            Collector<EventFeature> out) throws Exception {

        totalEvents++;
        int nodeid           = event.getNodeid();
        long currentTimestamp = event.getTimestamp();

        // 普通模式本地初始化（仅在 stagnationMode=false 或全局加载失败时使用）
        if (!tierInitialized) {
            seenNodeIds.add(nodeid);
            if (seenNodeIds.size() >= TIER_INIT_THRESHOLD) {
                initializeTierBoundsLocal();
            }
        }

        NodeHistory history = l2Cache.get(nodeid);

        // 分层命中统计（初始化后才计入）
        if (tierInitialized) {
            if (stagnationMode) {
                int sg = getStagGroup(nodeid);
                if (history != null) stagHitCount[sg]++;
                else                 stagMissCount[sg]++;
            } else {
                int tier = getTier(nodeid);
                if (history != null) tierHitCount[tier]++;
                else                 tierMissCount[tier]++;
            }
        }

        // 特征计算
        boolean isAbnormal       = false;
        double  avgPeakAmplitude = 0;
        int     recentEventCount = 0;

        if (history == null) {
            history = new NodeHistory();
            coldStartCount++;
        } else {
            l2Cache.evictExpiredEvents(nodeid, currentTimestamp);
            avgPeakAmplitude = history.getAvgPeakAmplitude();
            recentEventCount = history.getEventCount();

            if (recentEventCount > 0 && avgPeakAmplitude > 0) {
                double deviation = Math.abs(event.getPeakAmplitude() - avgPeakAmplitude)
                        / avgPeakAmplitude;
                if (deviation > ANOMALY_DEVIATION_THRESHOLD) {
                    isAbnormal = true;
                    anomalyCount++;
                }
            }
        }

        history.addEvent(currentTimestamp, event.getPeakAmplitude(), event.getRmsEnergy());
        l2Cache.put(nodeid, history);
        avgPeakAmplitude = history.getAvgPeakAmplitude();

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
                .setSourceProcessingTimeMs(event.getSourceProcessingTimeMs())
                .build();

        out.collect(feature);
        processedCount++;

        if (processedCount % TTL_CLEANUP_INTERVAL == 0) {
            l2Cache.ttlCleanup(currentTimestamp);
            l2HitRate   = l2Cache.getHitRate();
            l2Occupancy = l2Cache.getOccupancy();
        }

        if (processedCount % TIER_LOG_INTERVAL == 0) {
            logTierStats();
        }
    }

    // ==================== 本地初始化（普通模式 fallback）====================

    private void initializeTierBoundsLocal() {
        sortedNodeIds = new ArrayList<>(seenNodeIds);
        Collections.sort(sortedNodeIds);

        int n = sortedNodeIds.size();

        tier1EndIdx = Math.max((int)(n * TIER1_UPPER), 1);
        tier2EndIdx = Math.max((int)(n * TIER2_UPPER), tier1EndIdx + 1);
        tier3EndIdx = Math.max((int)(n * TIER3_UPPER), tier2EndIdx + 1);

        tierInitialized = true;
        seenNodeIds.clear();
        seenNodeIds = null;

        int subtask = getRuntimeContext().getIndexOfThisSubtask();
        LOG.info("subtask {} 本地分层初始化: {} 个节点", subtask, n);
        LOG.info("  T1: [{}, {}) {} 个", 0, tier1EndIdx, tier1EndIdx);
        LOG.info("  T2: [{}, {}) {} 个", tier1EndIdx, tier2EndIdx, tier2EndIdx - tier1EndIdx);
        LOG.info("  T3: [{}, {}) {} 个", tier2EndIdx, tier3EndIdx, tier3EndIdx - tier2EndIdx);
        LOG.info("  T4: [{}, {}) {} 个", tier3EndIdx, n, n - tier3EndIdx);
    }

    // ==================== 分层查找 ====================

    /** 普通模式：返回 0~3 */
    private int getTier(int nodeId) {
        if (!tierInitialized || sortedNodeIds == null) return 3;
        int idx = Collections.binarySearch(sortedNodeIds, nodeId);
        if (idx < 0) idx = -(idx + 1);
        if (idx < tier1EndIdx) return 0;
        if (idx < tier2EndIdx) return 1;
        if (idx < tier3EndIdx) return 2;
        return 3;
    }

    /**
     * 僵化模式：返回 0~5
     *
     * 使用全局排序列表做 binarySearch，确保与 StagnationProducer 的分层完全一致
     *
     *   0 = T1a 僵尸（全局 idx [0, 100)，Phase2 变低频）
     *   1 = T1b 活跃（全局 idx [100, 200)，Phase2 保持高频）
     *   2 = T2
     *   3 = T3
     *   4 = T4a 新热点（全局 idx [1200, 1300)，Phase2 变高频）
     *   5 = T4b 冷节点（全局 idx [1300, n)，Phase2 保持低频）
     */
    private int getStagGroup(int nodeId) {
        if (!tierInitialized || sortedNodeIds == null) return 5;
        int idx = Collections.binarySearch(sortedNodeIds, nodeId);
        if (idx < 0) idx = -(idx + 1);
        if (idx < stagT1aEnd) return 0;
        if (idx < stagT1bEnd) return 1;
        if (idx < stagT2End)  return 2;
        if (idx < stagT3End)  return 3;
        if (idx < stagT4aEnd) return 4;
        return 5;
    }

    // ==================== 日志格式化 ====================

    private void logTierStats() {
        int subtask = getRuntimeContext().getIndexOfThisSubtask();
        if (stagnationMode) {
            LOG.info("[{}] [S{}] L2缓存统计: {} | 异常={}, 冷启动={} | " +
                            "T1a(僵尸)={} T1b(活跃)={} T2={} T3={} T4a(热点)={} T4b(冷)={}",
                    cacheStrategyName, subtask,
                    l2Cache.getStats(),
                    anomalyCount, coldStartCount,
                    formatStagRate(0), formatStagRate(1),
                    formatStagRate(2), formatStagRate(3),
                    formatStagRate(4), formatStagRate(5));
        } else {
            LOG.info("[{}] [S{}] L2缓存统计: {} | 异常={}, 冷启动={} | " +
                            "分层命中率: T1={} T2={} T3={} T4={}",
                    cacheStrategyName, subtask,
                    l2Cache.getStats(),
                    anomalyCount, coldStartCount,
                    formatTierRate(0), formatTierRate(1),
                    formatTierRate(2), formatTierRate(3));
        }
    }

    private String formatTierRate(int tier) {
        long hit   = tierHitCount[tier];
        long miss  = tierMissCount[tier];
        long total = hit + miss;
        if (total == 0) return "0.0%(0/0)";
        return String.format("%.1f%%(%d/%d)", (double) hit / total * 100, hit, total);
    }

    private String formatStagRate(int group) {
        long hit   = stagHitCount[group];
        long miss  = stagMissCount[group];
        long total = hit + miss;
        if (total == 0) return "0.0%(0/0)";
        return String.format("%.1f%%(%d/%d)", (double) hit / total * 100, hit, total);
    }

    @Override
    public void close() throws Exception {
        int subtask = getRuntimeContext().getIndexOfThisSubtask();
        if (stagnationMode) {
            LOG.info("[{}] [S{}] subtask {} 关闭 | 总事件={}, 异常={}, 冷启动={} | L2: {} | " +
                            "T1a(僵尸)={} T1b(活跃)={} T2={} T3={} T4a(热点)={} T4b(冷)={}",
                    cacheStrategyName, subtask, subtask,
                    totalEvents, anomalyCount, coldStartCount,
                    l2Cache != null ? l2Cache.getStats() : "null",
                    formatStagRate(0), formatStagRate(1),
                    formatStagRate(2), formatStagRate(3),
                    formatStagRate(4), formatStagRate(5));
        } else {
            LOG.info("[{}] [S{}] subtask {} 关闭 | 总事件={}, 异常={}, 冷启动={} | L2: {} | " +
                            "分层: T1={} T2={} T3={} T4={}",
                    cacheStrategyName, subtask, subtask,
                    totalEvents, anomalyCount, coldStartCount,
                    l2Cache != null ? l2Cache.getStats() : "null",
                    formatTierRate(0), formatTierRate(1),
                    formatTierRate(2), formatTierRate(3));
        }
        super.close();
    }
}
