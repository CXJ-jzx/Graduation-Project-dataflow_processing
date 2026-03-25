package org.jzx.flink.operator;

/**
 * L2 缓存统一接口
 *
 * 所有缓存淘汰策略（FIFO / LRU / LFU / FA-LRU）均实现此接口，
 * 供 FeatureExtractionFunction 通过启动参数切换使用。
 */
public interface IL2Cache {

    /**
     * 获取节点历史（缓存查询）
     * 命中时内部自增 hitCount，未命中时自增 missCount
     *
     * @param nodeId 节点 ID
     * @return NodeHistory 或 null（未命中）
     */
    NodeHistory get(int nodeId);

    /**
     * 写入/更新节点历史
     *
     * @param nodeId  节点 ID
     * @param history 节点历史对象
     */
    void put(int nodeId, NodeHistory history);

    /**
     * 清理指定节点中超过时间窗口的旧事件
     *
     * @param nodeId           节点 ID
     * @param currentTimestamp  当前时间戳（秒）
     */
    void evictExpiredEvents(int nodeId, long currentTimestamp);

    /**
     * TTL 清理：移除超过 ttlSeconds 未访问的节点条目
     *
     * @param currentTimestamp 当前时间戳（秒）
     */
    void ttlCleanup(long currentTimestamp);

    /**
     * 判断是否为热点节点
     *
     * @param nodeId 节点 ID
     * @return true 表示热点
     */
    boolean isHotNode(int nodeId);

    /**
     * 当前缓存条目数量
     */
    int size();

    /**
     * 缓存命中率 [0.0, 1.0]
     */
    double getHitRate();

    /**
     * 缓存占用率 [0.0, 1.0]
     */
    double getOccupancy();

    /**
     * 命中次数
     */
    long getHitCount();

    /**
     * 未命中次数
     */
    long getMissCount();

    /**
     * LRU 淘汰次数
     */
    long getEvictCount();

    /**
     * TTL 淘汰次数
     */
    long getTtlEvictCount();

    /**
     * 统计信息字符串（用于日志）
     */
    String getStats();
}
