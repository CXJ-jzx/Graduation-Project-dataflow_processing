package org.jzx.flink.operator;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Iterator;

/**
 * L2 缓存（节点级历史事件缓存）
 *
 * 特性：
 * 1. 基于 LinkedHashMap 实现 LRU 淘汰
 * 2. 支持 LRU-K（K=2）：需被访问 2 次以上才视为热点
 * 3. 支持 TTL 过期清理
 * 4. 容量上限 1500 个节点
 */
public class L2Cache {

    // LRU 容量
    private final int maxCapacity;

    // TTL（秒）
    private final int ttlSeconds;

    // 时间窗口（秒）
    private final int timeWindowSeconds;

    // LRU-K 参数
    private final int lruK;

    // 底层存储：accessOrder=true 表示按访问顺序排列（LRU）
    private final LinkedHashMap<Integer, NodeHistory> cache;

    // 统计
    private long hitCount = 0;
    private long missCount = 0;
    private long evictCount = 0;
    private long ttlEvictCount = 0;

    /**
     * @param maxCapacity       最大容量（节点数）
     * @param timeWindowSeconds 时间窗口（秒），用于清理旧事件
     * @param ttlSeconds        TTL（秒），超过未访问则清除
     * @param lruK              LRU-K 参数
     */
    public L2Cache(int maxCapacity, int timeWindowSeconds, int ttlSeconds, int lruK) {
        this.maxCapacity = maxCapacity;
        this.timeWindowSeconds = timeWindowSeconds;
        this.ttlSeconds = ttlSeconds;
        this.lruK = lruK;

        // accessOrder=true：按访问顺序排列，最近访问的在末尾
        this.cache = new LinkedHashMap<Integer, NodeHistory>(maxCapacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, NodeHistory> eldest) {
                if (size() > maxCapacity) {
                    evictCount++;
                    return true;
                }
                return false;
            }
        };
    }

    /**
     * 获取节点历史（L2 命中/缺失）
     *
     * @return NodeHistory 或 null（缺失）
     */
    public NodeHistory get(int nodeid) {
        NodeHistory history = cache.get(nodeid);
        if (history != null) {
            hitCount++;
        } else {
            missCount++;
        }
        return history;
    }

    /**
     * 写入/更新节点历史
     */
    public void put(int nodeid, NodeHistory history) {
        cache.put(nodeid, history);
    }

    /**
     * 清理过期事件（在每个节点的 NodeHistory 中清理超时事件）
     */
    public void evictExpiredEvents(int nodeid, long currentTimestamp) {
        NodeHistory history = cache.get(nodeid);
        if (history != null) {
            history.evictExpired(currentTimestamp, timeWindowSeconds);
        }
    }

    /**
     * 定期执行 TTL 清理（清除超过 ttlSeconds 未访问的节点）
     */
    public void ttlCleanup(long currentTimestamp) {
        Iterator<Map.Entry<Integer, NodeHistory>> it = cache.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, NodeHistory> entry = it.next();
            long lastAccess = entry.getValue().getLastAccessTime();
            if (lastAccess > 0 && (currentTimestamp - lastAccess) > ttlSeconds) {
                it.remove();
                ttlEvictCount++;
            }
        }
    }

    /**
     * 判断是否为热点节点（LRU-K）
     * 访问次数 >= K 才视为热点
     */
    public boolean isHotNode(int nodeid) {
        NodeHistory history = cache.get(nodeid);
        return history != null && history.getAccessCount() >= lruK;
    }

    /**
     * 获取缓存大小
     */
    public int size() {
        return cache.size();
    }

    /**
     * 获取统计信息
     */
    public String getStats() {
        long total = hitCount + missCount;
        double hitRate = total > 0 ? (double) hitCount / total * 100 : 0;
        return String.format("size=%d, hit=%d, miss=%d, hitRate=%.2f%%, lruEvict=%d, ttlEvict=%d",
                cache.size(), hitCount, missCount, hitRate, evictCount, ttlEvictCount);
    }
}
