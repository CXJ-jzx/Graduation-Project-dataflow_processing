package org.jzx.flink.operator;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * L2 缓存 — FIFO 策略实现（对比实验 S1）
 *
 * 淘汰规则：先进先出，最早插入的条目最先被淘汰。
 * get() 不改变条目位置（accessOrder=false）。
 */
public class FIFOCache implements IL2Cache {

    private final int maxCapacity;
    private final int ttlSeconds;
    private final int timeWindowSeconds;
    private final int lruK;

    // accessOrder=false：按插入顺序排列（FIFO）
    private final LinkedHashMap<Integer, NodeHistory> cache;

    private long hitCount = 0;
    private long missCount = 0;
    private long evictCount = 0;
    private long ttlEvictCount = 0;

    public FIFOCache(int maxCapacity, int timeWindowSeconds, int ttlSeconds, int lruK) {
        this.maxCapacity = maxCapacity;
        this.timeWindowSeconds = timeWindowSeconds;
        this.ttlSeconds = ttlSeconds;
        this.lruK = lruK;

        // accessOrder=false：get() 不改变位置，纯插入顺序
        this.cache = new LinkedHashMap<Integer, NodeHistory>(maxCapacity, 0.75f, false) {
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

    @Override
    public NodeHistory get(int nodeId) {
        NodeHistory history = cache.get(nodeId);
        if (history != null) {
            hitCount++;
        } else {
            missCount++;
        }
        return history;
    }

    @Override
    public void put(int nodeId, NodeHistory history) {
        cache.put(nodeId, history);
    }

    @Override
    public void evictExpiredEvents(int nodeId, long currentTimestamp) {
        NodeHistory history = cache.get(nodeId);
        if (history != null) {
            history.evictExpired(currentTimestamp, timeWindowSeconds);
        }
    }

    @Override
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

    @Override
    public boolean isHotNode(int nodeId) {
        NodeHistory history = cache.get(nodeId);
        return history != null && history.getAccessCount() >= lruK;
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public double getHitRate() {
        long total = hitCount + missCount;
        return total > 0 ? (double) hitCount / total : 0.0;
    }

    @Override
    public double getOccupancy() {
        return maxCapacity > 0 ? (double) cache.size() / maxCapacity : 0.0;
    }

    @Override
    public long getHitCount() { return hitCount; }

    @Override
    public long getMissCount() { return missCount; }

    @Override
    public long getEvictCount() { return evictCount; }

    @Override
    public long getTtlEvictCount() { return ttlEvictCount; }

    @Override
    public String getStats() {
        long total = hitCount + missCount;
        double hitRate = total > 0 ? (double) hitCount / total * 100 : 0;
        return String.format("strategy=FIFO, size=%d, hit=%d, miss=%d, hitRate=%.2f%%, evict=%d, ttlEvict=%d",
                cache.size(), hitCount, missCount, hitRate, evictCount, ttlEvictCount);
    }
}
