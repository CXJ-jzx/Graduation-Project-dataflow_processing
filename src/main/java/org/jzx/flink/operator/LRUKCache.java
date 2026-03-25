package org.jzx.flink.operator;

import java.util.*;

/**
 * L2 缓存 — LRU-K 策略实现（K=2）
 *
 * 淘汰依据：每个条目记录最近 K 次访问时间，
 * 淘汰第 K 次访问距今最久的条目。
 * 若访问不足 K 次，优先淘汰。
 *
 * 效果：天然保护高频节点（第K次访问更近），
 *       比纯 LRU 抗扫描能力更强。
 */
public class LRUKCache implements IL2Cache {

    private final int maxCapacity;
    private final int ttlSeconds;
    private final int timeWindowSeconds;
    private final int k;

    private final HashMap<Integer, KEntry> cache;

    private long hitCount = 0;
    private long missCount = 0;
    private long evictCount = 0;
    private long ttlEvictCount = 0;

    private static class KEntry {
        NodeHistory history;
        long[] recentAccesses;  // 最近 K 次访问时间（毫秒），环形缓冲
        int accessCount;
        int ringIdx;

        KEntry(NodeHistory history, int k) {
            this.history = history;
            this.recentAccesses = new long[k];
            this.accessCount = 0;
            this.ringIdx = 0;
            recordAccess();
        }

        void recordAccess() {
            recentAccesses[ringIdx % recentAccesses.length] = System.currentTimeMillis();
            ringIdx++;
            accessCount++;
        }

        /**
         * 获取第 K 次（倒数第K次）访问时间
         * 若访问不足 K 次，返回 0（最容易被淘汰）
         */
        long getKthAccessTime() {
            if (accessCount < recentAccesses.length) {
                return 0;  // 不足 K 次，优先淘汰
            }
            // 环形缓冲中，当前 ringIdx 位置就是最老的一次
            int oldestIdx = ringIdx % recentAccesses.length;
            return recentAccesses[oldestIdx];
        }

        long getLastAccessTime() {
            int lastIdx = (ringIdx - 1 + recentAccesses.length) % recentAccesses.length;
            return recentAccesses[lastIdx];
        }
    }

    public LRUKCache(int maxCapacity, int timeWindowSeconds, int ttlSeconds, int lruK) {
        this.maxCapacity = maxCapacity;
        this.timeWindowSeconds = timeWindowSeconds;
        this.ttlSeconds = ttlSeconds;
        this.k = Math.max(lruK, 1);
        this.cache = new HashMap<>(maxCapacity);
    }

    @Override
    public NodeHistory get(int nodeId) {
        KEntry entry = cache.get(nodeId);
        if (entry != null) {
            entry.recordAccess();
            hitCount++;
            return entry.history;
        } else {
            missCount++;
            return null;
        }
    }

    @Override
    public void put(int nodeId, NodeHistory history) {
        KEntry existing = cache.get(nodeId);
        if (existing != null) {
            existing.history = history;
            existing.recordAccess();
            return;
        }

        if (cache.size() >= maxCapacity) {
            evictOne();
        }

        cache.put(nodeId, new KEntry(history, k));
    }

    /**
     * 淘汰第 K 次访问最久远的条目
     * 时间复杂度 O(n)，n=120 可忽略
     */
    private void evictOne() {
        int victimKey = -1;
        long oldestKth = Long.MAX_VALUE;

        for (Map.Entry<Integer, KEntry> e : cache.entrySet()) {
            long kth = e.getValue().getKthAccessTime();
            if (kth < oldestKth) {
                oldestKth = kth;
                victimKey = e.getKey();
            }
        }

        if (victimKey != -1) {
            cache.remove(victimKey);
            evictCount++;
        }
    }

    @Override
    public void evictExpiredEvents(int nodeId, long currentTimestamp) {
        KEntry entry = cache.get(nodeId);
        if (entry != null) {
            entry.history.evictExpired(currentTimestamp, timeWindowSeconds);
        }
    }

    @Override
    public void ttlCleanup(long currentTimestamp) {
        Iterator<Map.Entry<Integer, KEntry>> it = cache.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, KEntry> entry = it.next();
            long lastAccess = entry.getValue().history.getLastAccessTime();
            if (lastAccess > 0 && (currentTimestamp - lastAccess) > ttlSeconds) {
                it.remove();
                ttlEvictCount++;
            }
        }
    }

    @Override
    public boolean isHotNode(int nodeId) {
        KEntry entry = cache.get(nodeId);
        return entry != null && entry.accessCount >= k;
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
        return String.format("strategy=LRU-K(K=%d), size=%d, hit=%d, miss=%d, hitRate=%.2f%%, evict=%d, ttlEvict=%d",
                k, cache.size(), hitCount, missCount, hitRate, evictCount, ttlEvictCount);
    }
}
