package org.jzx.flink.operator;

import java.util.*;

/**
 * L2 缓存 — LFU 策略实现（对比实验 S3）
 *
 * 淘汰规则：严格按访问频次排序，频次最低的最先被淘汰。
 * 频次相同时，淘汰最早插入的条目。
 * 无衰减机制（经典 LFU 的固有缺陷，用于对比）。
 */
public class LFUCache implements IL2Cache {

    private final int maxCapacity;
    private final int ttlSeconds;
    private final int timeWindowSeconds;
    private final int lruK;

    // 主存储：nodeId → NodeHistory
    private final HashMap<Integer, NodeHistory> cache;

    // 频次记录：nodeId → 访问次数（缓存内部计数，与 NodeHistory.accessCount 独立）
    private final HashMap<Integer, Integer> freqMap;

    // 频次桶：freq → 该频次下的节点集合（LinkedHashSet 保持插入顺序，用于同频次淘汰）
    private final TreeMap<Integer, LinkedHashSet<Integer>> freqBuckets;

    private long hitCount = 0;
    private long missCount = 0;
    private long evictCount = 0;
    private long ttlEvictCount = 0;

    public LFUCache(int maxCapacity, int timeWindowSeconds, int ttlSeconds, int lruK) {
        this.maxCapacity = maxCapacity;
        this.timeWindowSeconds = timeWindowSeconds;
        this.ttlSeconds = ttlSeconds;
        this.lruK = lruK;

        this.cache = new HashMap<>(maxCapacity);
        this.freqMap = new HashMap<>(maxCapacity);
        this.freqBuckets = new TreeMap<>();
    }

    @Override
    public NodeHistory get(int nodeId) {
        NodeHistory history = cache.get(nodeId);
        if (history != null) {
            hitCount++;
            incrementFreq(nodeId);
            return history;
        } else {
            missCount++;
            return null;
        }
    }

    @Override
    public void put(int nodeId, NodeHistory history) {
        if (maxCapacity <= 0) return;

        // 已存在则更新
        if (cache.containsKey(nodeId)) {
            cache.put(nodeId, history);
            incrementFreq(nodeId);
            return;
        }

        // 容量满则淘汰
        if (cache.size() >= maxCapacity) {
            evictLowestFreq();
        }

        // 插入新条目，初始频次 = 1
        cache.put(nodeId, history);
        freqMap.put(nodeId, 1);
        freqBuckets.computeIfAbsent(1, k -> new LinkedHashSet<>()).add(nodeId);
    }

    /**
     * 增加节点的访问频次
     */
    private void incrementFreq(int nodeId) {
        int oldFreq = freqMap.getOrDefault(nodeId, 0);
        int newFreq = oldFreq + 1;
        freqMap.put(nodeId, newFreq);

        // 从旧频次桶移除
        if (oldFreq > 0) {
            LinkedHashSet<Integer> oldBucket = freqBuckets.get(oldFreq);
            if (oldBucket != null) {
                oldBucket.remove(nodeId);
                if (oldBucket.isEmpty()) {
                    freqBuckets.remove(oldFreq);
                }
            }
        }

        // 加入新频次桶
        freqBuckets.computeIfAbsent(newFreq, k -> new LinkedHashSet<>()).add(nodeId);
    }

    /**
     * 淘汰频次最低的条目（同频次中淘汰最早插入的）
     */
    private void evictLowestFreq() {
        if (freqBuckets.isEmpty()) return;

        Map.Entry<Integer, LinkedHashSet<Integer>> lowestEntry = freqBuckets.firstEntry();
        LinkedHashSet<Integer> bucket = lowestEntry.getValue();

        // 取 LinkedHashSet 的第一个元素（最早插入的）
        Iterator<Integer> it = bucket.iterator();
        int victimId = it.next();
        it.remove();

        if (bucket.isEmpty()) {
            freqBuckets.remove(lowestEntry.getKey());
        }

        cache.remove(victimId);
        freqMap.remove(victimId);
        evictCount++;
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
                int nodeId = entry.getKey();
                it.remove();

                // 同步清理频次结构
                int freq = freqMap.getOrDefault(nodeId, 0);
                freqMap.remove(nodeId);
                if (freq > 0) {
                    LinkedHashSet<Integer> bucket = freqBuckets.get(freq);
                    if (bucket != null) {
                        bucket.remove(nodeId);
                        if (bucket.isEmpty()) {
                            freqBuckets.remove(freq);
                        }
                    }
                }

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
        return String.format("strategy=LFU, size=%d, hit=%d, miss=%d, hitRate=%.2f%%, evict=%d, ttlEvict=%d",
                cache.size(), hitCount, missCount, hitRate, evictCount, ttlEvictCount);
    }
}
