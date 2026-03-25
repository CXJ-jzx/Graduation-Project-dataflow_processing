package org.jzx.flink.operator;

import java.util.*;

/**
 * W-TinyLFU 缓存策略
 *
 * 架构:
 *   Window LRU (windowSize) → 准入过滤(CountMinSketch) → Main SLRU
 *   Main SLRU = Protected(80%) + Probation(20%)
 *
 * 特性:
 *   - Count-Min Sketch 估计访问频率，空间 O(1)
 *   - 定期频率衰减（全部除以2），防止缓存僵化
 *   - Window 区吸收突发访问，避免污染主缓存
 */
public class WTinyLFUCache implements IL2Cache {

    // ==================== 容量配置 ====================
    private final int maxCapacity;
    private final int windowSize;       // Window LRU 容量 (~1% of total)
    private final int mainProtectedSize; // Main Protected 容量 (~80% of main)
    private final int mainProbationSize; // Main Probation 容量 (~20% of main)
    private final int timeWindowSeconds;
    private final int ttlSeconds;

    // ==================== 三段缓存 ====================
    // Window LRU: 新数据先进这里
    private final LinkedHashMap<Integer, NodeHistory> windowCache;

    // Main Protected: 被多次访问的热数据
    private final LinkedHashMap<Integer, NodeHistory> protectedCache;

    // Main Probation: 从 Window 晋升或从 Protected 降级的数据
    private final LinkedHashMap<Integer, NodeHistory> probationCache;

    // ==================== Count-Min Sketch ====================
    private final int sketchWidth;
    private final int sketchDepth;
    private final int[][] sketch;       // 频率估计矩阵
    private int sketchTotalCount;       // 总计数
    private final int sketchResetThreshold; // 衰减阈值 = 10 * maxCapacity

    // ==================== 统计 ====================
    private long hitCount = 0;
    private long missCount = 0;
    private long evictCount = 0;
    private long ttlEvictCount = 0;
    private long admissionRejectCount = 0;

    public WTinyLFUCache(int maxCapacity, int timeWindowSeconds, int ttlSeconds, int lruK) {
        this.maxCapacity = maxCapacity;
        this.timeWindowSeconds = timeWindowSeconds;
        this.ttlSeconds = ttlSeconds;

        // 容量分配: Window 1%, Main 99% (Protected 80%, Probation 20%)
        this.windowSize = Math.max(1, maxCapacity / 100);
        int mainSize = maxCapacity - windowSize;
        this.mainProtectedSize = (int) (mainSize * 0.8);
        this.mainProbationSize = mainSize - mainProtectedSize;

        // Window LRU
        this.windowCache = new LinkedHashMap<>(windowSize, 0.75f, true);

        // Main Protected LRU
        this.protectedCache = new LinkedHashMap<>(mainProtectedSize, 0.75f, true);

        // Main Probation LRU
        this.probationCache = new LinkedHashMap<>(mainProbationSize, 0.75f, true);

        // Count-Min Sketch: 4行 × maxCapacity列
        this.sketchDepth = 4;
        this.sketchWidth = Math.max(64, nextPowerOf2(maxCapacity));
        this.sketch = new int[sketchDepth][sketchWidth];
        this.sketchTotalCount = 0;
        this.sketchResetThreshold = maxCapacity * 10;
    }

    @Override
    public NodeHistory get(int nodeId) {
        // 1. 查 Protected
        NodeHistory history = protectedCache.get(nodeId);
        if (history != null) {
            incrementSketch(nodeId);
            hitCount++;
            return history;
        }

        // 2. 查 Probation → 命中则晋升到 Protected
        history = probationCache.remove(nodeId);
        if (history != null) {
            incrementSketch(nodeId);
            hitCount++;
            promoteToProtected(nodeId, history);
            return history;
        }

        // 3. 查 Window
        history = windowCache.get(nodeId);
        if (history != null) {
            incrementSketch(nodeId);
            hitCount++;
            return history;
        }

        // Miss
        incrementSketch(nodeId);
        missCount++;
        return null;
    }

    @Override
    public void put(int nodeId, NodeHistory history) {
        // 如果已在任一区域，原地更新
        if (protectedCache.containsKey(nodeId)) {
            protectedCache.put(nodeId, history);
            return;
        }
        if (probationCache.containsKey(nodeId)) {
            probationCache.put(nodeId, history);
            return;
        }
        if (windowCache.containsKey(nodeId)) {
            windowCache.put(nodeId, history);
            return;
        }

        // 新数据：放入 Window
        windowCache.put(nodeId, history);

        // 如果 Window 超容量，淘汰最老的到 Probation（经过准入过滤）
        if (windowCache.size() > windowSize) {
            evictFromWindow();
        }
    }

    /**
     * Window 淘汰 → 准入过滤 → Probation
     */
    private void evictFromWindow() {
        // Window LRU 淘汰最老的
        Iterator<Map.Entry<Integer, NodeHistory>> it = windowCache.entrySet().iterator();
        if (!it.hasNext()) return;

        Map.Entry<Integer, NodeHistory> windowVictim = it.next();
        int candidateId = windowVictim.getKey();
        NodeHistory candidateHistory = windowVictim.getValue();
        it.remove();

        // 如果 Probation 未满，直接放入
        if (probationCache.size() < mainProbationSize) {
            probationCache.put(candidateId, candidateHistory);
            return;
        }

        // Probation 已满，比较候选者 vs Probation 中最老的
        Iterator<Map.Entry<Integer, NodeHistory>> probIt =
                probationCache.entrySet().iterator();
        if (!probIt.hasNext()) {
            evictCount++;
            return;
        }

        Map.Entry<Integer, NodeHistory> probVictim = probIt.next();
        int victimId = probVictim.getKey();

        int candidateFreq = estimateFrequency(candidateId);
        int victimFreq = estimateFrequency(victimId);

        if (candidateFreq > victimFreq) {
            // 候选者频率更高，淘汰 Probation 中的
            probIt.remove();
            probationCache.put(candidateId, candidateHistory);
            evictCount++;
        } else {
            // 候选者频率更低，直接丢弃候选者
            admissionRejectCount++;
            evictCount++;
        }
    }

    /**
     * 从 Probation 晋升到 Protected
     */
    private void promoteToProtected(int nodeId, NodeHistory history) {
        protectedCache.put(nodeId, history);

        // Protected 超容量，降级最老的到 Probation
        while (protectedCache.size() > mainProtectedSize) {
            Iterator<Map.Entry<Integer, NodeHistory>> it =
                    protectedCache.entrySet().iterator();
            if (!it.hasNext()) break;

            Map.Entry<Integer, NodeHistory> demoted = it.next();
            it.remove();
            probationCache.put(demoted.getKey(), demoted.getValue());

            // Probation 也可能超容量
            while (probationCache.size() > mainProbationSize) {
                Iterator<Map.Entry<Integer, NodeHistory>> probIt =
                        probationCache.entrySet().iterator();
                if (!probIt.hasNext()) break;
                probIt.next();
                probIt.remove();
                evictCount++;
            }
        }
    }

    // ==================== Count-Min Sketch ====================

    private void incrementSketch(int nodeId) {
        for (int i = 0; i < sketchDepth; i++) {
            int idx = hash(nodeId, i) & (sketchWidth - 1);
            // 饱和计数，最大15（4 bit）
            if (sketch[i][idx] < 15) {
                sketch[i][idx]++;
            }
        }
        sketchTotalCount++;

        // 定期衰减：所有计数器除以2
        if (sketchTotalCount >= sketchResetThreshold) {
            resetSketch();
        }
    }

    private int estimateFrequency(int nodeId) {
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < sketchDepth; i++) {
            int idx = hash(nodeId, i) & (sketchWidth - 1);
            min = Math.min(min, sketch[i][idx]);
        }
        return min;
    }

    private void resetSketch() {
        for (int i = 0; i < sketchDepth; i++) {
            for (int j = 0; j < sketchWidth; j++) {
                sketch[i][j] = sketch[i][j] >> 1; // 除以2
            }
        }
        sketchTotalCount = sketchTotalCount >> 1;
    }

    private int hash(int key, int seed) {
        int h = key * (seed + 1) * 0x9E3779B1;
        h ^= (h >>> 16);
        h *= 0x85EBCA6B;
        h ^= (h >>> 13);
        return h;
    }

    private static int nextPowerOf2(int n) {
        n--;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        return n + 1;
    }

    // ==================== IL2Cache 接口 ====================

    @Override
    public void evictExpiredEvents(int nodeId, long currentTimestamp) {
        NodeHistory h = protectedCache.get(nodeId);
        if (h == null) h = probationCache.get(nodeId);
        if (h == null) h = windowCache.get(nodeId);
        if (h != null) {
            h.evictExpired(currentTimestamp, timeWindowSeconds);
        }
    }

    @Override
    public void ttlCleanup(long currentTimestamp) {
        ttlCleanupMap(windowCache, currentTimestamp);
        ttlCleanupMap(probationCache, currentTimestamp);
        ttlCleanupMap(protectedCache, currentTimestamp);
    }

    private void ttlCleanupMap(LinkedHashMap<Integer, NodeHistory> map,
                               long currentTimestamp) {
        Iterator<Map.Entry<Integer, NodeHistory>> it = map.entrySet().iterator();
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
        return protectedCache.containsKey(nodeId) || estimateFrequency(nodeId) >= 3;
    }

    @Override
    public int size() {
        return windowCache.size() + protectedCache.size() + probationCache.size();
    }

    @Override
    public double getHitRate() {
        long total = hitCount + missCount;
        return total > 0 ? (double) hitCount / total : 0.0;
    }

    @Override
    public double getOccupancy() {
        return maxCapacity > 0 ? (double) size() / maxCapacity : 0.0;
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
        return String.format(
                "strategy=W-TinyLFU, size=%d(W=%d,P=%d,Pr=%d), " +
                        "hit=%d, miss=%d, hitRate=%.2f%%, " +
                        "evict=%d, ttlEvict=%d, rejected=%d",
                size(), windowCache.size(), protectedCache.size(),
                probationCache.size(),
                hitCount, missCount, hitRate,
                evictCount, ttlEvictCount, admissionRejectCount);
    }
}
