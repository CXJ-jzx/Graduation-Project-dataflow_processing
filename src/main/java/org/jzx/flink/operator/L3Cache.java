package org.jzx.flink.operator;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * L3 缓存（网格级聚合缓存）
 *
 * 特性：
 * 1. 静态网格映射：从 grid_mapping.pb 加载，永不淘汰
 * 2. 动态网格状态：基于 LRU 淘汰
 * 3. 支持 TTL 过期清理
 * 4. 动态容量上限 50 个网格（总共 100 个网格，活跃的约 50 个）
 */
public class L3Cache {

    // 静态网格映射（grid_id → totalNodeCount），永不淘汰
    private final Map<String, Integer> staticGridMapping;

    // 动态网格状态（grid_id → GridState），LRU 淘汰
    private final LinkedHashMap<String, GridState> dynamicCache;

    // 配置参数
    private final int maxCapacity;
    private final int timeWindowSeconds;
    private final int ttlSeconds;

    // 统计
    private long hitCount = 0;
    private long missCount = 0;
    private long evictCount = 0;
    private long ttlEvictCount = 0;

    /**
     * @param maxCapacity       动态缓存最大容量
     * @param timeWindowSeconds 节点事件过期窗口（秒）
     * @param ttlSeconds        网格状态 TTL（秒）
     */
    public L3Cache(int maxCapacity, int timeWindowSeconds, int ttlSeconds) {
        this.maxCapacity = maxCapacity;
        this.timeWindowSeconds = timeWindowSeconds;
        this.ttlSeconds = ttlSeconds;
        this.staticGridMapping = new LinkedHashMap<>();

        // accessOrder=true：按访问顺序排列
        this.dynamicCache = new LinkedHashMap<String, GridState>(maxCapacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, GridState> eldest) {
                if (size() > maxCapacity) {
                    evictCount++;
                    return true;
                }
                return false;
            }
        };
    }

    /**
     * 加载静态网格映射（从 grid_mapping.pb 加载，永不淘汰）
     */
    public void loadStaticMapping(String gridId, int totalNodeCount) {
        staticGridMapping.put(gridId, totalNodeCount);
    }

    /**
     * 获取网格状态
     */
    public GridState get(String gridId) {
        GridState state = dynamicCache.get(gridId);
        if (state != null) {
            hitCount++;
        } else {
            missCount++;
        }
        return state;
    }

    /**
     * 写入/更新网格状态
     */
    public void put(String gridId, GridState state) {
        dynamicCache.put(gridId, state);
    }

    /**
     * 获取网格的总节点数（从静态映射中查询）
     */
    public int getTotalNodeCount(String gridId) {
        return staticGridMapping.getOrDefault(gridId, 20);  // 默认 20
    }

    /**
     * 获取或创建网格状态
     */
    public GridState getOrCreate(String gridId) {
        GridState state = dynamicCache.get(gridId);
        if (state != null) {
            hitCount++;
            return state;
        }

        // 缓存缺失，创建新的 GridState
        missCount++;
        int totalNodeCount = getTotalNodeCount(gridId);
        state = new GridState(totalNodeCount);
        dynamicCache.put(gridId, state);
        return state;
    }

    /**
     * 清理指定网格的过期节点事件
     */
    public void evictExpiredEvents(String gridId, long currentTimestamp) {
        GridState state = dynamicCache.get(gridId);
        if (state != null) {
            state.evictExpired(currentTimestamp, timeWindowSeconds);
        }
    }

    /**
     * TTL 清理（清除超过 ttlSeconds 未更新的网格状态）
     */
    public void ttlCleanup(long currentTimestamp) {
        Iterator<Map.Entry<String, GridState>> it = dynamicCache.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, GridState> entry = it.next();
            long lastUpdate = entry.getValue().getLastUpdateTimestamp();
            if (lastUpdate > 0 && (currentTimestamp - lastUpdate) > ttlSeconds) {
                it.remove();
                ttlEvictCount++;
            }
        }
    }

    /**
     * 获取动态缓存大小
     */
    public int size() {
        return dynamicCache.size();
    }

    /**
     * 获取静态映射大小
     */
    public int staticSize() {
        return staticGridMapping.size();
    }

    // >>>>>> ML-TUNER 新增：供 Flink Gauge 指标查询

    /**
     * 获取缓存命中率 [0.0, 1.0]
     * ML Tuner 通过 Flink REST API 查询此指标
     */
    public double getHitRate() {
        long total = hitCount + missCount;
        return total > 0 ? (double) hitCount / total : 0.0;
    }

    /**
     * 获取缓存占用率 [0.0, 1.0]
     * ML Tuner 通过 Flink REST API 查询此指标
     */
    public double getOccupancy() {
        return maxCapacity > 0 ? (double) dynamicCache.size() / maxCapacity : 0.0;
    }

    // <<<<<< ML-TUNER 新增

    /**
     * 获取统计信息
     */
    public String getStats() {
        long total = hitCount + missCount;
        double hitRate = total > 0 ? (double) hitCount / total * 100 : 0;
        return String.format("dynamicSize=%d, staticSize=%d, hit=%d, miss=%d, hitRate=%.2f%%, lruEvict=%d, ttlEvict=%d",
                dynamicCache.size(), staticGridMapping.size(),
                hitCount, missCount, hitRate, evictCount, ttlEvictCount);
    }
}
