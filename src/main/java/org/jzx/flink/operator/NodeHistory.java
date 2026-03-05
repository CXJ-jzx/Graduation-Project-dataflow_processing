package org.jzx.flink.operator;

import java.util.LinkedList;

/**
 * 节点历史事件记录（L2 缓存中的值对象）
 *
 * 维护单个节点最近 60 秒内的地震事件统计信息
 */
public class NodeHistory {

    // 历史事件的时间戳列表（用于按时间清理过期事件）
    private final LinkedList<EventSnapshot> recentEvents;

    // 累积统计量
    private double sumPeakAmplitude;    // 峰值振幅累加值
    private double sumRmsEnergy;        // RMS 能量累加值
    private int accessCount;            // 总访问次数（用于 LRU-K）
    private long lastAccessTime;        // 最后访问时间戳（秒级）

    public NodeHistory() {
        this.recentEvents = new LinkedList<>();
        this.sumPeakAmplitude = 0;
        this.sumRmsEnergy = 0;
        this.accessCount = 0;
        this.lastAccessTime = 0;
    }

    /**
     * 添加一个事件快照
     */
    public void addEvent(long timestamp, int peakAmplitude, double rmsEnergy) {
        recentEvents.addLast(new EventSnapshot(timestamp, peakAmplitude, rmsEnergy));
        sumPeakAmplitude += peakAmplitude;
        sumRmsEnergy += rmsEnergy;
        accessCount++;
        lastAccessTime = timestamp;
    }

    /**
     * 清理超过 timeWindowSeconds 秒的旧事件
     */
    public void evictExpired(long currentTimestamp, int timeWindowSeconds) {
        long threshold = currentTimestamp - timeWindowSeconds;
        while (!recentEvents.isEmpty() && recentEvents.peekFirst().timestamp < threshold) {
            EventSnapshot removed = recentEvents.pollFirst();
            sumPeakAmplitude -= removed.peakAmplitude;
            sumRmsEnergy -= removed.rmsEnergy;
        }
    }

    /**
     * 获取有效事件数量
     */
    public int getEventCount() {
        return recentEvents.size();
    }

    /**
     * 获取平均峰值振幅
     */
    public double getAvgPeakAmplitude() {
        return recentEvents.isEmpty() ? 0 : sumPeakAmplitude / recentEvents.size();
    }

    /**
     * 获取平均 RMS 能量
     */
    public double getAvgRmsEnergy() {
        return recentEvents.isEmpty() ? 0 : sumRmsEnergy / recentEvents.size();
    }

    public int getAccessCount() {
        return accessCount;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    /**
     * 单条事件快照（仅保留统计所需字段，不保留完整数据）
     */
    public static class EventSnapshot {
        final long timestamp;
        final int peakAmplitude;
        final double rmsEnergy;

        public EventSnapshot(long timestamp, int peakAmplitude, double rmsEnergy) {
            this.timestamp = timestamp;
            this.peakAmplitude = peakAmplitude;
            this.rmsEnergy = rmsEnergy;
        }
    }
}
