package org.jzx.flink.operator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 网格状态（L3 缓存中的值对象）
 *
 * 维护单个网格内各节点最近一段时间的事件记录
 */
public class GridState {

    // 该网格内每个活跃节点的最近事件信息
    private final Map<Integer, NodeEventInfo> activeNodes;

    // 该网格总节点数（固定值，从 grid_mapping.pb 加载）
    private final int totalNodeCount;

    // 统计
    private long lastUpdateTimestamp;
    private int accessCount;

    public GridState(int totalNodeCount) {
        this.totalNodeCount = totalNodeCount;
        this.activeNodes = new HashMap<>();
        this.lastUpdateTimestamp = 0;
        this.accessCount = 0;
    }

    /**
     * 更新节点事件
     */
    public void updateNode(int nodeid, int peakAmplitude, double rmsEnergy, long timestamp) {
        activeNodes.put(nodeid, new NodeEventInfo(peakAmplitude, rmsEnergy, timestamp));
        lastUpdateTimestamp = timestamp;
        accessCount++;
    }

    /**
     * 清理超时的节点事件
     */
    public void evictExpired(long currentTimestamp, int timeWindowSeconds) {
        long threshold = currentTimestamp - timeWindowSeconds;
        Iterator<Map.Entry<Integer, NodeEventInfo>> it = activeNodes.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, NodeEventInfo> entry = it.next();
            if (entry.getValue().timestamp < threshold) {
                it.remove();
            }
        }
    }

    /**
     * 获取活跃节点数
     */
    public int getActiveNodeCount() {
        return activeNodes.size();
    }

    /**
     * 获取总节点数
     */
    public int getTotalNodeCount() {
        return totalNodeCount;
    }

    /**
     * 计算触发率
     */
    public double getTriggerRate() {
        return totalNodeCount > 0 ? (double) activeNodes.size() / totalNodeCount : 0;
    }

    /**
     * 计算网格内平均峰值振幅
     */
    public double getAvgPeakAmplitude() {
        if (activeNodes.isEmpty()) return 0;
        double sum = 0;
        for (NodeEventInfo info : activeNodes.values()) {
            sum += info.peakAmplitude;
        }
        return sum / activeNodes.size();
    }

    /**
     * 计算网格内平均 RMS 能量
     */
    public double getAvgRmsEnergy() {
        if (activeNodes.isEmpty()) return 0;
        double sum = 0;
        for (NodeEventInfo info : activeNodes.values()) {
            sum += info.rmsEnergy;
        }
        return sum / activeNodes.size();
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public int getAccessCount() {
        return accessCount;
    }

    /**
     * 节点事件信息（仅保留最新一条）
     */
    public static class NodeEventInfo {
        final int peakAmplitude;
        final double rmsEnergy;
        final long timestamp;

        public NodeEventInfo(int peakAmplitude, double rmsEnergy, long timestamp) {
            this.peakAmplitude = peakAmplitude;
            this.rmsEnergy = rmsEnergy;
            this.timestamp = timestamp;
        }
    }
}
