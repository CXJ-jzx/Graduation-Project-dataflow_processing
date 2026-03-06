package org.jzx.test;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.jzx.proto.EventFeatureProto.EventFeature;
import org.jzx.proto.GridSummaryProto.GridSummary;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * V3: 消费 Flink 输出的两个 Topic，验证：
 *
 * seismic-node-events:
 *   1. EventFeature 反序列化成功
 *   2. 信号特征值合理性（peak_amplitude > 0, rms_energy > 0）
 *   3. 异常检测标注分布
 *   4. L2 缓存效果（recentEventCount > 0 的比例 = L2 命中率间接指标）
 *
 * seismic-grid-summary:
 *   1. GridSummary 反序列化成功
 *   2. trigger_rate 合理性（0~1 之间）
 *   3. total_node_count 与 grid_mapping.pb 一致
 *   4. 网格覆盖率（收到多少个不同的 grid_id）
 *
 * 运行前提：Flink 作业已启动并正在处理数据
 */
public class V3_PipelineOutputVerifier {

    private static final String NAMESRV_ADDR = "192.168.56.151:9876";

    public static void main(String[] args) throws Exception {
        System.out.println("========== V3: Flink 输出验证 ==========\n");

        CountDownLatch latch = new CountDownLatch(2);

        Thread featureThread = new Thread(() -> {
            try {
                verifyEventFeatures();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }, "feature-verifier");

        Thread gridThread = new Thread(() -> {
            try {
                verifyGridSummaries();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }, "grid-verifier");

        featureThread.start();
        gridThread.start();

        latch.await(180, TimeUnit.SECONDS);
        System.out.println("\n========== V3 验证完成 ==========");
    }

    // ================================================================
    //  seismic-node-events 验证
    // ================================================================
    private static void verifyEventFeatures() throws Exception {
        final int TARGET_COUNT = 5000;
        final AtomicInteger total = new AtomicInteger(0);
        final AtomicInteger parseOk = new AtomicInteger(0);
        final AtomicInteger parseFail = new AtomicInteger(0);
        final AtomicInteger anomalyCount = new AtomicInteger(0);
        final AtomicInteger l2HitCount = new AtomicInteger(0);
        final AtomicInteger zeroPeakCount = new AtomicInteger(0);
        final AtomicInteger zeroRmsCount = new AtomicInteger(0);
        final ConcurrentHashMap<Integer, AtomicInteger> nodeDistribution = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, AtomicInteger> gridDistribution = new ConcurrentHashMap<>();
        final List<Integer> peakAmplitudes = Collections.synchronizedList(new ArrayList<>());
        final List<Double> rmsEnergies = Collections.synchronizedList(new ArrayList<>());

        CountDownLatch done = new CountDownLatch(1);

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("verify-event-feature-group");
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("seismic-node-events", "*");
        consumer.setConsumeMessageBatchMaxSize(64);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext ctx) {
                for (MessageExt msg : msgs) {
                    int count = total.incrementAndGet();
                    try {
                        EventFeature feature = EventFeature.parseFrom(msg.getBody());
                        parseOk.incrementAndGet();

                        // 异常标注统计
                        if (feature.getIsAnomaly()) anomalyCount.incrementAndGet();

                        // L2 命中（recentEventCount > 0 说明 L2 有历史数据）
                        if (feature.getRecentEventCount() > 0) l2HitCount.incrementAndGet();

                        // 特征值合理性
                        if (feature.getPeakAmplitude() <= 0) zeroPeakCount.incrementAndGet();
                        if (feature.getRmsEnergy() <= 0) zeroRmsCount.incrementAndGet();

                        // 节点分布
                        nodeDistribution.computeIfAbsent(feature.getNodeid(),
                                k -> new AtomicInteger(0)).incrementAndGet();

                        // 网格分布
                        gridDistribution.computeIfAbsent(feature.getGridId(),
                                k -> new AtomicInteger(0)).incrementAndGet();

                        // 峰值 & RMS
                        peakAmplitudes.add(feature.getPeakAmplitude());
                        rmsEnergies.add(feature.getRmsEnergy());

                        // 前 5 条详细打印
                        if (count <= 5) {
                            System.out.printf("  [EventFeature #%d] node=%d, grid=%s, ts=%d, " +
                                            "peak=%d, rms=%.1f, sigLen=%d, " +
                                            "anomaly=%b, recentCount=%d, avgPeak=%.1f\n",
                                    count, feature.getNodeid(), feature.getGridId(),
                                    feature.getTimestamp(),
                                    feature.getPeakAmplitude(), feature.getRmsEnergy(),
                                    feature.getSignalLength(),
                                    feature.getIsAnomaly(), feature.getRecentEventCount(),
                                    feature.getAvgPeakAmplitude());
                        }

                    } catch (Exception e) {
                        parseFail.incrementAndGet();
                        if (parseFail.get() <= 3) {
                            System.out.println("  ✗ EventFeature 解析失败: " + e.getMessage());
                        }
                    }

                    if (count % 1000 == 0) {
                        System.out.printf("  [EventFeature] 进度: %d/%d\n", count, TARGET_COUNT);
                    }

                    if (count >= TARGET_COUNT) {
                        done.countDown();
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        System.out.println("[EventFeature] 开始消费 seismic-node-events ...");
        consumer.start();
        done.await(120, TimeUnit.SECONDS);
        consumer.shutdown();

        // ================== 结果输出 ==================
        System.out.println("\n──── EventFeature 验证结果 ────");
        System.out.println("总消息: " + total.get());
        System.out.println("解析成功: " + parseOk.get());
        System.out.println("解析失败: " + parseFail.get());

        System.out.println("\n[异常检测统计]");
        System.out.println("  异常标注数: " + anomalyCount.get());
        System.out.printf("  异常率: %.2f%%\n",
                parseOk.get() > 0 ? (double) anomalyCount.get() / parseOk.get() * 100 : 0);

        System.out.println("\n[L2 缓存效果]");
        System.out.println("  L2 命中数（recentEventCount > 0）: " + l2HitCount.get());
        System.out.printf("  L2 命中率（间接）: %.2f%%\n",
                parseOk.get() > 0 ? (double) l2HitCount.get() / parseOk.get() * 100 : 0);

        System.out.println("\n[特征值合理性]");
        System.out.println("  peakAmplitude <= 0: " + zeroPeakCount.get());
        System.out.println("  rmsEnergy <= 0: " + zeroRmsCount.get());

        if (!peakAmplitudes.isEmpty()) {
            IntSummaryStatistics ampStats = peakAmplitudes.stream()
                    .mapToInt(Integer::intValue).summaryStatistics();
            System.out.printf("  峰值振幅: min=%d, max=%d, avg=%.1f\n",
                    ampStats.getMin(), ampStats.getMax(), ampStats.getAverage());

            if (ampStats.getMin() <= 0) {
                System.out.println("  ⚠ 存在 peakAmplitude <= 0 的记录，信号提取可能有问题");
            }
            if (ampStats.getAverage() < 1000) {
                System.out.println("  ⚠ 平均峰值振幅偏低，检查信号段阈值是否合理");
            }
        }

        if (!rmsEnergies.isEmpty()) {
            DoubleSummaryStatistics rmsStats = rmsEnergies.stream()
                    .mapToDouble(Double::doubleValue).summaryStatistics();
            System.out.printf("  RMS 能量: min=%.1f, max=%.1f, avg=%.1f\n",
                    rmsStats.getMin(), rmsStats.getMax(), rmsStats.getAverage());
        }

        System.out.println("\n[覆盖度]");
        System.out.println("  覆盖节点数: " + nodeDistribution.size() + " / 2000");
        System.out.println("  覆盖网格数: " + gridDistribution.size() + " / 100");

        // 节点消息分布
        if (!nodeDistribution.isEmpty()) {
            IntSummaryStatistics nodeStats = nodeDistribution.values().stream()
                    .mapToInt(AtomicInteger::get).summaryStatistics();
            System.out.printf("  每节点消息数: min=%d, max=%d, avg=%.1f\n",
                    nodeStats.getMin(), nodeStats.getMax(), nodeStats.getAverage());
        }

        // 网格消息分布 TOP 3 / BOTTOM 3
        if (gridDistribution.size() > 3) {
            System.out.println("\n  消息最多的网格 TOP 3:");
            gridDistribution.entrySet().stream()
                    .sorted((a, b) -> b.getValue().get() - a.getValue().get())
                    .limit(3)
                    .forEach(e -> System.out.printf("    网格 %s: %d 条\n",
                            e.getKey(), e.getValue().get()));

            System.out.println("  消息最少的网格 BOTTOM 3:");
            gridDistribution.entrySet().stream()
                    .sorted(Comparator.comparingInt(e -> e.getValue().get()))
                    .limit(3)
                    .forEach(e -> System.out.printf("    网格 %s: %d 条\n",
                            e.getKey(), e.getValue().get()));
        }

        boolean featurePass = parseFail.get() == 0
                && parseOk.get() > 0
                && zeroPeakCount.get() == 0;
        System.out.println("\n" + (featurePass ? "✓ EventFeature 验证通过" : "✗ EventFeature 验证失败"));
    }

    // ================================================================
    //  seismic-grid-summary 验证
    // ================================================================
    private static void verifyGridSummaries() throws Exception {
        final int TARGET_COUNT = 1000;
        final AtomicInteger total = new AtomicInteger(0);
        final AtomicInteger parseOk = new AtomicInteger(0);
        final AtomicInteger parseFail = new AtomicInteger(0);
        final AtomicInteger invalidTriggerRate = new AtomicInteger(0);
        final AtomicInteger zeroActiveNode = new AtomicInteger(0);
        final AtomicInteger invalidTotalNode = new AtomicInteger(0);
        final ConcurrentHashMap<String, AtomicInteger> gridDistribution = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, Integer> gridTotalNodeCount = new ConcurrentHashMap<>();
        final List<Double> triggerRates = Collections.synchronizedList(new ArrayList<>());
        final List<Double> avgPeakAmplitudes = Collections.synchronizedList(new ArrayList<>());

        CountDownLatch done = new CountDownLatch(1);

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("verify-grid-summary-group");
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("seismic-grid-summary", "*");
        consumer.setConsumeMessageBatchMaxSize(64);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext ctx) {
                for (MessageExt msg : msgs) {
                    int count = total.incrementAndGet();
                    try {
                        GridSummary summary = GridSummary.parseFrom(msg.getBody());
                        parseOk.incrementAndGet();

                        // 网格分布
                        gridDistribution.computeIfAbsent(summary.getGridId(),
                                k -> new AtomicInteger(0)).incrementAndGet();

                        // totalNodeCount 记录
                        gridTotalNodeCount.put(summary.getGridId(), summary.getTotalNodeCount());

                        // trigger_rate 合理性
                        double rate = summary.getTriggerRate();
                        triggerRates.add(rate);
                        if (rate < 0 || rate > 1.0001) {  // 允许微小浮点误差
                            invalidTriggerRate.incrementAndGet();
                            if (invalidTriggerRate.get() <= 3) {
                                System.out.printf("  ✗ triggerRate 越界: grid=%s, rate=%.4f\n",
                                        summary.getGridId(), rate);
                            }
                        }

                        // activeNodeCount = 0
                        if (summary.getActiveNodeCount() == 0) {
                            zeroActiveNode.incrementAndGet();
                        }

                        // activeNodeCount > totalNodeCount
                        if (summary.getActiveNodeCount() > summary.getTotalNodeCount()) {
                            invalidTotalNode.incrementAndGet();
                            if (invalidTotalNode.get() <= 3) {
                                System.out.printf("  ✗ active > total: grid=%s, %d > %d\n",
                                        summary.getGridId(),
                                        summary.getActiveNodeCount(), summary.getTotalNodeCount());
                            }
                        }

                        // avgPeakAmplitude
                        avgPeakAmplitudes.add(summary.getAvgPeakAmplitude());

                        // 前 5 条详细打印
                        if (count <= 5) {
                            System.out.printf("  [GridSummary #%d] grid=%s, active=%d/%d, " +
                                            "avgPeak=%.1f, triggerRate=%.3f, lastUpdate=%d\n",
                                    count, summary.getGridId(),
                                    summary.getActiveNodeCount(), summary.getTotalNodeCount(),
                                    summary.getAvgPeakAmplitude(), summary.getTriggerRate(),
                                    summary.getLastUpdateTimestamp());
                        }

                    } catch (Exception e) {
                        parseFail.incrementAndGet();
                        if (parseFail.get() <= 3) {
                            System.out.println("  ✗ GridSummary 解析失败: " + e.getMessage());
                        }
                    }

                    if (count % 200 == 0) {
                        System.out.printf("  [GridSummary] 进度: %d/%d\n", count, TARGET_COUNT);
                    }

                    if (count >= TARGET_COUNT) {
                        done.countDown();
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        System.out.println("[GridSummary] 开始消费 seismic-grid-summary ...");
        consumer.start();
        done.await(120, TimeUnit.SECONDS);
        consumer.shutdown();

        // ================== 结果输出 ==================
        System.out.println("\n──── GridSummary 验证结果 ────");
        System.out.println("总消息: " + total.get());
        System.out.println("解析成功: " + parseOk.get());
        System.out.println("解析失败: " + parseFail.get());

        System.out.println("\n[字段合理性]");
        System.out.println("  triggerRate 越界 (< 0 或 > 1): " + invalidTriggerRate.get());
        System.out.println("  activeNodeCount = 0: " + zeroActiveNode.get());
        System.out.println("  activeNodeCount > totalNodeCount: " + invalidTotalNode.get());

        System.out.println("\n[覆盖度]");
        System.out.println("  覆盖网格数: " + gridDistribution.size() + " / 100");

        // totalNodeCount 分布
        if (!gridTotalNodeCount.isEmpty()) {
            IntSummaryStatistics nodeStats = gridTotalNodeCount.values().stream()
                    .mapToInt(Integer::intValue).summaryStatistics();
            System.out.printf("  totalNodeCount: min=%d, max=%d, avg=%.1f (预期约 20)\n",
                    nodeStats.getMin(), nodeStats.getMax(), nodeStats.getAverage());

            if (Math.abs(nodeStats.getAverage() - 20) > 5) {
                System.out.println("  ⚠ totalNodeCount 均值偏离 20 较多，检查 grid_mapping.pb");
            }
        }

        // triggerRate 分布
        if (!triggerRates.isEmpty()) {
            DoubleSummaryStatistics rateStats = triggerRates.stream()
                    .mapToDouble(Double::doubleValue).summaryStatistics();
            System.out.printf("  triggerRate: min=%.3f, max=%.3f, avg=%.3f\n",
                    rateStats.getMin(), rateStats.getMax(), rateStats.getAverage());
        }

        // avgPeakAmplitude 分布
        if (!avgPeakAmplitudes.isEmpty()) {
            DoubleSummaryStatistics ampStats = avgPeakAmplitudes.stream()
                    .mapToDouble(Double::doubleValue).summaryStatistics();
            System.out.printf("  avgPeakAmplitude: min=%.1f, max=%.1f, avg=%.1f\n",
                    ampStats.getMin(), ampStats.getMax(), ampStats.getAverage());
        }

        // 网格消息分布
        if (gridDistribution.size() > 3) {
            System.out.println("\n  消息最多的网格 TOP 3:");
            gridDistribution.entrySet().stream()
                    .sorted((a, b) -> b.getValue().get() - a.getValue().get())
                    .limit(3)
                    .forEach(e -> System.out.printf("    网格 %s: %d 条\n",
                            e.getKey(), e.getValue().get()));

            System.out.println("  消息最少的网格 BOTTOM 3:");
            gridDistribution.entrySet().stream()
                    .sorted(Comparator.comparingInt(e -> e.getValue().get()))
                    .limit(3)
                    .forEach(e -> System.out.printf("    网格 %s: %d 条\n",
                            e.getKey(), e.getValue().get()));
        }

        boolean gridPass = parseFail.get() == 0
                && invalidTriggerRate.get() == 0
                && invalidTotalNode.get() == 0
                && parseOk.get() > 0;
        System.out.println("\n" + (gridPass ? "✓ GridSummary 验证通过" : "✗ GridSummary 验证失败"));
    }
}
