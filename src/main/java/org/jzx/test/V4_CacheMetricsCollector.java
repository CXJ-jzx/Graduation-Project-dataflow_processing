package org.jzx.test;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.jzx.proto.EventFeatureProto.EventFeature;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * V4: 通过消费 seismic-node-events 的输出数据，间接推算缓存效果指标
 *
 * 核心指标：
 * 1. L2 命中率 = recentEventCount > 0 的比例
 * 2. 异常检测率 = isAnomaly = true 的比例
 * 3. 每秒吞吐量
 * 4. 各节点事件频率分布
 *
 * 输出：CSV 文件（用于画图）
 */
public class V4_CacheMetricsCollector {

    private static final String NAMESRV_ADDR = "192.168.56.151:9876";
    private static final String TOPIC = "seismic-node-events";
    private static final String CONSUMER_GROUP = "verify-cache-metrics-group";
    private static final int COLLECTION_DURATION_SECONDS = 120;  // 采集 120 秒

    // 每秒统计窗口
    private static final AtomicInteger windowTotal = new AtomicInteger(0);
    private static final AtomicInteger windowL2Hit = new AtomicInteger(0);
    private static final AtomicInteger windowAnomaly = new AtomicInteger(0);
    private static final AtomicLong windowPeakSum = new AtomicLong(0);

    // 全局统计
    private static final AtomicLong globalTotal = new AtomicLong(0);
    private static final AtomicLong globalL2Hit = new AtomicLong(0);
    private static final AtomicLong globalAnomaly = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        System.out.println("========== V4: 缓存指标实时采集 ==========");
        System.out.println("采集时长: " + COLLECTION_DURATION_SECONDS + " 秒\n");

        // 准备 CSV 输出
        String csvFile = "cache_metrics_" +
                new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".csv";
        PrintWriter csv = new PrintWriter(new FileWriter(csvFile));
        csv.println("timestamp,second,throughput,l2_hit_rate,anomaly_rate,avg_peak_amplitude");
        csv.flush();

        CountDownLatch done = new CountDownLatch(1);

        // 消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(TOPIC, "*");
        consumer.setConsumeMessageBatchMaxSize(64);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext ctx) {
                for (MessageExt msg : msgs) {
                    try {
                        EventFeature feature = EventFeature.parseFrom(msg.getBody());

                        windowTotal.incrementAndGet();
                        globalTotal.incrementAndGet();

                        if (feature.getRecentEventCount() > 0) {
                            windowL2Hit.incrementAndGet();
                            globalL2Hit.incrementAndGet();
                        }

                        if (feature.getIsAnomaly()) {
                            windowAnomaly.incrementAndGet();
                            globalAnomaly.incrementAndGet();
                        }

                        windowPeakSum.addAndGet(feature.getPeakAmplitude());

                    } catch (Exception e) {
                        // 忽略解析失败
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("开始采集...\n");
        System.out.println("秒数 | 吞吐量 | L2命中率 | 异常率 | 平均峰值");
        System.out.println("─────┼────────┼─────────┼───────┼─────────");

        // 每秒采集一次
        long startTime = System.currentTimeMillis();
        for (int sec = 1; sec <= COLLECTION_DURATION_SECONDS; sec++) {
            Thread.sleep(1000);

            // 读取并重置窗口计数器
            int total = windowTotal.getAndSet(0);
            int l2Hit = windowL2Hit.getAndSet(0);
            int anomaly = windowAnomaly.getAndSet(0);
            long peakSum = windowPeakSum.getAndSet(0);

            double l2HitRate = total > 0 ? (double) l2Hit / total * 100 : 0;
            double anomalyRate = total > 0 ? (double) anomaly / total * 100 : 0;
            double avgPeak = total > 0 ? (double) peakSum / total : 0;

            // 控制台输出
            System.out.printf("%4d | %6d | %6.2f%% | %5.2f%% | %9.0f\n",
                    sec, total, l2HitRate, anomalyRate, avgPeak);

            // CSV 输出
            csv.printf("%d,%d,%d,%.4f,%.4f,%.2f\n",
                    System.currentTimeMillis(), sec, total,
                    l2HitRate / 100, anomalyRate / 100, avgPeak);
            csv.flush();
        }

        consumer.shutdown();

        // 全局汇总
        long gTotal = globalTotal.get();
        System.out.println("\n========== 全局统计 ==========");
        System.out.println("总消息数: " + gTotal);
        System.out.printf("平均吞吐: %.1f 条/秒\n",
                (double) gTotal / COLLECTION_DURATION_SECONDS);
        System.out.printf("全局 L2 命中率: %.2f%%\n",
                gTotal > 0 ? (double) globalL2Hit.get() / gTotal * 100 : 0);
        System.out.printf("全局异常率: %.2f%%\n",
                gTotal > 0 ? (double) globalAnomaly.get() / gTotal * 100 : 0);
        System.out.println("CSV 已保存: " + csvFile);

        csv.close();
    }
}
