package org.jzx.test;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * V6: 三个 Topic 的吞吐量实时监控
 *
 * 同时监控：
 * - seismic-raw：Producer 写入速率
 * - seismic-node-events：Flink EventFeature 输出速率
 * - seismic-grid-summary：Flink GridSummary 输出速率
 *
 * 输出：
 * - 控制台实时表格（每秒刷新）
 * - CSV 文件（用于画图）
 *
 * 运行前提：Producer 和 Flink 作业已启动
 */
public class V6_ThroughputMonitor {

    private static final String NAMESRV_ADDR = "192.168.56.151:9876";
    private static final int MONITOR_DURATION_SECONDS = 120;

    // 每秒窗口计数器
    private static final AtomicInteger rawWindowCount = new AtomicInteger(0);
    private static final AtomicInteger eventWindowCount = new AtomicInteger(0);
    private static final AtomicInteger gridWindowCount = new AtomicInteger(0);

    // 每秒窗口字节数
    private static final AtomicLong rawWindowBytes = new AtomicLong(0);
    private static final AtomicLong eventWindowBytes = new AtomicLong(0);
    private static final AtomicLong gridWindowBytes = new AtomicLong(0);

    // 全局累计
    private static final AtomicLong rawTotalCount = new AtomicLong(0);
    private static final AtomicLong eventTotalCount = new AtomicLong(0);
    private static final AtomicLong gridTotalCount = new AtomicLong(0);
    private static final AtomicLong rawTotalBytes = new AtomicLong(0);
    private static final AtomicLong eventTotalBytes = new AtomicLong(0);
    private static final AtomicLong gridTotalBytes = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        System.out.println("========== V6: 吞吐量实时监控 ==========");
        System.out.println("监控时长: " + MONITOR_DURATION_SECONDS + " 秒");
        System.out.println("监控 Topic: seismic-raw / seismic-node-events / seismic-grid-summary\n");

        // 准备 CSV
        String csvFile = "throughput_" +
                new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".csv";
        PrintWriter csv = new PrintWriter(new FileWriter(csvFile));
        csv.println("second," +
                "raw_count,raw_bytes," +
                "event_count,event_bytes," +
                "grid_count,grid_bytes," +
                "raw_total,event_total,grid_total");
        csv.flush();

        // ============================
        // 启动三个消费者
        // ============================
        DefaultMQPushConsumer rawConsumer = createConsumer(
                "monitor-raw-group",
                "seismic-raw",
                rawWindowCount, rawWindowBytes,
                rawTotalCount, rawTotalBytes);

        DefaultMQPushConsumer eventConsumer = createConsumer(
                "monitor-event-group",
                "seismic-node-events",
                eventWindowCount, eventWindowBytes,
                eventTotalCount, eventTotalBytes);

        DefaultMQPushConsumer gridConsumer = createConsumer(
                "monitor-grid-group",
                "seismic-grid-summary",
                gridWindowCount, gridWindowBytes,
                gridTotalCount, gridTotalBytes);

        rawConsumer.start();
        eventConsumer.start();
        gridConsumer.start();

        System.out.println("三个消费者已启动，开始监控...\n");

        // 峰值记录
        int peakRaw = 0;
        int peakEvent = 0;
        int peakGrid = 0;

        // 打印初始表头
        printTableHeader();

        // ============================
        // 每秒采集一次
        // ============================
        for (int sec = 1; sec <= MONITOR_DURATION_SECONDS; sec++) {
            Thread.sleep(1000);

            // 读取并重置窗口计数器
            int rawCount   = rawWindowCount.getAndSet(0);
            int eventCount = eventWindowCount.getAndSet(0);
            int gridCount  = gridWindowCount.getAndSet(0);
            long rawBytes   = rawWindowBytes.getAndSet(0);
            long eventBytes = eventWindowBytes.getAndSet(0);
            long gridBytes  = gridWindowBytes.getAndSet(0);

            // 峰值更新
            peakRaw   = Math.max(peakRaw,   rawCount);
            peakEvent = Math.max(peakEvent, eventCount);
            peakGrid  = Math.max(peakGrid,  gridCount);

            // 控制台输出（每行一秒）
            System.out.printf(
                    "%4d | %6d | %7.1f KB | %6d | %7.1f KB | %5d | %6.1f KB" +
                            " | %9d | %9d | %7d%n",
                    sec,
                    rawCount,   rawBytes   / 1024.0,
                    eventCount, eventBytes / 1024.0,
                    gridCount,  gridBytes  / 1024.0,
                    rawTotalCount.get(),
                    eventTotalCount.get(),
                    gridTotalCount.get());

            // 每 20 行重新打印表头，防止滚动后看不清
            if (sec % 20 == 0) {
                printTableHeader();
            }

            // CSV 输出
            csv.printf("%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                    sec,
                    rawCount,   rawBytes,
                    eventCount, eventBytes,
                    gridCount,  gridBytes,
                    rawTotalCount.get(),
                    eventTotalCount.get(),
                    gridTotalCount.get());
            csv.flush();
        }

        // ============================
        // 停止消费者
        // ============================
        rawConsumer.shutdown();
        eventConsumer.shutdown();
        gridConsumer.shutdown();

        // ============================
        // 最终汇总
        // ============================
        printFinalSummary(peakRaw, peakEvent, peakGrid);

        csv.close();
        System.out.println("CSV 已保存至: " + csvFile);
    }

    // ================================================================
    //  创建监控消费者（通用）
    // ================================================================
    private static DefaultMQPushConsumer createConsumer(
            String consumerGroup,
            String topic,
            AtomicInteger windowCount,
            AtomicLong windowBytes,
            AtomicLong totalCount,
            AtomicLong totalBytes) throws Exception {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(topic, "*");
        consumer.setConsumeMessageBatchMaxSize(64);
        consumer.setConsumeThreadMin(2);
        consumer.setConsumeThreadMax(4);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(
                    List<MessageExt> msgs,
                    ConsumeConcurrentlyContext ctx) {

                for (MessageExt msg : msgs) {
                    int bodyLen = (msg.getBody() != null) ? msg.getBody().length : 0;
                    windowCount.incrementAndGet();
                    windowBytes.addAndGet(bodyLen);
                    totalCount.incrementAndGet();
                    totalBytes.addAndGet(bodyLen);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        return consumer;
    }

    // ================================================================
    //  打印表头
    // ================================================================
    private static void printTableHeader() {
        System.out.println();
        System.out.println(
                "─────┬────────┬────────────┬────────┬────────────┬───────┬───────────" +
                        "┬───────────┬───────────┬─────────");
        System.out.println(
                " 秒  │ raw/s  │  raw KB/s  │event/s │ event KB/s │grid/s │ grid KB/s " +
                        "│ raw累计   │ event累计  │ grid累计");
        System.out.println(
                "─────┼────────┼────────────┼────────┼────────────┼───────┼───────────" +
                        "┼───────────┼───────────┼─────────");
    }

    // ================================================================
    //  打印最终汇总
    // ================================================================
    private static void printFinalSummary(int peakRaw, int peakEvent, int peakGrid) {
        long durationSec = MONITOR_DURATION_SECONDS;

        long totalRaw   = rawTotalCount.get();
        long totalEvent = eventTotalCount.get();
        long totalGrid  = gridTotalCount.get();
        long totalRawB  = rawTotalBytes.get();
        long totalEventB = eventTotalBytes.get();
        long totalGridB  = gridTotalBytes.get();

        System.out.println("\n========== 吞吐量汇总 ==========");

        System.out.println("\n[seismic-raw]");
        System.out.printf("  总消息数:   %d 条%n", totalRaw);
        System.out.printf("  总字节数:   %.2f MB%n", totalRawB / 1024.0 / 1024.0);
        System.out.printf("  平均吞吐:   %.1f 条/秒%n", (double) totalRaw / durationSec);
        System.out.printf("  平均带宽:   %.2f KB/s%n", totalRawB / 1024.0 / durationSec);
        System.out.printf("  峰值吞吐:   %d 条/秒%n", peakRaw);
        System.out.printf("  平均消息体: %d 字节%n",
                totalRaw > 0 ? totalRawB / totalRaw : 0);

        System.out.println("\n[seismic-node-events]");
        System.out.printf("  总消息数:   %d 条%n", totalEvent);
        System.out.printf("  总字节数:   %.2f MB%n", totalEventB / 1024.0 / 1024.0);
        System.out.printf("  平均吞吐:   %.1f 条/秒%n", (double) totalEvent / durationSec);
        System.out.printf("  平均带宽:   %.2f KB/s%n", totalEventB / 1024.0 / durationSec);
        System.out.printf("  峰值吞吐:   %d 条/秒%n", peakEvent);
        System.out.printf("  平均消息体: %d 字节%n",
                totalEvent > 0 ? totalEventB / totalEvent : 0);

        System.out.println("\n[seismic-grid-summary]");
        System.out.printf("  总消息数:   %d 条%n", totalGrid);
        System.out.printf("  总字节数:   %.2f MB%n", totalGridB / 1024.0 / 1024.0);
        System.out.printf("  平均吞吐:   %.1f 条/秒%n", (double) totalGrid / durationSec);
        System.out.printf("  平均带宽:   %.2f KB/s%n", totalGridB / 1024.0 / durationSec);
        System.out.printf("  峰值吞吐:   %d 条/秒%n", peakGrid);
        System.out.printf("  平均消息体: %d 字节%n",
                totalGrid > 0 ? totalGridB / totalGrid : 0);

        System.out.println("\n[关键比率]");
        // raw → event 转化率（信号提取过滤掉了多少）
        if (totalRaw > 0) {
            System.out.printf("  raw → event 转化率: %.2f%% (过滤掉 %.2f%% 的纯噪音)%n",
                    (double) totalEvent / totalRaw * 100,
                    (100.0 - (double) totalEvent / totalRaw * 100));
        }
        // 平均每个 event 产生多少 grid summary
        if (totalEvent > 0) {
            System.out.printf("  event → grid 比率:  %.4f (每条 event 对应 %.4f 条 grid)%n",
                    (double) totalGrid / totalEvent,
                    (double) totalGrid / totalEvent);
        }
        // Protobuf 压缩效果（raw 消息体 vs 原始 3048 字节）
        if (totalRaw > 0) {
            long avgRawMsgSize = totalRawB / totalRaw;
            System.out.printf("  Protobuf 压缩比:    %.1f%% (raw 平均 %d 字节 vs 原始 3048 字节)%n",
                    (double) avgRawMsgSize / 3048 * 100, avgRawMsgSize);
            if (avgRawMsgSize > 3048) {
                System.out.println("  ⚠ 消息体大于原始数据块！建议将 proto 中 int32 改为 sint32");
            } else {
                System.out.println("  ✓ Protobuf 编码有效压缩了消息体");
            }
        }
    }
}
