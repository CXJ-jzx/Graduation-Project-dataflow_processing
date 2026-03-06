package org.jzx.test;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.jzx.proto.EventFeatureProto.EventFeature;
import org.jzx.proto.GridSummaryProto.GridSummary;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * V5: 端到端延迟测量
 *
 * 测量方式：
 * RocketMQ 消息自带 bornTimestamp（Producer 发送时间戳），
 * 在消费 Flink 输出 Topic 时，用当前时间 - bornTimestamp 近似端到端延迟。
 *
 * 注意：这个延迟包含了 Producer→RocketMQ→Flink处理→RocketMQ→Consumer 的全路径，
 * 比纯 Flink 处理延迟要大，但更贴近论文中"端到端延迟"的定义。
 *
 * 为了更精确测量 Flink 处理延迟：
 * 用 seismic-raw 消息的 bornTimestamp 作为注入时间，
 * 用 seismic-node-events 消息的 bornTimestamp 作为输出时间，
 * 差值 = Flink 处理延迟 + Sink 写入延迟
 *
 * 这里采用方式2：对比两个 Topic 中同一 nodeid+timestamp 的消息时间差
 */
public class V5_EndToEndLatencyTest {

    private static final String NAMESRV_ADDR = "192.168.56.151:9876";
    private static final int MEASUREMENT_SECONDS = 60;

    // 存储 seismic-raw 中消息的注入时间
    // Key: "nodeid_timestamp" → Value: RocketMQ bornTimestamp (ms)
    private static final Map<String, Long> rawIngestTime =
            Collections.synchronizedMap(new LinkedHashMap<String, Long>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                    return size() > 100000;  // 限制内存
                }
            });

    // 延迟样本
    private static final List<Long> latencySamples =
            Collections.synchronizedList(new ArrayList<>());

    // 直接延迟（消费时间 - bornTimestamp）
    private static final List<Long> directLatencySamples =
            Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) throws Exception {
        System.out.println("========== V5: 端到端延迟测量 ==========");
        System.out.println("测量时长: " + MEASUREMENT_SECONDS + " 秒\n");

        // 准备 CSV
        String csvFile = "latency_" +
                new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".csv";
        PrintWriter csv = new PrintWriter(new FileWriter(csvFile));
        csv.println("second,sample_count,avg_latency_ms,p50_ms,p95_ms,p99_ms,max_ms,direct_avg_ms");
        csv.flush();

        CountDownLatch done = new CountDownLatch(1);

        // ============================
        // 消费者1：seismic-raw（记录注入时间）
        // ============================
        DefaultMQPushConsumer rawConsumer = new DefaultMQPushConsumer("verify-latency-raw-group");
        rawConsumer.setNamesrvAddr(NAMESRV_ADDR);
        rawConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        rawConsumer.subscribe("seismic-raw", "*");
        rawConsumer.setConsumeMessageBatchMaxSize(64);

        rawConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext ctx) {
                for (MessageExt msg : msgs) {
                    try {
                        org.jzx.proto.SeismicRecordProto.SeismicRecord record =
                                org.jzx.proto.SeismicRecordProto.SeismicRecord.parseFrom(msg.getBody());

                        String key = record.getNodeid() + "_" + record.getTimestamp();
                        rawIngestTime.put(key, msg.getBornTimestamp());

                    } catch (Exception e) {
                        // 忽略
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // ============================
        // 消费者2：seismic-node-events（计算延迟）
        // ============================
        DefaultMQPushConsumer eventConsumer = new DefaultMQPushConsumer("verify-latency-event-group");
        eventConsumer.setNamesrvAddr(NAMESRV_ADDR);
        eventConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        eventConsumer.subscribe("seismic-node-events", "*");
        eventConsumer.setConsumeMessageBatchMaxSize(64);

        final AtomicInteger matchCount = new AtomicInteger(0);

        eventConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext ctx) {
                for (MessageExt msg : msgs) {
                    long consumeTime = System.currentTimeMillis();

                    // 直接延迟
                    long directLatency = consumeTime - msg.getBornTimestamp();
                    directLatencySamples.add(directLatency);

                    try {
                        EventFeature feature = EventFeature.parseFrom(msg.getBody());

                        // 匹配 seismic-raw 中的注入时间
                        String key = feature.getNodeid() + "_" + feature.getTimestamp();
                        Long ingestTime = rawIngestTime.get(key);

                        if (ingestTime != null) {
                            long e2eLatency = msg.getBornTimestamp() - ingestTime;
                            // e2eLatency = Flink Sink bornTimestamp - Producer bornTimestamp
                            // 即 RocketMQ入队 → Flink消费处理 → RocketMQ出队 的总时间
                            latencySamples.add(e2eLatency);
                            matchCount.incrementAndGet();
                        }

                    } catch (Exception e) {
                        // 忽略
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        rawConsumer.start();
        System.out.println("raw 消费者已启动，等待 10 秒建立缓存...");
        Thread.sleep(10000);  // 等 10 秒

        eventConsumer.start();
        System.out.println("event 消费者已启动，开始匹配...");
        System.out.println("消费者已启动，开始采集延迟数据...\n");
        System.out.println("秒数 | 匹配数 | 平均延迟 | P50  | P95  | P99  | 最大");
        System.out.println("─────┼───────┼─────────┼──────┼──────┼──────┼──────");

        // 每秒输出统计
        for (int sec = 1; sec <= MEASUREMENT_SECONDS; sec++) {
            Thread.sleep(1000);

            if (!latencySamples.isEmpty()) {
                List<Long> snapshot;
                synchronized (latencySamples) {
                    snapshot = new ArrayList<>(latencySamples);
                }
                Collections.sort(snapshot);

                int n = snapshot.size();
                long avg = snapshot.stream().mapToLong(Long::longValue).sum() / n;
                long p50 = snapshot.get((int) (n * 0.50));
                long p95 = snapshot.get((int) (n * 0.95));
                long p99 = snapshot.get(Math.min((int) (n * 0.99), n - 1));
                long max = snapshot.get(n - 1);

                // 直接延迟均值
                long directAvg = 0;
                if (!directLatencySamples.isEmpty()) {
                    synchronized (directLatencySamples) {
                        directAvg = directLatencySamples.stream()
                                .mapToLong(Long::longValue).sum() / directLatencySamples.size();
                    }
                }

                System.out.printf("%4d | %5d | %5d ms | %4d | %4d | %4d | %4d\n",
                        sec, matchCount.get(), avg, p50, p95, p99, max);

                csv.printf("%d,%d,%d,%d,%d,%d,%d,%d\n",
                        sec, n, avg, p50, p95, p99, max, directAvg);
                csv.flush();
            } else {
                System.out.printf("%4d | %5d | (等待数据...)\n", sec, matchCount.get());
            }
        }

        rawConsumer.shutdown();
        eventConsumer.shutdown();

        // 最终汇总
        printLatencySummary();

        csv.close();
        System.out.println("CSV 已保存: " + csvFile);
    }

    private static void printLatencySummary() {
        System.out.println("\n========== 延迟测量最终汇总 ==========");

        if (!latencySamples.isEmpty()) {
            List<Long> sorted;
            synchronized (latencySamples) {
                sorted = new ArrayList<>(latencySamples);
            }
            Collections.sort(sorted);
            int n = sorted.size();

            long avg = sorted.stream().mapToLong(Long::longValue).sum() / n;
            long p50 = sorted.get((int) (n * 0.50));
            long p95 = sorted.get((int) (n * 0.95));
            long p99 = sorted.get(Math.min((int) (n * 0.99), n - 1));
            long min = sorted.get(0);
            long max = sorted.get(n - 1);

            System.out.println("[Flink 处理延迟 = Sink.bornTs - Source.bornTs]");
            System.out.println("  样本数: " + n);
            System.out.printf("  平均: %d ms\n", avg);
            System.out.printf("  P50:  %d ms\n", p50);
            System.out.printf("  P95:  %d ms\n", p95);
            System.out.printf("  P99:  %d ms\n", p99);
            System.out.printf("  Min:  %d ms\n", min);
            System.out.printf("  Max:  %d ms\n", max);
            System.out.println("  目标: ≤ 5000 ms → " + (p95 <= 5000 ? "✓ 达标" : "✗ 未达标"));
        } else {
            System.out.println("  未采集到匹配的延迟样本（可能是 raw 和 events 消息的 timestamp 未匹配上）");
        }

        if (!directLatencySamples.isEmpty()) {
            List<Long> sorted;
            synchronized (directLatencySamples) {
                sorted = new ArrayList<>(directLatencySamples);
            }
            Collections.sort(sorted);
            int n = sorted.size();

            long avg = sorted.stream().mapToLong(Long::longValue).sum() / n;
            long p95 = sorted.get((int) (n * 0.95));

            System.out.println("\n[端到端直接延迟 = Consumer 消费时间 - Sink.bornTs]");
            System.out.println("  样本数: " + n);
            System.out.printf("  平均: %d ms\n", avg);
            System.out.printf("  P95:  %d ms\n", p95);
        }
    }
}
