package org.jzx.test;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.jzx.proto.NodeConfigProto.*;
import org.jzx.proto.SeismicRecordProto.SeismicRecord;

import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * V2: 从真实 RocketMQ seismic-raw Topic 消费消息，验证：
 * 1. Protobuf 反序列化成功率
 * 2. 字段完整性（nodeid / fixed_x / fixed_y / grid_id / samples）
 * 3. 坐标修正正确性（fixed_x/y 与 node_mapping.pb 一致）
 * 4. 采样点数 = 1000
 * 5. 消息体大小分布
 *
 * 运行方式：先启动 Producer 发送数据，然后运行本验证器
 * 注意：使用独立 ConsumerGroup，不影响 Flink 消费
 */
public class V2_RocketMQRawVerifier {

    private static final String NAMESRV_ADDR = "192.168.56.151:9876";
    private static final String TOPIC = "seismic-raw";
    private static final String CONSUMER_GROUP = "seismic-verify-raw-group";  // 独立消费组
    private static final int VERIFY_COUNT = 10000;  // 验证前 10000 条消息

    // 统计
    private static final AtomicInteger totalMessages = new AtomicInteger(0);
    private static final AtomicInteger parseSuccess = new AtomicInteger(0);
    private static final AtomicInteger parseFailure = new AtomicInteger(0);
    private static final AtomicInteger coordMismatch = new AtomicInteger(0);
    private static final AtomicInteger sampleCountError = new AtomicInteger(0);
    private static final AtomicLong totalMessageBytes = new AtomicLong(0);

    // 统计各节点收到的消息数
    private static final ConcurrentHashMap<Integer, AtomicInteger> nodeMessageCount = new ConcurrentHashMap<>();

    // 消息体大小分布
    private static final AtomicInteger sizeUnder1K = new AtomicInteger(0);
    private static final AtomicInteger size1Kto3K = new AtomicInteger(0);
    private static final AtomicInteger size3Kto5K = new AtomicInteger(0);
    private static final AtomicInteger sizeOver5K = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        System.out.println("========== V2: RocketMQ seismic-raw 消息验证 ==========");
        System.out.println("目标: 消费 " + VERIFY_COUNT + " 条消息进行验证\n");

        // 加载坐标映射
        Map<Integer, NodeInfo> nodeMapping = loadNodeMapping();
        System.out.println("[1] 坐标映射加载完成，共 " + nodeMapping.size() + " 个节点");

        // 创建消费者
        CountDownLatch latch = new CountDownLatch(1);

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(TOPIC, "*");
        consumer.setConsumeMessageBatchMaxSize(64);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    int count = totalMessages.incrementAndGet();
                    byte[] body = msg.getBody();
                    totalMessageBytes.addAndGet(body.length);

                    // 消息体大小分布统计
                    if (body.length < 1024) sizeUnder1K.incrementAndGet();
                    else if (body.length < 3072) size1Kto3K.incrementAndGet();
                    else if (body.length < 5120) size3Kto5K.incrementAndGet();
                    else sizeOver5K.incrementAndGet();

                    try {
                        // Protobuf 反序列化
                        SeismicRecord record = SeismicRecord.parseFrom(body);
                        parseSuccess.incrementAndGet();

                        // 统计节点消息分布
                        nodeMessageCount.computeIfAbsent(record.getNodeid(),
                                k -> new AtomicInteger(0)).incrementAndGet();

                        // 验证采样点数
                        if (record.getSamplesCount() != 1000) {
                            sampleCountError.incrementAndGet();
                            if (sampleCountError.get() <= 5) {
                                System.out.printf("  ✗ 节点 %d 采样点数异常: %d\n",
                                        record.getNodeid(), record.getSamplesCount());
                            }
                        }

                        // 验证坐标修正
                        NodeInfo expected = nodeMapping.get(record.getNodeid());
                        if (expected != null) {
                            if (record.getFixedX() != expected.getFixedX()
                                    || record.getFixedY() != expected.getFixedY()
                                    || !record.getGridId().equals(expected.getGridId())) {
                                coordMismatch.incrementAndGet();
                                if (coordMismatch.get() <= 5) {
                                    System.out.printf("  ✗ 节点 %d 坐标不匹配: " +
                                                    "msg(x=%d,y=%d,grid=%s) vs config(x=%d,y=%d,grid=%s)\n",
                                            record.getNodeid(),
                                            record.getFixedX(), record.getFixedY(), record.getGridId(),
                                            expected.getFixedX(), expected.getFixedY(), expected.getGridId());
                                }
                            }
                        }

                        // 前 3 条打印详细信息
                        if (count <= 3) {
                            printRecordDetail(record, body.length);
                        }

                    } catch (Exception e) {
                        parseFailure.incrementAndGet();
                        if (parseFailure.get() <= 5) {
                            System.out.println("  ✗ 反序列化失败: " + e.getMessage());
                        }
                    }

                    // 每 2000 条打印进度
                    if (count % 2000 == 0) {
                        System.out.printf("  进度: %d/%d, 成功率: %.2f%%\n",
                                count, VERIFY_COUNT,
                                (double) parseSuccess.get() / count * 100);
                    }

                    // 达到验证数量后停止
                    if (count >= VERIFY_COUNT) {
                        latch.countDown();
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        System.out.println("[2] 开始消费消息...\n");
        consumer.start();

        // 等待验证完成，最多等 120 秒
        boolean completed = latch.await(120, TimeUnit.SECONDS);
        consumer.shutdown();

        // 打印验证结果
        printResults(completed);
    }

    private static void printRecordDetail(SeismicRecord record, int bodySize) {
        // 分析波形
        int noiseCount = 0, signalCount = 0;
        int maxAbs = 0, negativeCount = 0;
        for (int i = 0; i < record.getSamplesCount(); i++) {
            int v = record.getSamples(i);
            int abs = Math.abs(v);
            if (v < 0) negativeCount++;
            if (abs > maxAbs) maxAbs = abs;
            if (abs <= 9) noiseCount++;
            else if (abs > 100) signalCount++;
        }

        System.out.println("  ┌──────────────────────────────────");
        System.out.printf("  │ nodeid=%d, ts=%d, voltage=%d, temp=%d\n",
                record.getNodeid(), record.getTimestamp(),
                record.getBatVoltage(), record.getBatTemp());
        System.out.printf("  │ coord=(%d,%d), grid=%s\n",
                record.getFixedX(), record.getFixedY(), record.getGridId());
        System.out.printf("  │ 采样点: 1000, 噪音=%d, 信号=%d, 负数=%d, 峰值=%d\n",
                noiseCount, signalCount, negativeCount, maxAbs);
        System.out.printf("  │ 消息体大小: %d 字节 (原始块 3048 字节, 比率 %.1f%%)\n",
                bodySize, (double) bodySize / 3048 * 100);
        System.out.println("  └──────────────────────────────────");
    }

    private static void printResults(boolean completed) {
        System.out.println("\n========== V2 验证结果 ==========");
        System.out.println("完成状态: " + (completed ? "正常完成" : "超时"));
        System.out.println("总消息数: " + totalMessages.get());
        System.out.println("反序列化成功: " + parseSuccess.get());
        System.out.println("反序列化失败: " + parseFailure.get());
        System.out.println("坐标不匹配: " + coordMismatch.get());
        System.out.println("采样点数异常: " + sampleCountError.get());
        System.out.println("覆盖节点数: " + nodeMessageCount.size());

        long totalBytes = totalMessageBytes.get();
        System.out.printf("平均消息体大小: %d 字节\n",
                totalMessages.get() > 0 ? totalBytes / totalMessages.get() : 0);

        System.out.println("\n消息体大小分布:");
        System.out.println("  < 1KB:  " + sizeUnder1K.get());
        System.out.println("  1-3KB:  " + size1Kto3K.get());
        System.out.println("  3-5KB:  " + size3Kto5K.get());
        System.out.println("  > 5KB:  " + sizeOver5K.get());

        // 节点消息分布
        if (!nodeMessageCount.isEmpty()) {
            IntSummaryStatistics stats = nodeMessageCount.values().stream()
                    .mapToInt(AtomicInteger::get)
                    .summaryStatistics();
            System.out.printf("\n节点消息分布: min=%d, max=%d, avg=%.1f\n",
                    stats.getMin(), stats.getMax(), stats.getAverage());
        }

        // 判定结果
        boolean pass = parseFailure.get() == 0
                && coordMismatch.get() == 0
                && sampleCountError.get() == 0;
        System.out.println("\n" + (pass ? "✓ 全部通过" : "✗ 存在错误"));
    }

    private static Map<Integer, NodeInfo> loadNodeMapping() throws Exception {
        Map<Integer, NodeInfo> map = new HashMap<>();
        try (FileInputStream fis = new FileInputStream("config/node_mapping.pb")) {
            NodeMapping mapping = NodeMapping.parseFrom(fis);
            for (NodeInfo node : mapping.getNodesList()) {
                map.put(node.getNodeid(), node);
            }
        }
        return map;
    }
}
