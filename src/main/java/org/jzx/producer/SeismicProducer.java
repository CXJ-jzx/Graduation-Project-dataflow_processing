package org.jzx.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.jzx.proto.NodeConfigProto.*;
import org.jzx.proto.SeismicRecordProto.SeismicRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 地震数据生产者 — 模拟 2000 节点 × 1Hz 持续采集
 *
 * 设计要点：
 * 1. 20 个工作线程，每个线程负责 ~100 个节点
 * 2. 异步发送，Semaphore 控制在途消息数
 * 3. 每轮精确控速 1 秒（1Hz 采样率）
 * 4. 文件读完后自动重置，持续循环发送
 * 5. 支持 Ctrl+C 优雅停机
 */
public class SeismicProducer {
    private static final Logger LOG = LoggerFactory.getLogger(SeismicProducer0.class);

    // RocketMQ 配置
    private static final String NAMESRV_ADDR = "192.168.56.151:9876";
    private static final String PRODUCER_GROUP = "seismic-producer-group";
    private static final String TOPIC = "seismic-raw";

    // .seis 文件格式常量
    private static final int FILE_HEADER_SIZE = 1024;
    private static final int BLOCK_SIZE = 3048;
    private static final int METADATA_SIZE = 48;
    private static final int SAMPLES_COUNT = 1000;

    // 元数据字段偏移
    private static final int OFFSET_NODEID = 12;
    private static final int OFFSET_TIMESTAMP = 32;
    private static final int OFFSET_BAT_VOLTAGE = 40;
    private static final int OFFSET_BAT_TEMP = 42;

    // 并发控制
    private static final int THREAD_POOL_SIZE = 20;
    private static final long TARGET_ROUND_MS = 1000;
    private static final int MAX_IN_FLIGHT = 500;

    // 流控信号量
    private static final Semaphore inflightSemaphore = new Semaphore(MAX_IN_FLIGHT);

    // 统计计数器
    private static final AtomicLong totalMessagesSent = new AtomicLong(0);
    private static final AtomicLong totalBytesSent = new AtomicLong(0);
    private static final AtomicLong totalErrors = new AtomicLong(0);

    // 坐标映射
    private static Map<Integer, NodeInfo> nodeMapping = new HashMap<>();

    // 文件读取器映射
    private static Map<Integer, RandomAccessFile> fileReaders = new ConcurrentHashMap<>();

    // 节点分组
    private static List<List<Integer>> nodeGroups;

    // 循环计数
    private static volatile int cycle = 1;

    // 优雅停机标志
    private static volatile boolean running = true;

    public static void main(String[] args) {
        DefaultMQProducer producer = null;
        try {
            LOG.info("========== 启动 SeismicProducer（2000节点 × 1Hz 持续模式）==========");
            LOG.info("提示：按 Ctrl+C 优雅停止");

            // 注册 Shutdown Hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("收到停机信号，正在优雅退出...");
                running = false;
            }));

            // Step 1: 加载坐标映射配置
            loadNodeMapping();

            // Step 2: 初始化 RocketMQ Producer
            producer = initProducer();

            // Step 3: 扫描 dataset 目录，打开所有文件
            List<File> seisFiles = scanAndOpenFiles();
            LOG.info("成功打开 {} 个 .seis 文件，对应 {} 个节点",
                    seisFiles.size(), fileReaders.size());

            // Step 4: 将节点分组分配给线程
            nodeGroups = partitionNodes(new ArrayList<>(fileReaders.keySet()), THREAD_POOL_SIZE);
            LOG.info("节点分组完成：{} 个线程，每组约 {} 个节点",
                    nodeGroups.size(), fileReaders.size() / THREAD_POOL_SIZE);

            // Step 5: 创建线程池
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
            long startTime = System.currentTimeMillis();

            // Step 6: 持续循环发送
            int round = 1;

            while (running) {
                long roundStartTime = System.currentTimeMillis();

                final DefaultMQProducer prod = producer;
                AtomicInteger roundSuccess = new AtomicInteger(0);
                AtomicInteger roundFail = new AtomicInteger(0);

                List<Future<?>> futures = new ArrayList<>();
                for (List<Integer> group : nodeGroups) {
                    futures.add(executor.submit(() -> {
                        for (int nodeid : group) {
                            if (!running) return;
                            RandomAccessFile raf = fileReaders.get(nodeid);
                            if (raf == null) continue;

                            boolean ok = sendOneBlockAsync(nodeid, raf, prod);
                            if (ok) {
                                roundSuccess.incrementAndGet();
                            } else {
                                roundFail.incrementAndGet();
                            }
                        }
                    }));
                }

                // 等待本轮所有线程完成
                for (Future<?> future : futures) {
                    try {
                        future.get(5, TimeUnit.SECONDS);
                    } catch (TimeoutException e) {
                        LOG.warn("第 {} 轮部分线程超时", round);
                    } catch (Exception e) {
                        LOG.error("第 {} 轮执行异常", round, e);
                    }
                }

                int success = roundSuccess.get();

                // 所有节点都读到文件末尾，重置继续
                if (success == 0) {
                    resetAllFiles();
                    LOG.info("========== 循环 {} 完成（共 {} 轮），重置文件，开始循环 {} / 总发送 {} ==========",
                            cycle, round - 1, cycle + 1, totalMessagesSent.get());
                    cycle++;
                    round++;
                    continue;
                }

                long roundDuration = System.currentTimeMillis() - roundStartTime;

                // 每 50 轮或每个循环的前 3 轮打印日志
                if (round % 50 == 0 || round <= 3) {
                    LOG.info("[循环{} | 轮次{}] 成功 {} / 失败 {} / 耗时 {} ms / 在途 {} / 总发送 {}",
                            cycle, round, success, roundFail.get(), roundDuration,
                            MAX_IN_FLIGHT - inflightSemaphore.availablePermits(),
                            totalMessagesSent.get());
                }

                // 精确控速：不足 1 秒则 sleep 补齐
                long sleepTime = TARGET_ROUND_MS - roundDuration;
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                }

                round++;
            }

            // 优雅退出：等待在途消息完成
            LOG.info("等待在途异步消息完成...");
            inflightSemaphore.acquire(MAX_IN_FLIGHT);
            inflightSemaphore.release(MAX_IN_FLIGHT);

            closeAllFiles();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            long endTime = System.currentTimeMillis();
            printStatistics(startTime, endTime, round - 1);

        } catch (Exception e) {
            LOG.error("Producer 运行失败", e);
        } finally {
            closeAllFiles();
            if (producer != null) {
                producer.shutdown();
            }
            LOG.info("========== SeismicProducer 已停止 ==========");
        }
    }

    /**
     * 重置所有文件读取器到数据起始位置
     */
    private static void resetAllFiles() {
        for (Map.Entry<Integer, RandomAccessFile> entry : fileReaders.entrySet()) {
            try {
                entry.getValue().seek(FILE_HEADER_SIZE);
            } catch (Exception e) {
                LOG.error("重置节点 {} 文件指针失败", entry.getKey(), e);
            }
        }
        LOG.info("所有文件指针已重置到起始位置");
    }

    /**
     * 将节点列表均匀分成 N 组
     */
    private static List<List<Integer>> partitionNodes(List<Integer> nodeIds, int groupCount) {
        List<List<Integer>> groups = new ArrayList<>();
        for (int i = 0; i < groupCount; i++) {
            groups.add(new ArrayList<>());
        }
        for (int i = 0; i < nodeIds.size(); i++) {
            groups.get(i % groupCount).add(nodeIds.get(i));
        }
        groups.removeIf(List::isEmpty);
        return groups;
    }

    /**
     * 异步发送单个节点的一个数据块
     */
    private static boolean sendOneBlockAsync(int nodeid, RandomAccessFile raf, DefaultMQProducer producer) {
        try {
            byte[] blockBuffer = new byte[BLOCK_SIZE];

            synchronized (raf) {
                int bytesRead = raf.read(blockBuffer);
                if (bytesRead < BLOCK_SIZE) {
                    return false;
                }
            }

            ByteBuffer metaBuffer = ByteBuffer.wrap(blockBuffer, 0, METADATA_SIZE)
                    .order(ByteOrder.LITTLE_ENDIAN);

            int timestamp = metaBuffer.getInt(OFFSET_TIMESTAMP);
            short batVoltage = metaBuffer.getShort(OFFSET_BAT_VOLTAGE);
            short batTemp = metaBuffer.getShort(OFFSET_BAT_TEMP);

            NodeInfo nodeInfo = nodeMapping.get(nodeid);
            if (nodeInfo == null) {
                totalErrors.incrementAndGet();
                return false;
            }

            int[] samples = new int[SAMPLES_COUNT];
            for (int i = 0; i < SAMPLES_COUNT; i++) {
                int offset = METADATA_SIZE + i * 3;
                int b0 = blockBuffer[offset] & 0xFF;
                int b1 = blockBuffer[offset + 1] & 0xFF;
                int b2 = blockBuffer[offset + 2];
                int value = (b2 << 16) | (b1 << 8) | b0;
                if ((value & 0x800000) != 0) {
                    value |= 0xFF000000;
                }
                samples[i] = value;
            }

            SeismicRecord.Builder recordBuilder = SeismicRecord.newBuilder()
                    .setNodeid(nodeid)
                    .setFixedX(nodeInfo.getFixedX())
                    .setFixedY(nodeInfo.getFixedY())
                    .setGridId(nodeInfo.getGridId())
                    .setTimestamp(timestamp)
                    .setBatVoltage(batVoltage)
                    .setBatTemp(batTemp);

            for (int sample : samples) {
                recordBuilder.addSamples(sample);
            }

            byte[] payload = recordBuilder.build().toByteArray();

            inflightSemaphore.acquire();

            Message msg = new Message(TOPIC, String.valueOf(nodeid), payload);
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    totalMessagesSent.incrementAndGet();
                    totalBytesSent.addAndGet(payload.length);
                    inflightSemaphore.release();
                }

                @Override
                public void onException(Throwable e) {
                    totalErrors.incrementAndGet();
                    inflightSemaphore.release();
                }
            });

            return true;

        } catch (Exception e) {
            totalErrors.incrementAndGet();
            inflightSemaphore.release();
            return false;
        }
    }

    private static void loadNodeMapping() throws Exception {
        LOG.info("加载坐标映射配置...");
        File configFile = new File("config/node_mapping.pb");
        if (!configFile.exists()) {
            throw new RuntimeException("配置文件不存在: config/node_mapping.pb");
        }

        try (FileInputStream fis = new FileInputStream(configFile)) {
            NodeMapping mapping = NodeMapping.parseFrom(fis);
            for (NodeInfo node : mapping.getNodesList()) {
                nodeMapping.put(node.getNodeid(), node);
            }
            LOG.info("加载完成，共 {} 个节点映射", nodeMapping.size());
        }
    }

    private static DefaultMQProducer initProducer() throws Exception {
        LOG.info("初始化 RocketMQ Producer...");
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.setRetryTimesWhenSendFailed(10);
        producer.setRetryTimesWhenSendAsyncFailed(10);
        producer.setSendMsgTimeout(10000);
        producer.setCompressMsgBodyOverHowmuch(4096);
        producer.setMaxMessageSize(4 * 1024 * 1024);
        producer.start();
        LOG.info("RocketMQ Producer 启动成功，NameServer: {}", NAMESRV_ADDR);

        try {
            Message testMsg = new Message(TOPIC, "health-check", "ping".getBytes());
            producer.send(testMsg);
            LOG.info("Topic [{}] 路由验证通过", TOPIC);
        } catch (Exception e) {
            producer.shutdown();
            throw new RuntimeException("Topic [" + TOPIC + "] 不可用", e);
        }
        return producer;
    }

    private static List<File> scanAndOpenFiles() throws Exception {
        File datasetDir = new File("dataset");
        if (!datasetDir.exists() || !datasetDir.isDirectory()) {
            throw new RuntimeException("dataset/ 目录不存在");
        }

        File[] folders = datasetDir.listFiles(File::isDirectory);
        if (folders == null || folders.length == 0) {
            throw new RuntimeException("dataset/ 目录为空");
        }

        List<File> seisFiles = new ArrayList<>();
        for (File folder : folders) {
            File[] files = folder.listFiles((dir, name) -> name.endsWith(".seis"));
            if (files != null && files.length > 0) {
                File seisFile = files[0];
                seisFiles.add(seisFile);

                RandomAccessFile raf = new RandomAccessFile(seisFile, "r");
                raf.seek(FILE_HEADER_SIZE);

                byte[] metadata = new byte[METADATA_SIZE];
                raf.read(metadata);
                ByteBuffer buffer = ByteBuffer.wrap(metadata).order(ByteOrder.LITTLE_ENDIAN);
                int nodeid = buffer.getInt(OFFSET_NODEID);

                raf.seek(FILE_HEADER_SIZE);
                fileReaders.put(nodeid, raf);
            }
        }

        return seisFiles;
    }

    private static void closeAllFiles() {
        for (RandomAccessFile raf : fileReaders.values()) {
            try {
                raf.close();
            } catch (Exception e) {
                // ignore
            }
        }
        fileReaders.clear();
    }

    private static void printStatistics(long startTime, long endTime, int totalRounds) {
        long duration = (endTime - startTime) / 1000;
        long totalMessages = totalMessagesSent.get();
        long totalBytes = totalBytesSent.get();
        long errors = totalErrors.get();

        LOG.info("==================== 最终统计 ====================");
        LOG.info("总循环次数: {}", cycle);
        LOG.info("总轮次: {}", totalRounds);
        LOG.info("总耗时: {} 秒", duration);
        LOG.info("发送消息总数: {}", totalMessages);
        LOG.info("发送字节总数: {} MB", String.format("%.2f", totalBytes / 1024.0 / 1024.0));
        LOG.info("实际吞吐率: {} 条/秒", duration > 0 ? totalMessages / duration : 0);
        LOG.info("目标吞吐率: 2000 条/秒");
        LOG.info("错误数: {} （错误率: {}%）",
                errors, String.format("%.2f", totalMessages > 0 ? errors * 100.0 / (totalMessages + errors) : 0));
        LOG.info("==================================================");
    }
}
