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
 * 高吞吐量地震数据生产者
 * ================================================
 * 与原始 SeismicProducer 的区别：
 *   - 支持命令行指定目标发送速率（records/s）
 *   - 所有节点统一频率，不做差异化分层
 *   - 通过微批次 + 精确限速实现稳定的高吞吐
 *   - 支持多种速率档位，方便测试不同压力
 *
 * 用法：
 *   java -cp xxx.jar org.jzx.producer.HighThroughputSeismicProducer [目标速率]
 *
 * 示例：
 *   java -cp xxx.jar org.jzx.producer.HighThroughputSeismicProducer 5000
 *   java -cp xxx.jar org.jzx.producer.HighThroughputSeismicProducer 10000
 *   java -cp xxx.jar org.jzx.producer.HighThroughputSeismicProducer 20000
 *
 * 不传参数默认 5000 records/s
 * ================================================
 */
public class HighThroughputSeismicProducer {
    private static final Logger LOG = LoggerFactory.getLogger(HighThroughputSeismicProducer.class);

    // ========== RocketMQ 配置 ==========
    private static final String NAMESRV_ADDR = "192.168.56.151:9876";
    private static final String PRODUCER_GROUP = "seismic-producer-group-ht";
    private static final String TOPIC = "seismic-raw";

    // ========== .seis 文件格式常量（与原 Producer 完全一致） ==========
    private static final int FILE_HEADER_SIZE = 1024;
    private static final int BLOCK_SIZE = 3048;
    private static final int METADATA_SIZE = 48;
    private static final int SAMPLES_COUNT = 1000;
    private static final int OFFSET_NODEID = 12;
    private static final int OFFSET_TIMESTAMP = 32;
    private static final int OFFSET_BAT_VOLTAGE = 40;
    private static final int OFFSET_BAT_TEMP = 42;

    // ========== 吞吐控制 ==========
    private static final int THREAD_POOL_SIZE = 20;
    private static final int MAX_IN_FLIGHT = 2000;              // 加大在途消息上限
    private static final int MICRO_BATCH_INTERVAL_MS = 50;      // 微批次间隔 50ms（每秒 20 个微批次）
    private static final int MICRO_BATCHES_PER_SECOND = 1000 / MICRO_BATCH_INTERVAL_MS; // = 20

    private static int targetRatePerSecond = 5000;              // 目标速率，可命令行覆盖

    // ========== 流控 ==========
    private static final Semaphore inflightSemaphore = new Semaphore(MAX_IN_FLIGHT);

    // ========== 统计 ==========
    private static final AtomicLong totalMessagesSent = new AtomicLong(0);
    private static final AtomicLong totalBytesSent = new AtomicLong(0);
    private static final AtomicLong totalErrors = new AtomicLong(0);

    // 每秒实际发送计数（用于实时速率统计）
    private static final AtomicLong currentSecondSent = new AtomicLong(0);
    private static volatile long lastSecondSent = 0;

    // ========== 数据 ==========
    private static final Map<Integer, NodeInfo> nodeMapping = new HashMap<>();
    private static final Map<Integer, RandomAccessFile> fileReaders = new ConcurrentHashMap<>();
    private static List<Integer> allNodeIds = new ArrayList<>();  // 排序后的全部节点ID

    // ========== 控制 ==========
    private static volatile boolean running = true;
    private static volatile int cycle = 1;

    // ================================================================
    // main
    // ================================================================
    public static void main(String[] args) {
        DefaultMQProducer producer = null;
        ScheduledExecutorService statsScheduler = null;

        try {
            // 解析命令行参数
            if (args.length > 0) {
                try {
                    targetRatePerSecond = Integer.parseInt(args[0]);
                } catch (NumberFormatException e) {
                    LOG.warn("无法解析目标速率 '{}', 使用默认值 {}", args[0], targetRatePerSecond);
                }
            }

            LOG.info("==========================================================");
            LOG.info("  高吞吐量 SeismicProducer 启动");
            LOG.info("  目标速率: {} records/s", targetRatePerSecond);
            LOG.info("  微批次间隔: {} ms (每秒 {} 个微批次)", MICRO_BATCH_INTERVAL_MS, MICRO_BATCHES_PER_SECOND);
            LOG.info("  每微批次发送: {} 条", targetRatePerSecond / MICRO_BATCHES_PER_SECOND);
            LOG.info("  最大在途消息: {}", MAX_IN_FLIGHT);
            LOG.info("  工作线程: {}", THREAD_POOL_SIZE);
            LOG.info("  按 Ctrl+C 优雅停止");
            LOG.info("==========================================================");

            // Shutdown Hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("收到停机信号，正在优雅退出...");
                running = false;
            }));

            // 初始化
            loadNodeMapping();
            producer = initProducer();
            List<File> seisFiles = scanAndOpenFiles();
            allNodeIds = new ArrayList<>(fileReaders.keySet());
            Collections.sort(allNodeIds);

            LOG.info("加载 {} 个 .seis 文件，{} 个节点", seisFiles.size(), allNodeIds.size());

            if (allNodeIds.isEmpty()) {
                throw new RuntimeException("没有可用的数据文件");
            }

            // 创建线程池
            ExecutorService sendExecutor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

            // 启动统计打印线程（每 5 秒）
            statsScheduler = Executors.newSingleThreadScheduledExecutor();
            statsScheduler.scheduleAtFixedRate(
                    HighThroughputSeismicProducer::printPeriodicStats,
                    5, 5, TimeUnit.SECONDS
            );

            long startTime = System.currentTimeMillis();

            // ============ 核心发送循环 ============
            runSendLoop(producer, sendExecutor);

            // 优雅退出
            LOG.info("等待在途消息完成...");
            inflightSemaphore.acquire(MAX_IN_FLIGHT);
            inflightSemaphore.release(MAX_IN_FLIGHT);

            sendExecutor.shutdown();
            sendExecutor.awaitTermination(10, TimeUnit.SECONDS);

            long endTime = System.currentTimeMillis();
            printFinalStatistics(startTime, endTime);

        } catch (Exception e) {
            LOG.error("Producer 运行失败", e);
        } finally {
            closeAllFiles();
            if (statsScheduler != null) statsScheduler.shutdownNow();
            if (producer != null) producer.shutdown();
            LOG.info("========== HighThroughputSeismicProducer 已停止 ==========");
        }
    }

    // ================================================================
    // 核心发送循环 — 微批次限速
    // ================================================================
    /**
     * 发送策略：
     * - 将 1 秒分成 20 个微批次（每 50ms 一个）
     * - 每个微批次发送 targetRate / 20 条消息
     * - 从 allNodeIds 中轮询取节点，文件读完自动重置
     * - 用多线程并行发送每个微批次中的消息
     */
    private static void runSendLoop(DefaultMQProducer producer, ExecutorService executor) throws InterruptedException {
        int batchSize = targetRatePerSecond / MICRO_BATCHES_PER_SECOND;
        if (batchSize < 1) batchSize = 1;

        // 节点轮询游标（全局）
        int cursor = 0;
        long microBatchCount = 0;

        // 每秒统计
        long lastStatTime = System.currentTimeMillis();

        LOG.info("开始发送, 每微批次 {} 条, 共 {} 个节点可轮询", batchSize, allNodeIds.size());

        while (running) {
            long batchStart = System.currentTimeMillis();

            // 构建本微批次要发送的节点列表
            List<SendTask> tasks = new ArrayList<>(batchSize);

            for (int i = 0; i < batchSize; i++) {
                if (!running) break;

                int nodeId = allNodeIds.get(cursor % allNodeIds.size());
                RandomAccessFile raf = fileReaders.get(nodeId);

                if (raf == null) {
                    cursor++;
                    continue;
                }

                // 读取一个 block
                byte[] blockData = readOneBlock(nodeId, raf);

                if (blockData == null) {
                    // 该节点文件读完，重置到开头
                    resetSingleFile(nodeId, raf);
                    // 重读
                    blockData = readOneBlock(nodeId, raf);
                    if (blockData == null) {
                        cursor++;
                        continue;
                    }
                }

                tasks.add(new SendTask(nodeId, blockData));
                cursor++;

                // 整体循环检测：如果所有节点都过了一遍
                if (cursor > 0 && cursor % allNodeIds.size() == 0) {
                    cycle++;
                }
            }

            if (tasks.isEmpty()) {
                Thread.sleep(MICRO_BATCH_INTERVAL_MS);
                continue;
            }

            // 将任务分组给线程池并行发送
            int groupCount = Math.min(THREAD_POOL_SIZE, tasks.size());
            List<List<SendTask>> groups = partitionTasks(tasks, groupCount);
            List<Future<?>> futures = new ArrayList<>(groupCount);

            final DefaultMQProducer prod = producer;
            for (List<SendTask> group : groups) {
                futures.add(executor.submit(() -> {
                    for (SendTask task : group) {
                        if (!running) return;
                        sendOneBlockAsync(task.nodeId, task.blockData, prod);
                    }
                }));
            }

            // 等待本批次完成（最多等 2 秒）
            for (Future<?> f : futures) {
                try {
                    f.get(2, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    LOG.warn("微批次 {} 部分任务超时", microBatchCount);
                } catch (Exception e) {
                    LOG.debug("微批次异常: {}", e.getMessage());
                }
            }

            microBatchCount++;

            // 精确限速：补齐到 MICRO_BATCH_INTERVAL_MS
            long elapsed = System.currentTimeMillis() - batchStart;
            long sleepMs = MICRO_BATCH_INTERVAL_MS - elapsed;
            if (sleepMs > 0) {
                Thread.sleep(sleepMs);
            }

            // 每秒更新实时速率
            long now = System.currentTimeMillis();
            if (now - lastStatTime >= 1000) {
                lastSecondSent = currentSecondSent.getAndSet(0);
                lastStatTime = now;
            }
        }
    }

    // ================================================================
    // 数据读取（与原 Producer 一致）
    // ================================================================

    /**
     * 从文件读取一个 block，返回 null 表示文件读完
     */
    private static byte[] readOneBlock(int nodeId, RandomAccessFile raf) {
        try {
            byte[] blockBuffer = new byte[BLOCK_SIZE];
            synchronized (raf) {
                int bytesRead = raf.read(blockBuffer);
                if (bytesRead < BLOCK_SIZE) {
                    return null;
                }
            }
            return blockBuffer;
        } catch (Exception e) {
            LOG.debug("读取节点 {} 数据失败: {}", nodeId, e.getMessage());
            return null;
        }
    }

    /**
     * 重置单个文件到数据起始位置
     */
    private static void resetSingleFile(int nodeId, RandomAccessFile raf) {
        try {
            synchronized (raf) {
                raf.seek(FILE_HEADER_SIZE);
            }
        } catch (Exception e) {
            LOG.error("重置节点 {} 文件指针失败", nodeId, e);
        }
    }

    // ================================================================
    // 异步发送（与原 Producer 数据格式完全一致）
    // ================================================================

    private static void sendOneBlockAsync(int nodeId, byte[] blockData, DefaultMQProducer producer) {
        try {
            ByteBuffer metaBuffer = ByteBuffer.wrap(blockData, 0, METADATA_SIZE)
                    .order(ByteOrder.LITTLE_ENDIAN);

            int timestamp = metaBuffer.getInt(OFFSET_TIMESTAMP);
            short batVoltage = metaBuffer.getShort(OFFSET_BAT_VOLTAGE);
            short batTemp = metaBuffer.getShort(OFFSET_BAT_TEMP);

            NodeInfo nodeInfo = nodeMapping.get(nodeId);
            if (nodeInfo == null) {
                totalErrors.incrementAndGet();
                return;
            }

            // 解析采样数据（24-bit 小端）
            int[] samples = new int[SAMPLES_COUNT];
            for (int i = 0; i < SAMPLES_COUNT; i++) {
                int offset = METADATA_SIZE + i * 3;
                int b0 = blockData[offset] & 0xFF;
                int b1 = blockData[offset + 1] & 0xFF;
                int b2 = blockData[offset + 2];
                int value = (b2 << 16) | (b1 << 8) | b0;
                if ((value & 0x800000) != 0) {
                    value |= 0xFF000000;
                }
                samples[i] = value;
            }

            // 构建 Protobuf
            SeismicRecord.Builder recordBuilder = SeismicRecord.newBuilder()
                    .setNodeid(nodeId)
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

            // 流控
            inflightSemaphore.acquire();

            Message msg = new Message(TOPIC, String.valueOf(nodeId), payload);
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    totalMessagesSent.incrementAndGet();
                    totalBytesSent.addAndGet(payload.length);
                    currentSecondSent.incrementAndGet();
                    inflightSemaphore.release();
                }

                @Override
                public void onException(Throwable e) {
                    totalErrors.incrementAndGet();
                    inflightSemaphore.release();
                }
            });

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            totalErrors.incrementAndGet();
            inflightSemaphore.release();
        }
    }

    // ================================================================
    // 辅助类和方法
    // ================================================================

    /**
     * 发送任务（数据已从文件读出，等待发送）
     */
    private static class SendTask {
        final int nodeId;
        final byte[] blockData;

        SendTask(int nodeId, byte[] blockData) {
            this.nodeId = nodeId;
            this.blockData = blockData;
        }
    }

    /**
     * 均匀分组
     */
    private static <T> List<List<T>> partitionTasks(List<T> items, int groupCount) {
        int actual = Math.min(groupCount, items.size());
        List<List<T>> groups = new ArrayList<>(actual);
        for (int i = 0; i < actual; i++) {
            groups.add(new ArrayList<>());
        }
        for (int i = 0; i < items.size(); i++) {
            groups.get(i % actual).add(items.get(i));
        }
        groups.removeIf(List::isEmpty);
        return groups;
    }

    /**
     * 周期性统计（每 5 秒）
     */
    private static void printPeriodicStats() {
        long total = totalMessagesSent.get();
        long errors = totalErrors.get();
        long realRate = lastSecondSent;
        int inflight = MAX_IN_FLIGHT - inflightSemaphore.availablePermits();

        LOG.info("[统计] 目标={}r/s  实际≈{}r/s  总发送={}  错误={}  在途={}  循环={}",
                targetRatePerSecond, realRate, total, errors, inflight, cycle);
    }

    // ================================================================
    // 初始化（与原 Producer 完全一致，不做任何修改）
    // ================================================================

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
            try { raf.close(); } catch (Exception ignored) {}
        }
        fileReaders.clear();
    }

    private static void printFinalStatistics(long startTime, long endTime) {
        long duration = (endTime - startTime) / 1000;
        long total = totalMessagesSent.get();
        long bytes = totalBytesSent.get();
        long errors = totalErrors.get();

        LOG.info("==================== 最终统计 ====================");
        LOG.info("目标速率: {} records/s", targetRatePerSecond);
        LOG.info("总循环次数: {}", cycle);
        LOG.info("总耗时: {} 秒", duration);
        LOG.info("发送消息总数: {}", total);
        LOG.info("发送字节总数: {:.2f} MB", bytes / 1024.0 / 1024.0);
        LOG.info("实际平均吞吐率: {} 条/秒", duration > 0 ? total / duration : 0);
        LOG.info("错误数: {} (错误率: {:.2f}%)",
                errors, total > 0 ? errors * 100.0 / (total + errors) : 0);
        LOG.info("==================================================");
    }
}
