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
 * 地震数据生产者 — 模拟 2000 节点 × 差异化频率持续采集
 *
 * 设计要点：
 * 1. 20 个工作线程，每个线程负责动态分配的节点
 * 2. 异步发送，Semaphore 控制在途消息数
 * 3. 每轮精确控速 1 秒（1Hz 采样率）
 * 4. 文件读完后自动重置，持续循环发送
 * 5. 支持 Ctrl+C 优雅停机
 * 6. 差异化上传频率：T1(1s)/T2(2s)/T3(3s)/T4(5s)
 * 7. 每轮次内 shuffle 打乱发送顺序，T1/T2/T3/T4 交替到达
 */
public class SeismicProducer {
    private static final Logger LOG = LoggerFactory.getLogger(SeismicProducer.class);

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

    // 循环计数
    private static volatile int cycle = 1;

    // 优雅停机标志
    private static volatile boolean running = true;

    // 差异化上传频率映射：nodeId → 上传间隔（秒/轮）
    private static Map<Integer, Integer> uploadIntervalMap = new HashMap<>();

    // 随机数生成器（固定种子保证可复现）
    private static final Random SHUFFLE_RANDOM = new Random(42L);

    // 调试模式：前 N 轮打印 shuffle 详情（设为 0 关闭）
    private static final int DEBUG_SHUFFLE_ROUNDS = 5;

    public static void main(String[] args) {
        DefaultMQProducer producer = null;
        try {
            LOG.info("========== 启动 SeismicProducer（2000节点 × 差异化频率 × 轮次内随机发送）==========");
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

            // Step 4: 初始化差异化上传频率
            initUploadIntervals();

            // Step 5: 创建线程池
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
            long startTime = System.currentTimeMillis();

            // Step 6: 持续循环发送
            int round = 1;

            while (running) {
                long roundStartTime = System.currentTimeMillis();

                final DefaultMQProducer prod = producer;
                final int currentRound = round;
                AtomicInteger roundSuccess = new AtomicInteger(0);
                AtomicInteger roundFail = new AtomicInteger(0);
                AtomicInteger roundSkipped = new AtomicInteger(0);

                // ===== 每轮收集本轮应发送的节点 =====
                List<Integer> thisRoundNodes = buildThisRoundNodes(currentRound, roundSkipped);

                // 所有节点都读到文件末尾且没有跳过的，说明该重置了
                if (thisRoundNodes.isEmpty() && roundSkipped.get() == 0) {
                    resetAllFiles();
                    LOG.info("========== 循环 {} 完成（共 {} 轮），重置文件，开始循环 {} / 总发送 {} ==========",
                            cycle, round - 1, cycle + 1, totalMessagesSent.get());
                    cycle++;
                    round++;
                    continue;
                }

                // ===== 调试日志：shuffle 前 =====
                if (currentRound <= DEBUG_SHUFFLE_ROUNDS && thisRoundNodes.size() > 10) {
                    // 统计本轮各层级节点数
                    int t1 = 0, t2 = 0, t3 = 0, t4 = 0;
                    for (int nid : thisRoundNodes) {
                        int interval = uploadIntervalMap.getOrDefault(nid, 1);
                        switch (interval) {
                            case 1: t1++; break;
                            case 2: t2++; break;
                            case 3: t3++; break;
                            case 5: t4++; break;
                        }
                    }
                    LOG.info("[轮次{}] 本轮应发送: {} 个节点 (T1={}, T2={}, T3={}, T4={})",
                            currentRound, thisRoundNodes.size(), t1, t2, t3, t4);
                    LOG.info("[轮次{}] shuffle 前前10个: {} (层级: {})",
                            currentRound,
                            thisRoundNodes.subList(0, Math.min(10, thisRoundNodes.size())),
                            getTierLabels(thisRoundNodes.subList(0, Math.min(10, thisRoundNodes.size()))));
                }

                // ===== shuffle：打乱本轮发送顺序 =====
                Collections.shuffle(thisRoundNodes, SHUFFLE_RANDOM);

                // ===== 调试日志：shuffle 后 =====
                if (currentRound <= DEBUG_SHUFFLE_ROUNDS && thisRoundNodes.size() > 10) {
                    LOG.info("[轮次{}] shuffle 后前10个: {} (层级: {})",
                            currentRound,
                            thisRoundNodes.subList(0, Math.min(10, thisRoundNodes.size())),
                            getTierLabels(thisRoundNodes.subList(0, Math.min(10, thisRoundNodes.size()))));
                }

                // ===== 将打乱后的节点分组分配给线程池 =====
                List<List<Integer>> thisRoundGroups =
                        partitionNodes(thisRoundNodes, THREAD_POOL_SIZE);

                List<Future<?>> futures = new ArrayList<>();
                for (List<Integer> group : thisRoundGroups) {
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
                int skipped = roundSkipped.get();

                // 本轮应发送但全部读到末尾
                if (success == 0 && thisRoundNodes.size() > 0) {
                    resetAllFiles();
                    LOG.info("========== 循环 {} 完成（共 {} 轮），重置文件，开始循环 {} / 总发送 {} ==========",
                            cycle, round - 1, cycle + 1, totalMessagesSent.get());
                    cycle++;
                    round++;
                    continue;
                }

                long roundDuration = System.currentTimeMillis() - roundStartTime;

                // 每 10 轮或前 5 轮打印日志
                if (round % 10 == 0 || round <= DEBUG_SHUFFLE_ROUNDS) {
                    LOG.info("[循环{} | 轮次{}] 发送 {} / 跳过 {} / 失败 {} / 耗时 {} ms / 在途 {} / 总发送 {}",
                            cycle, round, success, skipped, roundFail.get(), roundDuration,
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
     * 构建本轮应发送的节点列表
     *
     * 根据每个节点的上传间隔，判断当前轮次是否应发送。
     * 收集后由调用方 shuffle 打乱顺序。
     *
     * @param currentRound 当前轮次号
     * @param roundSkipped 跳过计数器（输出参数）
     * @return 本轮应发送的 nodeId 列表（未 shuffle）
     */
    private static List<Integer> buildThisRoundNodes(int currentRound, AtomicInteger roundSkipped) {
        List<Integer> result = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : uploadIntervalMap.entrySet()) {
            int nodeid = entry.getKey();
            int interval = entry.getValue();
            if (currentRound % interval == 0) {
                result.add(nodeid);
            } else {
                roundSkipped.incrementAndGet();
            }
        }
        return result;
    }

    /**
     * 获取节点列表对应的层级标签（调试用）
     */
    private static List<String> getTierLabels(List<Integer> nodeIds) {
        List<String> labels = new ArrayList<>();
        for (int nid : nodeIds) {
            int interval = uploadIntervalMap.getOrDefault(nid, 0);
            switch (interval) {
                case 1: labels.add("T1"); break;
                case 2: labels.add("T2"); break;
                case 3: labels.add("T3"); break;
                case 5: labels.add("T4"); break;
                default: labels.add("T?"); break;
            }
        }
        return labels;
    }

    /**
     * 初始化差异化上传频率：按节点ID排序后分为 T1/T2/T3/T4 四个层级
     *
     * T1(前200个):  每轮上传，间隔1s  — 模拟近炮点高频采集
     * T2(201~600):  每2轮上传，间隔2s — 模拟中距离常规采集
     * T3(601~1200): 每3轮上传，间隔3s — 模拟远端低频采集
     * T4(1201~2000):每5轮上传，间隔5s — 模拟边缘节能模式
     */
    private static void initUploadIntervals() {
        List<Integer> sortedNodeIds = new ArrayList<>(fileReaders.keySet());
        Collections.sort(sortedNodeIds);

        for (int i = 0; i < sortedNodeIds.size(); i++) {
            int interval;
            if (i < 200) interval = 1;
            else if (i < 600) interval = 2;
            else if (i < 1200) interval = 3;
            else interval = 5;
            uploadIntervalMap.put(sortedNodeIds.get(i), interval);
        }

        long t1 = uploadIntervalMap.values().stream().filter(v -> v == 1).count();
        long t2 = uploadIntervalMap.values().stream().filter(v -> v == 2).count();
        long t3 = uploadIntervalMap.values().stream().filter(v -> v == 3).count();
        long t4 = uploadIntervalMap.values().stream().filter(v -> v == 5).count();
        LOG.info("差异化上传频率分配完成: T1(1s)={}, T2(2s)={}, T3(3s)={}, T4(5s)={}", t1, t2, t3, t4);
        LOG.info("轮次内随机发送已启用（seed=42，结果可复现）");

        // 打印各层级的 nodeId 范围
        LOG.info("T1 nodeId 范围: [{}, {}]", sortedNodeIds.get(0), sortedNodeIds.get(Math.min(199, sortedNodeIds.size() - 1)));
        if (sortedNodeIds.size() > 200)
            LOG.info("T2 nodeId 范围: [{}, {}]", sortedNodeIds.get(200), sortedNodeIds.get(Math.min(599, sortedNodeIds.size() - 1)));
        if (sortedNodeIds.size() > 600)
            LOG.info("T3 nodeId 范围: [{}, {}]", sortedNodeIds.get(600), sortedNodeIds.get(Math.min(1199, sortedNodeIds.size() - 1)));
        if (sortedNodeIds.size() > 1200)
            LOG.info("T4 nodeId 范围: [{}, {}]", sortedNodeIds.get(1200), sortedNodeIds.get(sortedNodeIds.size() - 1));

        // 打印预期每轮发送数量
        LOG.info("===== 预期每轮发送数量 =====");
        for (int r = 1; r <= 10; r++) {
            int count = 0;
            int ct1 = 0, ct2 = 0, ct3 = 0, ct4 = 0;
            for (Map.Entry<Integer, Integer> e : uploadIntervalMap.entrySet()) {
                if (r % e.getValue() == 0) {
                    count++;
                    switch (e.getValue()) {
                        case 1: ct1++; break;
                        case 2: ct2++; break;
                        case 3: ct3++; break;
                        case 5: ct4++; break;
                    }
                }
            }
            LOG.info("  轮次{}: 总={}, T1={}, T2={}, T3={}, T4={}", r, count, ct1, ct2, ct3, ct4);
        }
        LOG.info("============================");
    }

    /**
     * 将节点列表均匀分成 N 组
     */
    private static List<List<Integer>> partitionNodes(List<Integer> nodeIds, int groupCount) {
        int actualGroups = Math.min(groupCount, nodeIds.size());
        List<List<Integer>> groups = new ArrayList<>();
        for (int i = 0; i < actualGroups; i++) {
            groups.add(new ArrayList<>());
        }
        for (int i = 0; i < nodeIds.size(); i++) {
            groups.get(i % actualGroups).add(nodeIds.get(i));
        }
        groups.removeIf(List::isEmpty);
        return groups;
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
     * 异步发送单个节点的一个数据块
     */
    private static boolean sendOneBlockAsync(int nodeid, RandomAccessFile raf,
                                             DefaultMQProducer producer) {
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
        LOG.info("目标吞吐率: ~760 条/秒（差异化上传模式）");
        LOG.info("错误数: {} （错误率: {}%）",
                errors, String.format("%.2f", totalMessages > 0 ? errors * 100.0 / (totalMessages + errors) : 0));
        LOG.info("==================================================");
    }
}
