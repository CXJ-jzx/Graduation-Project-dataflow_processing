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
 * 缓存僵化实验专用 Producer
 *
 * 支持命令行参数：
 *   --switch-time N   频率漂移时间点（秒），默认 90
 *   --duration N      Producer 最大运行时长（秒），默认 300
 *
 * Phase1 (0 ~ switch-time): 正常频率分布
 *   T1(前200): 1s   T2(201~600): 2s   T3(601~1200): 3s   T4(1201~2000): 5s
 *
 * Phase2 (switch-time ~ 结束): 部分节点频率漂移
 *   T1 前100个(idx 0~99):     1s → 5s  （僵尸节点）
 *   T1 后100个(idx 100~199):  保持 1s
 *   T4 前100个(idx 1200~1299): 5s → 1s  （新热点）
 *   T4 后700个(idx 1300~1999): 保持 5s
 */
public class StagnationProducer {
    private static final Logger LOG = LoggerFactory.getLogger(StagnationProducer.class);

    // ==================== RocketMQ 配置 ====================
    private static final String NAMESRV_ADDR = "192.168.56.151:9876";
    private static final String PRODUCER_GROUP = "seismic-stagnation-producer-group";
    private static final String TOPIC = "seismic-raw";

    // ==================== .seis 文件格式常量 ====================
    private static final int FILE_HEADER_SIZE = 1024;
    private static final int BLOCK_SIZE = 3048;
    private static final int METADATA_SIZE = 48;
    private static final int SAMPLES_COUNT = 1000;
    private static final int OFFSET_NODEID = 12;
    private static final int OFFSET_TIMESTAMP = 32;
    private static final int OFFSET_BAT_VOLTAGE = 40;
    private static final int OFFSET_BAT_TEMP = 42;

    // ==================== 并发控制 ====================
    private static final int THREAD_POOL_SIZE = 20;
    private static final long TARGET_ROUND_MS = 1000;
    private static final int MAX_IN_FLIGHT = 500;
    private static final Semaphore inflightSemaphore = new Semaphore(MAX_IN_FLIGHT);

    // ==================== 僵化实验核心参数（可通过命令行覆盖）====================
    // 频率漂移时间点（秒），默认 90（WARMUP=30 + PHASE1=60）
    private static int switchTimeSeconds = 90;
    // Producer 最大运行时长（秒），默认 300
    private static int maxDurationSeconds = 300;

    // 漂移节点数量
    private static final int DRIFT_NODE_COUNT = 100;

    // ==================== 统计计数器 ====================
    private static final AtomicLong totalMessagesSent = new AtomicLong(0);
    private static final AtomicLong totalBytesSent = new AtomicLong(0);
    private static final AtomicLong totalErrors = new AtomicLong(0);

    // ==================== 运行时状态 ====================
    private static Map<Integer, NodeInfo> nodeMapping = new HashMap<>();
    private static Map<Integer, RandomAccessFile> fileReaders = new ConcurrentHashMap<>();
    private static volatile int cycle = 1;
    private static volatile boolean running = true;

    private static List<Integer> sortedNodeIds;

    private static int tier1End = 200;
    private static int tier2End = 600;
    private static int tier3End = 1200;

    private static Map<Integer, Integer> phase1IntervalMap = new HashMap<>();
    private static Map<Integer, Integer> phase2IntervalMap = new HashMap<>();
    private static volatile Map<Integer, Integer> currentIntervalMap;

    private static final Random SHUFFLE_RANDOM = new Random(42L);

    private static volatile boolean drifted = false;

    private static Set<Integer> zombieNodes = new HashSet<>();
    private static Set<Integer> newHotNodes = new HashSet<>();

    /**
     * 解析命令行参数
     *
     * 支持：
     *   --switch-time 30    → switchTimeSeconds = 30
     *   --duration 90       → maxDurationSeconds = 90
     */
    private static void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--switch-time":
                    if (i + 1 < args.length) {
                        switchTimeSeconds = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--duration":
                    if (i + 1 < args.length) {
                        maxDurationSeconds = Integer.parseInt(args[++i]);
                    }
                    break;
                default:
                    LOG.warn("未知参数: {}", args[i]);
                    break;
            }
        }
    }

    public static void main(String[] args) {
        DefaultMQProducer producer = null;
        try {
            // ===== 解析命令行参数 =====
            parseArgs(args);

            LOG.info("========== 启动 StagnationProducer（缓存僵化实验）==========");
            LOG.info("命令行参数: switch-time={}s, duration={}s", switchTimeSeconds, maxDurationSeconds);
            LOG.info("频率漂移时间点: {}s 后", switchTimeSeconds);
            LOG.info("最大运行时长:   {}s", maxDurationSeconds);
            LOG.info("漂移节点数: {} 个 T1→低频, {} 个 T4→高频", DRIFT_NODE_COUNT, DRIFT_NODE_COUNT);
            LOG.info("Phase1: 正常频率 T1=1s T2=2s T3=3s T4=5s");
            LOG.info("Phase2: T1前{}个→5s, T4前{}个→1s, 其余不变", DRIFT_NODE_COUNT, DRIFT_NODE_COUNT);
            LOG.info("提示：按 Ctrl+C 优雅停止");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("收到停机信号，正在优雅退出...");
                running = false;
            }));

            // Step 1: 加载坐标映射
            loadNodeMapping();

            // Step 2: 初始化 RocketMQ
            producer = initProducer();

            // Step 3: 扫描文件
            List<File> seisFiles = scanAndOpenFiles();
            LOG.info("成功打开 {} 个 .seis 文件", seisFiles.size());

            // Step 4: 初始化两套频率映射
            initIntervalMaps();
            currentIntervalMap = phase1IntervalMap;

            // Step 5: 线程池
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
            long startTime = System.currentTimeMillis();
            long switchTimestamp = startTime + switchTimeSeconds * 1000L;
            long deadlineTimestamp = startTime + maxDurationSeconds * 1000L;

            // Step 6: 持续循环发送
            int round = 1;

            while (running) {
                long roundStartTime = System.currentTimeMillis();
                long elapsedSec = (roundStartTime - startTime) / 1000;

                // 检查是否超过最大运行时长
                if (roundStartTime >= deadlineTimestamp) {
                    LOG.info("已达最大运行时长 {}s，自动停止", maxDurationSeconds);
                    break;
                }

                // 检查是否到达漂移时间点
                if (!drifted && roundStartTime >= switchTimestamp) {
                    drifted = true;
                    currentIntervalMap = phase2IntervalMap;

                    LOG.info("╔═══════════════════════════════════════════════════════════╗");
                    LOG.info("║  >>> 频率漂移触发！Phase1 → Phase2 <<<                    ║");
                    LOG.info("║  {}个 T1 节点: 1s → 5s（僵尸节点）                        ║", DRIFT_NODE_COUNT);
                    LOG.info("║  {}个 T4 节点: 5s → 1s（新热点）                          ║", DRIFT_NODE_COUNT);
                    LOG.info("║  已运行 {}s，累计发送 {} 条                                ║",
                            elapsedSec, totalMessagesSent.get());
                    LOG.info("╚═══════════════════════════════════════════════════════════╝");
                }

                final DefaultMQProducer prod = producer;
                final int currentRound = round;
                AtomicInteger roundSuccess = new AtomicInteger(0);
                AtomicInteger roundFail = new AtomicInteger(0);
                AtomicInteger roundSkipped = new AtomicInteger(0);

                // 构建本轮应发送的节点
                List<Integer> thisRoundNodes = buildThisRoundNodes(currentRound, roundSkipped);

                if (thisRoundNodes.isEmpty() && roundSkipped.get() == 0) {
                    resetAllFiles();
                    LOG.info("循环 {} 完成（共 {} 轮），重置文件，开始循环 {}",
                            cycle, round - 1, cycle + 1);
                    cycle++;
                    round++;
                    continue;
                }

                // Shuffle
                Collections.shuffle(thisRoundNodes, SHUFFLE_RANDOM);

                // 分组分配给线程池
                List<List<Integer>> groups = partitionNodes(thisRoundNodes, THREAD_POOL_SIZE);
                List<Future<?>> futures = new ArrayList<>();

                for (List<Integer> group : groups) {
                    futures.add(executor.submit(() -> {
                        for (int nodeid : group) {
                            if (!running) return;
                            RandomAccessFile raf = fileReaders.get(nodeid);
                            if (raf == null) continue;
                            boolean ok = sendOneBlockAsync(nodeid, raf, prod);
                            if (ok) roundSuccess.incrementAndGet();
                            else roundFail.incrementAndGet();
                        }
                    }));
                }

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
                if (success == 0 && !thisRoundNodes.isEmpty()) {
                    resetAllFiles();
                    cycle++;
                    round++;
                    continue;
                }

                long roundDuration = System.currentTimeMillis() - roundStartTime;

                // 每 10 轮打印
                if (round % 10 == 0) {
                    String phase = drifted ? "Phase2(漂移)" : "Phase1(正常)";
                    LOG.info("[{} | 循环{} | 轮次{}] 发送={} 跳过={} 失败={} 耗时={}ms 总={} 已运行={}s/{}s",
                            phase, cycle, round, success, roundSkipped.get(),
                            roundFail.get(), roundDuration, totalMessagesSent.get(),
                            elapsedSec, maxDurationSeconds);
                }

                long sleepTime = TARGET_ROUND_MS - roundDuration;
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                }

                round++;
            }

            // 优雅退出
            LOG.info("等待在途异步消息完成...");
            inflightSemaphore.acquire(MAX_IN_FLIGHT);
            inflightSemaphore.release(MAX_IN_FLIGHT);

            closeAllFiles();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            long endTime = System.currentTimeMillis();
            printStatistics(startTime, endTime, round - 1);

        } catch (Exception e) {
            LOG.error("StagnationProducer 运行失败", e);
        } finally {
            closeAllFiles();
            if (producer != null) {
                producer.shutdown();
            }
            LOG.info("========== StagnationProducer 已停止 ==========");
        }
    }

    /**
     * 初始化两套频率映射
     */
    private static void initIntervalMaps() {
        sortedNodeIds = new ArrayList<>(fileReaders.keySet());
        Collections.sort(sortedNodeIds);

        int n = sortedNodeIds.size();
        tier1End = Math.min(200, n);
        tier2End = Math.min(600, n);
        tier3End = Math.min(1200, n);

        // ===== Phase1: 标准频率 =====
        for (int i = 0; i < n; i++) {
            int nodeId = sortedNodeIds.get(i);
            int interval;
            if (i < tier1End) interval = 1;
            else if (i < tier2End) interval = 2;
            else if (i < tier3End) interval = 3;
            else interval = 5;
            phase1IntervalMap.put(nodeId, interval);
        }

        // ===== Phase2: 漂移频率 =====
        phase2IntervalMap.putAll(phase1IntervalMap);

        // T1 前 DRIFT_NODE_COUNT 个: 1s → 5s（僵尸节点）
        int driftCount = Math.min(DRIFT_NODE_COUNT, tier1End);
        for (int i = 0; i < driftCount; i++) {
            int nodeId = sortedNodeIds.get(i);
            phase2IntervalMap.put(nodeId, 5);
            zombieNodes.add(nodeId);
        }

        // T4 前 DRIFT_NODE_COUNT 个: 5s → 1s（新热点）
        int t4Start = tier3End;
        int hotCount = Math.min(DRIFT_NODE_COUNT, n - t4Start);
        for (int i = 0; i < hotCount; i++) {
            int nodeId = sortedNodeIds.get(t4Start + i);
            phase2IntervalMap.put(nodeId, 1);
            newHotNodes.add(nodeId);
        }

        // 统计打印
        LOG.info("===== 频率映射初始化完成 =====");
        LOG.info("switch-time={}s, duration={}s", switchTimeSeconds, maxDurationSeconds);
        LOG.info("Phase1 标准频率:");
        printIntervalStats(phase1IntervalMap);

        LOG.info("Phase2 漂移频率:");
        printIntervalStats(phase2IntervalMap);

        LOG.info("僵尸节点(T1→5s): {} 个, nodeId范围 [{}, {}]",
                zombieNodes.size(),
                sortedNodeIds.get(0),
                sortedNodeIds.get(driftCount - 1));
        LOG.info("新热点(T4→1s):   {} 个, nodeId范围 [{}, {}]",
                newHotNodes.size(),
                sortedNodeIds.get(t4Start),
                sortedNodeIds.get(t4Start + hotCount - 1));

        long phase2HighFreq = phase2IntervalMap.values().stream()
                .filter(v -> v == 1).count();
        LOG.info("Phase2 高频(1s)节点总数: {}", phase2HighFreq);
        LOG.info("==================================");
    }

    private static void printIntervalStats(Map<Integer, Integer> intervalMap) {
        long t1 = intervalMap.values().stream().filter(v -> v == 1).count();
        long t2 = intervalMap.values().stream().filter(v -> v == 2).count();
        long t3 = intervalMap.values().stream().filter(v -> v == 3).count();
        long t4 = intervalMap.values().stream().filter(v -> v == 5).count();
        LOG.info("  1s={}, 2s={}, 3s={}, 5s={}", t1, t2, t3, t4);
    }

    /**
     * 构建本轮应发送的节点列表
     */
    private static List<Integer> buildThisRoundNodes(int currentRound, AtomicInteger roundSkipped) {
        List<Integer> result = new ArrayList<>();
        Map<Integer, Integer> intervalMap = currentIntervalMap;

        for (Map.Entry<Integer, Integer> entry : intervalMap.entrySet()) {
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

    // ==================== 工具方法 ====================

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

    private static void resetAllFiles() {
        for (Map.Entry<Integer, RandomAccessFile> entry : fileReaders.entrySet()) {
            try {
                entry.getValue().seek(FILE_HEADER_SIZE);
            } catch (Exception e) {
                LOG.error("重置节点 {} 文件指针失败", entry.getKey(), e);
            }
        }
    }

    private static void loadNodeMapping() throws Exception {
        File configFile = new File("config/node_mapping.pb");
        if (!configFile.exists()) {
            throw new RuntimeException("配置文件不存在: config/node_mapping.pb");
        }
        try (FileInputStream fis = new FileInputStream(configFile)) {
            NodeMapping mapping = NodeMapping.parseFrom(fis);
            for (NodeInfo node : mapping.getNodesList()) {
                nodeMapping.put(node.getNodeid(), node);
            }
            LOG.info("加载坐标映射: {} 个节点", nodeMapping.size());
        }
    }

    private static DefaultMQProducer initProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.setRetryTimesWhenSendFailed(10);
        producer.setRetryTimesWhenSendAsyncFailed(10);
        producer.setSendMsgTimeout(10000);
        producer.setCompressMsgBodyOverHowmuch(4096);
        producer.setMaxMessageSize(4 * 1024 * 1024);
        producer.start();
        LOG.info("RocketMQ Producer 启动成功");

        try {
            Message testMsg = new Message(TOPIC, "health-check", "ping".getBytes());
            producer.send(testMsg);
            LOG.info("Topic [{}] 路由验证通过", TOPIC);
        } catch (Exception e) {
            producer.shutdown();
            throw new RuntimeException("Topic 不可用", e);
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
            try { raf.close(); } catch (Exception e) { /* ignore */ }
        }
        fileReaders.clear();
    }

    private static void printStatistics(long startTime, long endTime, int totalRounds) {
        long duration = (endTime - startTime) / 1000;
        long totalMessages = totalMessagesSent.get();
        long errors = totalErrors.get();

        LOG.info("==================== 僵化实验统计 ====================");
        LOG.info("参数: switch-time={}s, duration={}s", switchTimeSeconds, maxDurationSeconds);
        LOG.info("总轮次: {}, 总耗时: {}s", totalRounds, duration);
        LOG.info("发送消息: {}, 错误: {}", totalMessages, errors);
        LOG.info("僵尸节点: {} 个, 新热点: {} 个", zombieNodes.size(), newHotNodes.size());
        LOG.info("漂移是否触发: {}", drifted ? "是" : "否");
        LOG.info("====================================================");
    }
}
