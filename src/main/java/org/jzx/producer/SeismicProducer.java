package org.jzx.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * 地震数据生产者（模拟真实并发采集场景）
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
    private static final int THREAD_POOL_SIZE = 100;
    private static final long SEND_INTERVAL_MS = 1000;

    // 统计计数器
    private static final AtomicLong totalMessagesSent = new AtomicLong(0);
    private static final AtomicLong totalBytesSent = new AtomicLong(0);
    private static final AtomicLong totalErrors = new AtomicLong(0);

    // 坐标映射
    private static Map<Integer, NodeInfo> nodeMapping = new HashMap<>();

    // 文件读取器映射
    private static Map<Integer, RandomAccessFile> fileReaders = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        DefaultMQProducer producer = null;
        try {
            LOG.info("========== 启动 SeismicProducer（并发模式）==========");

            // Step 1: 加载坐标映射配置
            loadNodeMapping();

            // Step 2: 初始化 RocketMQ Producer
            producer = initProducer();

            // Step 3: 扫描 dataset 目录，打开所有文件
            List<File> seisFiles = scanAndOpenFiles();
            LOG.info("成功打开 {} 个 .seis 文件", seisFiles.size());

            // Step 4: 创建线程池
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
            long startTime = System.currentTimeMillis();

            // Step 5: 循环发送数据（按轮次）
            int round = 1;
            boolean hasMoreData = true;

            while (hasMoreData) {
                LOG.info("========== 第 {} 轮发送开始 ==========", round);
                long roundStartTime = System.currentTimeMillis();

                // 为每个节点提交发送任务
                List<Future<Boolean>> futures = new ArrayList<>();
                for (Map.Entry<Integer, RandomAccessFile> entry : fileReaders.entrySet()) {
                    // 使用 final 局部变量
                    final int nodeid = entry.getKey();
                    final RandomAccessFile raf = entry.getValue();
                    final DefaultMQProducer prod = producer;

                    futures.add(executor.submit(() -> sendOneBlock(nodeid, raf, prod)));
                }

                // 等待本轮所有任务完成
                int successCount = 0;
                int failCount = 0;
                for (Future<Boolean> future : futures) {
                    try {
                        if (future.get()) {
                            successCount++;
                        } else {
                            failCount++;
                        }
                    } catch (Exception e) {
                        LOG.error("任务执行异常", e);
                        failCount++;
                    }
                }

                // 如果所有节点都没有数据了，退出循环
                if (successCount == 0) {
                    hasMoreData = false;
                    LOG.info("所有节点数据已发送完毕");
                } else {
                    long roundEndTime = System.currentTimeMillis();
                    LOG.info("第 {} 轮发送完成: 成功 {} 个，失败 {} 个，耗时 {} ms",
                            round, successCount, failCount, roundEndTime - roundStartTime);

                    // 速率控制：每轮间隔 1 秒
                    Thread.sleep(SEND_INTERVAL_MS);
                    round++;
                }
            }

            // Step 6: 关闭所有文件
            closeAllFiles();

            // Step 7: 关闭线程池
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            // Step 8: 打印统计信息
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
     * 加载坐标映射配置
     */
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

    /**
     * 初始化 RocketMQ Producer
     */
    private static DefaultMQProducer initProducer() throws Exception {
        LOG.info("初始化 RocketMQ Producer...");
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setSendMsgTimeout(5000);
        producer.start();
        LOG.info("RocketMQ Producer 启动成功，NameServer: {}", NAMESRV_ADDR);
        return producer;
    }

    /**
     * 扫描 dataset 目录，打开所有 .seis 文件
     */
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

                // 打开文件并跳过文件头
                RandomAccessFile raf = new RandomAccessFile(seisFile, "r");
                raf.seek(FILE_HEADER_SIZE);

                // 读取第一个数据块获取 nodeid
                byte[] metadata = new byte[METADATA_SIZE];
                raf.read(metadata);
                ByteBuffer buffer = ByteBuffer.wrap(metadata).order(ByteOrder.LITTLE_ENDIAN);
                int nodeid = buffer.getInt(OFFSET_NODEID);

                // 重新定位到文件头后
                raf.seek(FILE_HEADER_SIZE);

                // 存储文件读取器
                fileReaders.put(nodeid, raf);
            }
        }

        return seisFiles;
    }

    /**
     * 发送单个节点的一个数据块
     */
    private static boolean sendOneBlock(int nodeid, RandomAccessFile raf, DefaultMQProducer producer) {
        try {
            byte[] blockBuffer = new byte[BLOCK_SIZE];

            // 读取 3048 字节数据块
            synchronized (raf) {
                int bytesRead = raf.read(blockBuffer);
                if (bytesRead < BLOCK_SIZE) {
                    return false;
                }
            }

            // 解析元数据
            ByteBuffer metaBuffer = ByteBuffer.wrap(blockBuffer, 0, METADATA_SIZE)
                    .order(ByteOrder.LITTLE_ENDIAN);

            int timestamp = metaBuffer.getInt(OFFSET_TIMESTAMP);
            short batVoltage = metaBuffer.getShort(OFFSET_BAT_VOLTAGE);
            short batTemp = metaBuffer.getShort(OFFSET_BAT_TEMP);

            // 查询坐标映射
            NodeInfo nodeInfo = nodeMapping.get(nodeid);
            if (nodeInfo == null) {
                LOG.warn("未找到节点 {} 的坐标映射", nodeid);
                totalErrors.incrementAndGet();
                return false;
            }

            // 解析波形数据
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

            // 构造 Protobuf 消息
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

            SeismicRecord record = recordBuilder.build();
            byte[] payload = record.toByteArray();

            // 发送到 RocketMQ
            Message msg = new Message(
                    TOPIC,
                    String.valueOf(nodeid),
                    payload
            );
            producer.send(msg);

            // 统计
            totalMessagesSent.incrementAndGet();
            totalBytesSent.addAndGet(payload.length);

            return true;

        } catch (Exception e) {
            LOG.error("节点 {} 发送失败", nodeid, e);
            totalErrors.incrementAndGet();
            return false;
        }
    }

    /**
     * 关闭所有文件
     */
    private static void closeAllFiles() {
        for (RandomAccessFile raf : fileReaders.values()) {
            try {
                raf.close();
            } catch (Exception e) {
                LOG.error("关闭文件失败", e);
            }
        }
        fileReaders.clear();
    }

    /**
     * 打印统计信息
     */
    private static void printStatistics(long startTime, long endTime, int totalRounds) {
        long duration = (endTime - startTime) / 1000;
        long totalMessages = totalMessagesSent.get();
        long totalBytes = totalBytesSent.get();
        long errors = totalErrors.get();

        LOG.info("========== 统计信息 ==========");
        LOG.info("总轮次: {}", totalRounds);
        LOG.info("总耗时: {} 秒", duration);
        LOG.info("发送消息总数: {}", totalMessages);
        LOG.info("发送字节总数: {} MB", totalBytes / 1024 / 1024);
        LOG.info("平均速率: {} 条/秒", duration > 0 ? totalMessages / duration : 0);
        LOG.info("错误数: {}", errors);
    }
}
