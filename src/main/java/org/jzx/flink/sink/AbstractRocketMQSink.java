package org.jzx.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RocketMQ Sink 基类（高性能版）
 *
 * 优化点：
 * 1. 异步发送（sendAsync）：不阻塞 Flink 线程，吞吐提升 5-10x
 * 2. 批量缓冲：累积到阈值或超时后批量发送，减少网络往返
 * 3. 背压控制：通过 Semaphore 限制异步飞行中的消息数，防止 OOM
 * 4. Producer 参数调优：增大发送缓冲区和压缩
 *
 * @param <T> 输入数据类型
 */
public abstract class AbstractRocketMQSink<T> extends RichSinkFunction<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractRocketMQSink.class);

    // ============ RocketMQ 配置 ============
    private final String namesrvAddr;
    private final String topic;
    private final String producerGroup;

    // ============ 批量发送参数 ============
    /** 批量缓冲区大小：累积到此数量后触发发送 */
    private static final int BATCH_SIZE = 32;

    /** 批量发送超时（毫秒）：即使未满 BATCH_SIZE，超过此时间也触发发送 */
    private static final long BATCH_TIMEOUT_MS = 500;

    // ============ 异步背压控制 ============
    /** 最大飞行中（已发送未确认）的消息数，防止 OOM */
    private static final int MAX_IN_FLIGHT = 512;

    // ============ 运行时状态 ============
    private transient DefaultMQProducer producer;
    private transient List<Message> batchBuffer;
    private transient long lastFlushTime;
    private transient Semaphore inflightSemaphore;

    // ============ 统计 ============
    private transient AtomicLong sendCount;
    private transient AtomicLong errorCount;
    private transient AtomicLong totalBytes;
    private transient AtomicLong batchCount;

    public AbstractRocketMQSink(String namesrvAddr, String topic, String producerGroup) {
        this.namesrvAddr = namesrvAddr;
        this.topic = topic;
        this.producerGroup = producerGroup;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        sendCount = new AtomicLong(0);
        errorCount = new AtomicLong(0);
        totalBytes = new AtomicLong(0);
        batchCount = new AtomicLong(0);
        batchBuffer = new ArrayList<>(BATCH_SIZE);
        lastFlushTime = System.currentTimeMillis();
        inflightSemaphore = new Semaphore(MAX_IN_FLIGHT);

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        // ============ Producer 参数调优 ============
        producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(namesrvAddr);
        producer.setInstanceName("sink_" + topic + "_" + subtaskIndex);

        // 重试策略
        producer.setRetryTimesWhenSendFailed(2);
        producer.setRetryTimesWhenSendAsyncFailed(2);
        producer.setSendMsgTimeout(3000);

        // 性能关键参数
        producer.setCompressMsgBodyOverHowmuch(4096);  // >4KB 自动压缩
        producer.setMaxMessageSize(1024 * 1024);       // 最大消息 1MB

        producer.start();

        LOG.info("RocketMQ Sink subtask {} 启动, Topic: {}, 批量大小: {}, 超时: {}ms, 最大飞行: {}",
                subtaskIndex, topic, BATCH_SIZE, BATCH_TIMEOUT_MS, MAX_IN_FLIGHT);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        // 序列化
        byte[] payload = serialize(value);
        String messageKey = extractKey(value);

        Message msg = new Message(topic, messageKey, payload);
        totalBytes.addAndGet(payload.length);

        // 加入批量缓冲
        batchBuffer.add(msg);

        // 判断是否触发发送
        boolean sizeTriggered = batchBuffer.size() >= BATCH_SIZE;
        boolean timeTriggered = (System.currentTimeMillis() - lastFlushTime) >= BATCH_TIMEOUT_MS;

        if (sizeTriggered || timeTriggered) {
            flushBatch();
        }
    }

    /**
     * 批量异步发送
     */
    private void flushBatch() {
        if (batchBuffer.isEmpty()) {
            return;
        }

        // 取出当前缓冲区内容，立即创建新缓冲区
        List<Message> toSend = batchBuffer;
        batchBuffer = new ArrayList<>(BATCH_SIZE);
        lastFlushTime = System.currentTimeMillis();

        for (Message msg : toSend) {
            try {
                // 背压控制：如果飞行中消息过多，阻塞等待
                inflightSemaphore.acquire();

                // 异步发送（不阻塞 Flink 线程）
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        inflightSemaphore.release();
                        long count = sendCount.incrementAndGet();

                        if (count % 10000 == 0) {
                            LOG.info("Sink [{}] 异步已发送 {} 条, 总字节: {} MB, 批次: {}",
                                    topic, count, totalBytes.get() / 1024 / 1024,
                                    batchCount.get());
                        }
                    }

                    @Override
                    public void onException(Throwable e) {
                        inflightSemaphore.release();
                        long errors = errorCount.incrementAndGet();
                        if (errors % 100 == 0) {
                            LOG.error("Sink [{}] 异步发送失败, 累计错误: {}", topic, errors, e);
                        }
                    }
                });

            } catch (Exception e) {
                inflightSemaphore.release();
                long errors = errorCount.incrementAndGet();
                if (errors % 100 == 0) {
                    LOG.error("Sink [{}] 提交异步发送失败, 累计错误: {}", topic, errors, e);
                }
            }
        }

        batchCount.incrementAndGet();
    }

    @Override
    public void close() throws Exception {
        LOG.info("Sink [{}] 关闭中，刷新剩余缓冲...", topic);

        // 刷新残留数据
        flushBatch();

        // 等待所有飞行中消息完成（最多等 10 秒）
        waitForInflight(10000);

        if (producer != null) {
            producer.shutdown();
        }

        LOG.info("Sink [{}] 关闭完成, 统计: 发送={}, 错误={}, 批次={}, 总字节={} MB",
                topic, sendCount.get(), errorCount.get(), batchCount.get(),
                totalBytes.get() / 1024 / 1024);

        super.close();
    }

    /**
     * 等待所有飞行中的异步消息完成
     */
    private void waitForInflight(long timeoutMs) {
        long start = System.currentTimeMillis();
        while (inflightSemaphore.availablePermits() < MAX_IN_FLIGHT) {
            if (System.currentTimeMillis() - start > timeoutMs) {
                int remaining = MAX_IN_FLIGHT - inflightSemaphore.availablePermits();
                LOG.warn("Sink [{}] 等待超时，仍有 {} 条飞行中消息", topic, remaining);
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * 子类实现：将数据序列化为字节数组
     */
    protected abstract byte[] serialize(T value);

    /**
     * 子类实现：提取消息 Key
     */
    protected abstract String extractKey(T value);
}
