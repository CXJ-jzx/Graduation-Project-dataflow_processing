package org.jzx.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * RocketMQ Sink 基类
 * 提供 Producer 初始化、消息发送、统计等通用逻辑
 *
 * @param <T> 输入数据类型
 */
public abstract class AbstractRocketMQSink<T> extends RichSinkFunction<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractRocketMQSink.class);

    // RocketMQ 配置
    private final String namesrvAddr;
    private final String topic;
    private final String producerGroup;

    // RocketMQ Producer
    private transient DefaultMQProducer producer;

    // 统计
    private transient AtomicLong sendCount;
    private transient AtomicLong errorCount;
    private transient AtomicLong totalBytes;

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

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        // 初始化 Producer
        producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(namesrvAddr);
        producer.setInstanceName("sink_" + topic + "_" + subtaskIndex);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setSendMsgTimeout(5000);
        producer.start();

        LOG.info("RocketMQ Sink subtask {} 启动, Topic: {}", subtaskIndex, topic);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        try {
            // 子类实现序列化
            byte[] payload = serialize(value);
            String messageKey = extractKey(value);

            // 构造消息
            Message msg = new Message(topic, messageKey, payload);

            // 发送
            producer.send(msg);

            // 统计
            long count = sendCount.incrementAndGet();
            totalBytes.addAndGet(payload.length);

            if (count % 10000 == 0) {
                LOG.info("Sink [{}] 已发送 {} 条消息, 总字节: {} MB",
                        topic, count, totalBytes.get() / 1024 / 1024);
            }

        } catch (Exception e) {
            long errors = errorCount.incrementAndGet();
            if (errors % 100 == 0) {
                LOG.error("Sink [{}] 发送失败, 累计错误: {}", topic, errors, e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Sink [{}] 关闭, 统计: 发送={}, 错误={}, 总字节={} MB",
                topic, sendCount.get(), errorCount.get(), totalBytes.get() / 1024 / 1024);
        if (producer != null) {
            producer.shutdown();
        }
        super.close();
    }

    /**
     * 子类实现：将数据序列化为字节数组
     */
    protected abstract byte[] serialize(T value);

    /**
     * 子类实现：提取消息 Key（用于消息路由和查询）
     */
    protected abstract String extractKey(T value);
}
