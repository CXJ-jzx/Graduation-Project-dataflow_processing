package org.jzx.flink.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.jzx.proto.SeismicRecordProto.SeismicRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 自定义 RocketMQ Source
 * 从 seismic-raw Topic 消费消息，反序列化为 SeismicRecord Protobuf 对象
 *
 * v2 修改:
 *   - 新增 consumeFromLatest 参数，控制消费起始位置
 *   - 全新启动时从最新位置消费，避免积压导致指标失真
 *   - Savepoint 恢复时从上次位置继续（CONSUME_FROM_LAST_OFFSET）
 */
public class RocketMQSeismicSource extends RichParallelSourceFunction<SeismicRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSeismicSource.class);

    // RocketMQ 配置
    private final String namesrvAddr;
    private final String topic;
    private final String consumerGroup;

    // ★ 新增：是否从最新位置消费
    private final boolean consumeFromLatest;

    // 运行控制
    private volatile boolean running = true;
    private transient DefaultMQPushConsumer consumer;

    // 统计
    private transient AtomicLong messageCount;

    /**
     * 兼容旧构造（默认从头消费）
     */
    public RocketMQSeismicSource(String namesrvAddr, String topic,
                                 String consumerGroup) {
        this(namesrvAddr, topic, consumerGroup, false);
    }

    /**
     * ★ 新构造：支持指定消费起始位置
     *
     * @param consumeFromLatest true = 从 Broker 最新 offset 开始（全新启动）
     *                          false = 从上次消费位置继续（Savepoint 恢复）
     */
    public RocketMQSeismicSource(String namesrvAddr, String topic,
                                 String consumerGroup,
                                 boolean consumeFromLatest) {
        this.namesrvAddr = namesrvAddr;
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.consumeFromLatest = consumeFromLatest;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        messageCount = new AtomicLong(0);

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        LOG.info("Source subtask {}/{} 启动, consumerGroup={}, consumeFromLatest={}",
                subtaskIndex, parallelism, consumerGroup, consumeFromLatest);

        // 初始化 RocketMQ Consumer
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setInstanceName("source_" + subtaskIndex);

        // ★ 根据参数决定消费起始位置
        if (consumeFromLatest) {
            // 全新启动：从 Broker 最新位置开始
            // 跳过历史积压，确保采集到的是当前实时数据
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            LOG.info("Source subtask {} 消费模式: LAST_OFFSET (从最新位置)", subtaskIndex);
        } else {
            // 正常/恢复模式：从头开始或从上次 offset 继续
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            LOG.info("Source subtask {} 消费模式: FIRST_OFFSET (从头/续点)", subtaskIndex);
        }

        // 订阅 Topic
        consumer.subscribe(topic, "*");

        // 设置消费参数
        consumer.setConsumeMessageBatchMaxSize(32);
        consumer.setPullBatchSize(64);
        consumer.setConsumeThreadMin(4);
        consumer.setConsumeThreadMax(8);
    }

    @Override
    public void run(SourceContext<SeismicRecord> ctx) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        LOG.info("Source subtask {} 开始消费, Topic: {}, Group: {}",
                subtaskIndex, topic, consumerGroup);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(
                    List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    if (!running) {
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }

                    try {
                        SeismicRecord raw = SeismicRecord.parseFrom(msg.getBody());

                        SeismicRecord record = raw.toBuilder()
                                .setSourceProcessingTimeMs(System.currentTimeMillis())
                                .build();

                        synchronized (ctx.getCheckpointLock()) {
                            ctx.collect(record);
                        }

                        long count = messageCount.incrementAndGet();
                        if (count % 10000 == 0) {
                            LOG.info("Source subtask {} 已消费 {} 条消息",
                                    subtaskIndex, count);
                        }

                    } catch (Exception e) {
                        LOG.error("消息反序列化失败, msgId={}",
                                msg.getMsgId(), e);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        LOG.info("Source subtask {} RocketMQ Consumer 启动成功, Group={}",
                subtaskIndex, consumerGroup);

        while (running) {
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        LOG.info("Source 收到取消信号");
        running = false;
    }

    @Override
    public void close() throws Exception {
        LOG.info("Source 关闭, 共消费 {} 条消息",
                messageCount != null ? messageCount.get() : 0);
        running = false;
        if (consumer != null) {
            consumer.shutdown();
        }
        super.close();
    }
}
