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
 */
public class RocketMQSeismicSource extends RichParallelSourceFunction<SeismicRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSeismicSource.class);

    // RocketMQ 配置
    private final String namesrvAddr;
    private final String topic;
    private final String consumerGroup;

    // 运行控制
    private volatile boolean running = true;
    private transient DefaultMQPushConsumer consumer;

    // 统计
    private transient AtomicLong messageCount;

    public RocketMQSeismicSource(String namesrvAddr, String topic, String consumerGroup) {
        this.namesrvAddr = namesrvAddr;
        this.topic = topic;
        this.consumerGroup = consumerGroup;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        messageCount = new AtomicLong(0);

        // 获取当前子任务信息
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        LOG.info("Source subtask {}/{} 启动", subtaskIndex, parallelism);

        // 初始化 RocketMQ Consumer
        // 每个子任务使用不同的实例名，避免冲突
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setInstanceName("source_" + subtaskIndex);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // 订阅 Topic
        consumer.subscribe(topic, "*");

        // 设置消费参数
        consumer.setConsumeMessageBatchMaxSize(32);     // 每批最多消费 32 条
        consumer.setPullBatchSize(64);                  // 每次拉取 64 条
        consumer.setConsumeThreadMin(4);                // 最小消费线程数
        consumer.setConsumeThreadMax(8);                // 最大消费线程数
    }

    @Override
    public void run(SourceContext<SeismicRecord> ctx) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        LOG.info("Source subtask {} 开始消费, Topic: {}", subtaskIndex, topic);

        // 注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    if (!running) {
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }

                    try {
                        // Protobuf 反序列化
                        SeismicRecord record = SeismicRecord.parseFrom(msg.getBody());

                        // 通过 SourceContext 发射数据（线程安全）
                        synchronized (ctx.getCheckpointLock()) {
                            ctx.collect(record);
                        }

                        // 统计
                        long count = messageCount.incrementAndGet();
                        if (count % 10000 == 0) {
                            LOG.info("Source subtask {} 已消费 {} 条消息", subtaskIndex, count);
                        }

                    } catch (Exception e) {
                        LOG.error("消息反序列化失败, msgId={}", msg.getMsgId(), e);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 启动消费者
        consumer.start();
        LOG.info("Source subtask {} RocketMQ Consumer 启动成功", subtaskIndex);

        // 保持运行，直到 cancel() 被调用
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
        LOG.info("Source 关闭, 共消费 {} 条消息", messageCount != null ? messageCount.get() : 0);
        running = false;
        if (consumer != null) {
            consumer.shutdown();
        }
        super.close();
    }
}
