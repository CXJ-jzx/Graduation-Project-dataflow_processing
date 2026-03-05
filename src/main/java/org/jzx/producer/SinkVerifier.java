package org.jzx.producer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.jzx.proto.EventFeatureProto.EventFeature;
import org.jzx.proto.GridSummaryProto.GridSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sink 输出验证工具
 */
public class SinkVerifier {
    private static final Logger LOG = LoggerFactory.getLogger(SinkVerifier.class);

    private static final String NAMESRV_ADDR = "192.168.56.151:9876";

    public static void main(String[] args) throws Exception {
        startEventFeatureConsumer();
        startGridSummaryConsumer();

        LOG.info("========== Sink 验证工具已启动，等待消息... ==========");
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 消费 seismic-node-events Topic
     */
    private static void startEventFeatureConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("verify-event-feature-group");
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe("seismic-node-events", "*");

        AtomicLong count = new AtomicLong(0);
        AtomicLong anomalyCount = new AtomicLong(0);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(
                    List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        EventFeature feature = EventFeature.parseFrom(msg.getBody());
                        long c = count.incrementAndGet();

                        if (feature.getIsAnomaly()) {
                            anomalyCount.incrementAndGet();
                        }

                        // 打印前 10 条
                        if (c <= 10) {
                            LOG.info("[EventFeature] nodeid={}, grid={}, ts={}, peak={}, rms={}, anomaly={}, events={}, avgPeak={}",
                                    feature.getNodeid(),
                                    feature.getGridId(),
                                    feature.getTimestamp(),
                                    feature.getPeakAmplitude(),
                                    String.format("%.0f", feature.getRmsEnergy()),
                                    feature.getIsAnomaly(),
                                    feature.getRecentEventCount(),
                                    String.format("%.0f", feature.getAvgPeakAmplitude()));
                        }

                        if (c % 5000 == 0) {
                            LOG.info("[EventFeature] 已消费 {} 条, 其中异常 {} 条",
                                    c, anomalyCount.get());
                        }

                    } catch (Exception e) {
                        LOG.error("EventFeature 反序列化失败", e);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        LOG.info("EventFeature 消费者已启动, Topic: seismic-node-events");
    }

    /**
     * 消费 seismic-grid-summary Topic
     */
    private static void startGridSummaryConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("verify-grid-summary-group");
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe("seismic-grid-summary", "*");

        AtomicLong count = new AtomicLong(0);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(
                    List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        GridSummary summary = GridSummary.parseFrom(msg.getBody());
                        long c = count.incrementAndGet();

                        // 打印前 10 条
                        if (c <= 10) {
                            LOG.info("[GridSummary] grid={}, active={}/{}, triggerRate={}%, avgPeak={}, ts={}",
                                    summary.getGridId(),
                                    summary.getActiveNodeCount(),
                                    summary.getTotalNodeCount(),
                                    String.format("%.2f", summary.getTriggerRate() * 100),
                                    String.format("%.0f", summary.getAvgPeakAmplitude()),
                                    summary.getLastUpdateTimestamp());
                        }

                        if (c % 5000 == 0) {
                            LOG.info("[GridSummary] 已消费 {} 条", c);
                        }

                    } catch (Exception e) {
                        LOG.error("GridSummary 反序列化失败", e);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        LOG.info("GridSummary 消费者已启动, Topic: seismic-grid-summary");
    }
}
