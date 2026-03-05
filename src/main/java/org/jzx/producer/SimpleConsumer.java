package org.jzx.producer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.jzx.proto.SeismicRecordProto.SeismicRecord;

import java.util.List;

/**
 * 简单消费者，用于验证 Producer 发送的消息
 */
public class SimpleConsumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test-consumer-group");
        consumer.setNamesrvAddr("192.168.56.151:9876");
        consumer.subscribe("seismic-raw", "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            private int count = 0;

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        // 反序列化 Protobuf
                        SeismicRecord record = SeismicRecord.parseFrom(msg.getBody());

                        count++;
                        if (count <= 10) {  // 只打印前 10 条
                            System.out.printf("消息 %d: nodeid=%d, x=%d, y=%d, grid=%s, timestamp=%d, samples=%d%n",
                                    count,
                                    record.getNodeid(),
                                    record.getFixedX(),
                                    record.getFixedY(),
                                    record.getGridId(),
                                    record.getTimestamp(),
                                    record.getSamplesCount());
                        }

                        if (count % 1000 == 0) {
                            System.out.println("已消费 " + count + " 条消息");
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("消费者已启动，等待消息...");
        Thread.sleep(Long.MAX_VALUE);
    }
}
