package org.jzx.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.jzx.proto.SeismicRecordProto.SeismicRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;

public class SeismicProducer {
    private static final Logger LOG = LoggerFactory.getLogger(SeismicProducer.class);

    private static final String NAMESRV_ADDR = "192.168.56.151:9876";
    private static final String TOPIC = "seismic-raw";

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("seismic-producer-group");
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.start();

        File dataFile = new File("data/seismic_data.pb");
        if (!dataFile.exists()) {
            LOG.error("数据文件不存在: {}", dataFile.getAbsolutePath());
            return;
        }

        LOG.info("开始发送数据...");
        long sendCount = 0;

        try (FileInputStream fis = new FileInputStream(dataFile)) {
            while (fis.available() > 0) {
                SeismicRecord record = SeismicRecord.parseDelimitedFrom(fis);
                if (record == null) break;

                int nodeid = record.getNodeid();

                // 构造消息
                Message msg = new Message(
                        TOPIC,
                        String.valueOf(nodeid),  // Tag = nodeid
                        record.toByteArray()
                );

                // ============================
                // 使用 MessageQueueSelector 保证顺序
                // 同一 nodeid 的消息发送到同一 Queue
                // ============================
                producer.send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        Integer nodeid = (Integer) arg;
                        // 直接使用 nodeid 取模，不需要 hashCode()
                        int index = Math.abs(nodeid) % mqs.size();
                        return mqs.get(index);
                    }
                }, nodeid);  // 传入 nodeid 作为选择依据

                sendCount++;
                if (sendCount % 10000 == 0) {
                    LOG.info("已发送 {} 条消息", sendCount);
                }
            }
        }

        LOG.info("发送完成，总计 {} 条消息", sendCount);
        producer.shutdown();
    }
}
