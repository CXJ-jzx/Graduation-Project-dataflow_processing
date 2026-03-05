package org.jzx.flink.sink;

import org.jzx.proto.EventFeatureProto.EventFeature;

/**
 * EventFeature Sink
 * 将节点级特征数据序列化为 Protobuf 写入 RocketMQ
 *
 * Topic: seismic-node-events
 * MessageKey: nodeid
 */
public class EventFeatureSink extends AbstractRocketMQSink<EventFeature> {

    public EventFeatureSink(String namesrvAddr) {
        super(namesrvAddr, "seismic-node-events", "event-feature-sink-group");
    }

    @Override
    protected byte[] serialize(EventFeature value) {
        return value.toByteArray();
    }

    @Override
    protected String extractKey(EventFeature value) {
        return String.valueOf(value.getNodeid());
    }
}
