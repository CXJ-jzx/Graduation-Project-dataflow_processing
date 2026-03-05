package org.jzx.flink.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jzx.proto.SeismicRecordProto.SeismicRecord;
import org.jzx.proto.SeismicEventProto.SeismicEvent;
import org.jzx.proto.EventFeatureProto.EventFeature;
import org.jzx.proto.GridSummaryProto.GridSummary;

/**
 * 注册 Protobuf 类到 Flink Kryo 序列化器
 */
public class ProtobufSerializerRegistrar {

    public static void registerAll(StreamExecutionEnvironment env) {
        env.getConfig().registerKryoType(SeismicRecord.class);
        env.getConfig().registerKryoType(SeismicEvent.class);
        env.getConfig().registerKryoType(EventFeature.class);
        env.getConfig().registerKryoType(GridSummary.class);
    }
}
