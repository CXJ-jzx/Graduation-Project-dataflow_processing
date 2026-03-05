package org.jzx.flink.sink;

import org.jzx.proto.GridSummaryProto.GridSummary;

/**
 * GridSummary Sink
 * 将网格级聚合数据序列化为 Protobuf 写入 RocketMQ
 *
 * Topic: seismic-grid-summary
 * MessageKey: grid_id
 */
public class GridSummarySink extends AbstractRocketMQSink<GridSummary> {

    public GridSummarySink(String namesrvAddr) {
        super(namesrvAddr, "seismic-grid-summary", "grid-summary-sink-group");
    }

    @Override
    protected byte[] serialize(GridSummary value) {
        return value.toByteArray();
    }

    @Override
    protected String extractKey(GridSummary value) {
        return value.getGridId();
    }
}
