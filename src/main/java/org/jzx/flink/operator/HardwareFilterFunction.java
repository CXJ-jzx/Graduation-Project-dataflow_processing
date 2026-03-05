package org.jzx.flink.operator;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.jzx.proto.SeismicRecordProto.SeismicRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 硬件指标过滤算子
 *
 * 功能：
 * 1. 检查电池电压（< 10 为异常）
 * 2. 检查电池温度（> 80 为异常）
 * 3. 检查时间戳间隔（< 0 或 > 10 秒为异常）
 * 4. 异常数据输出到 SideOutput，正常数据继续流向下游
 */
public class HardwareFilterFunction extends ProcessFunction<SeismicRecord, SeismicRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(HardwareFilterFunction.class);

    // 过滤阈值
    private static final int MIN_VOLTAGE = 10;      // 最低电压
    private static final int MAX_TEMP = 80;         // 最高温度
    private static final int MAX_TIME_GAP = 10;     // 最大时间间隔（秒）

    // 侧输出标签（用于输出异常数据）
    public static final OutputTag<SeismicRecord> FILTERED_DATA_TAG =
            new OutputTag<SeismicRecord>("filtered-data") {};

    // 状态：存储该节点上一条数据的时间戳
    private transient ValueState<Long> lastTimestampState;

    // 统计计数器
    private transient long normalCount = 0;
    private transient long filteredCount = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 初始化状态
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>("lastTimestamp", Long.class);
        lastTimestampState = getRuntimeContext().getState(descriptor);

        LOG.info("HardwareFilterFunction 已启动");
    }

    @Override
    public void processElement(
            SeismicRecord record,
            Context ctx,
            Collector<SeismicRecord> out) throws Exception {

        boolean isAbnormal = false;
        String reason = "";

        // ============================
        // 1. 检查电池电压
        // ============================
        if (record.getBatVoltage() < MIN_VOLTAGE) {
            isAbnormal = true;
            reason = String.format("低电压: %d", record.getBatVoltage());
        }

        // ============================
        // 2. 检查电池温度
        // ============================
        if (record.getBatTemp() > MAX_TEMP) {
            isAbnormal = true;
            if (!reason.isEmpty()) reason += ", ";
            reason += String.format("高温: %d", record.getBatTemp());
        }

        // ============================
        // 3. 检查时间戳异常
        // ============================
        Long lastTimestamp = lastTimestampState.value();
        if (lastTimestamp != null) {
            long timeDiff = record.getTimestamp() - lastTimestamp;

            // 时间倒退或间隔过大
            if (timeDiff < 0) {
                isAbnormal = true;
                if (!reason.isEmpty()) reason += ", ";
                reason += String.format("时间倒退: %d秒", timeDiff);
            } else if (timeDiff > MAX_TIME_GAP) {
                isAbnormal = true;
                if (!reason.isEmpty()) reason += ", ";
                reason += String.format("时间间隔过大: %d秒", timeDiff);
            }
        }

        // 更新状态（无论是否异常都要更新）
        lastTimestampState.update(record.getTimestamp());

        // ============================
        // 4. 输出数据
        // ============================
        if (isAbnormal) {
            // 输出到侧输出流
            ctx.output(FILTERED_DATA_TAG, record);
            filteredCount++;

            // 定期打印异常日志（避免日志过多）
            if (filteredCount % 100 == 0) {
                LOG.warn("节点 {} 异常数据已过滤 {} 条, 最近原因: {}",
                        record.getNodeid(), filteredCount, reason);
            }
        } else {
            // 输出到主流
            out.collect(record);
            normalCount++;

            // 定期打印统计
            if (normalCount % 10000 == 0) {
                LOG.info("已处理正常数据 {} 条, 过滤异常数据 {} 条",
                        normalCount, filteredCount);
            }
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("HardwareFilterFunction 关闭, 统计: 正常 {} 条, 异常 {} 条",
                normalCount, filteredCount);
        super.close();
    }
}
