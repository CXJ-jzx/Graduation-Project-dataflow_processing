package org.jzx.flink.operator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.jzx.proto.SeismicEventProto.SeismicEvent;
import org.jzx.proto.SeismicRecordProto.SeismicRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 地震信号提取算子
 *
 * 信号检测策略：
 * 以 10 个采样点为一个窗口，计算窗口内绝对值均值，
 * 与阈值比较来判断是否为信号段。
 *
 * 数据特征：
 * - 噪音段：采样点在 -9 ~ 9 之间（绝对值均值 < 10）
 * - 信号段：采样点在 ±655359 范围波动，但中间可能夹杂 0 值
 */
public class SignalExtractionFunction
        extends KeyedProcessFunction<Integer, SeismicRecord, SeismicEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(SignalExtractionFunction.class);

    // 窗口大小：每 10 个采样点为一个判断单元
    private static final int WINDOW_SIZE = 10;

    // 窗口内绝对值均值阈值
    // 噪音段均值约 0~9，信号段均值约 数万~数十万
    // 设置 100 即可明确区分
    private static final double WINDOW_AVG_THRESHOLD = 100.0;

    // 统计计数器
    private transient long totalRecords;
    private transient long signalRecords;
    private transient long noiseRecords;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        totalRecords = 0;
        signalRecords = 0;
        noiseRecords = 0;
        LOG.info("SignalExtractionFunction 已启动, 窗口大小={}, 阈值={}",
                WINDOW_SIZE, WINDOW_AVG_THRESHOLD);
    }

    @Override
    public void processElement(
            SeismicRecord record,
            Context ctx,
            Collector<SeismicEvent> out) throws Exception {

        totalRecords++;

        // 检查采样点数
        int samplesCount = record.getSamplesCount();
        if (samplesCount != 1000) {
            LOG.warn("节点 {} 采样点数异常: {}, 跳过", record.getNodeid(), samplesCount);
            return;
        }

        // ============================
        // 1. 计算每个窗口的绝对值均值
        // ============================
        int windowCount = samplesCount / WINDOW_SIZE;  // 1000 / 10 = 100 个窗口
        double[] windowAvgs = new double[windowCount];

        for (int w = 0; w < windowCount; w++) {
            long absSum = 0;
            int startIdx = w * WINDOW_SIZE;
            for (int i = 0; i < WINDOW_SIZE; i++) {
                absSum += Math.abs(record.getSamples(startIdx + i));
            }
            windowAvgs[w] = (double) absSum / WINDOW_SIZE;
        }

        // ============================
        // 2. 找信号起始窗口（第一个均值 > 阈值的窗口）
        // ============================
        int startWindow = -1;
        for (int w = 0; w < windowCount; w++) {
            if (windowAvgs[w] > WINDOW_AVG_THRESHOLD) {
                startWindow = w;
                break;
            }
        }

        // 未检测到信号，丢弃
        if (startWindow == -1) {
            noiseRecords++;
            if (noiseRecords % 1000 == 0) {
                LOG.debug("已丢弃 {} 条纯噪音数据", noiseRecords);
            }
            return;
        }

        // ============================
        // 3. 找信号结束窗口（从信号起始往后，找第一个均值 < 阈值的窗口）
        // ============================
        int endWindow = windowCount - 1;  // 默认到最后
        for (int w = startWindow + 1; w < windowCount; w++) {
            if (windowAvgs[w] < WINDOW_AVG_THRESHOLD) {
                endWindow = w;
                break;
            }
        }

        // ============================
        // 4. 换算为采样点下标
        // ============================
        int signal_start_idx = startWindow * WINDOW_SIZE;
        int signal_end_idx = Math.min(endWindow * WINDOW_SIZE + WINDOW_SIZE - 1, samplesCount - 1);
        int signal_length = signal_end_idx - signal_start_idx + 1;

        if (signal_length <= 0) {
            LOG.warn("节点 {} 信号长度异常: {}, 跳过", record.getNodeid(), signal_length);
            return;
        }

        // ============================
        // 5. 在信号段内计算峰值振幅
        // ============================
        int peak_amplitude = 0;
        for (int i = signal_start_idx; i <= signal_end_idx; i++) {
            int abs = Math.abs(record.getSamples(i));
            if (abs > peak_amplitude) {
                peak_amplitude = abs;
            }
        }

        // ============================
        // 6. 在信号段内计算 RMS 能量
        // ============================
        double sumSquare = 0.0;
        for (int i = signal_start_idx; i <= signal_end_idx; i++) {
            double sample = record.getSamples(i);
            sumSquare += sample * sample;
        }
        double rms_energy = Math.sqrt(sumSquare / signal_length);

        // ============================
        // 7. 构造 SeismicEvent 输出
        // ============================
        SeismicEvent event = SeismicEvent.newBuilder()
                .setNodeid(record.getNodeid())
                .setFixedX(record.getFixedX())
                .setFixedY(record.getFixedY())
                .setGridId(record.getGridId())
                .setTimestamp(record.getTimestamp())
                .setSignalStartIdx(signal_start_idx)
                .setSignalEndIdx(signal_end_idx)
                .setSignalLength(signal_length)
                .setPeakAmplitude(peak_amplitude)
                .setRmsEnergy(rms_energy)
                .build();

        out.collect(event);
        signalRecords++;

        // 定期打印统计
        if (totalRecords % 10000 == 0) {
            LOG.info("信号提取统计: 总数={}, 有信号={}, 纯噪音={}, 信号率={}",
                    totalRecords, signalRecords, noiseRecords,
                    String.format("%.2f%%", (double) signalRecords / totalRecords * 100));
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("SignalExtractionFunction 关闭, 最终统计: 总数={}, 有信号={}, 纯噪音={}",
                totalRecords, signalRecords, noiseRecords);
        super.close();
    }
}
