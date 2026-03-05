package org.jzx;

import org.jzx.proto.SeismicRecordProto.SeismicRecord;
import java.util.Arrays;

public class ProtobufTest {
    public static void main(String[] args) throws Exception {
        // 模拟 1000 个采样点的数组
        int[] samplesArray = new int[1000];
        // 前 500 个是噪音（个位数）
        for (int i = 0; i < 500; i++) {
            samplesArray[i] = (int) (Math.random() * 10);
        }
        // 后 500 个是地震信号（数十万级）
        for (int i = 500; i < 1000; i++) {
            samplesArray[i] = (int) (Math.random() * 500000);
        }

        // 构造 SeismicRecord 对象
        SeismicRecord record = SeismicRecord.newBuilder()
                .setNodeid(45050001)
                .setFixedX(100)
                .setFixedY(200)
                .setGridId("0_0")
                .setTimestamp(1770110889)
                .setBatVoltage(47)
                .setBatTemp(43)
                .addAllSamples(Arrays.asList(
                        Arrays.stream(samplesArray).boxed().toArray(Integer[]::new)
                ))  // 一次性添加整个数组
                .build();

        // 序列化为字节数组
        byte[] bytes = record.toByteArray();
        System.out.println("序列化后字节数: " + bytes.length);

        // 反序列化
        SeismicRecord parsed = SeismicRecord.parseFrom(bytes);
        System.out.println("反序列化成功:");
        System.out.println("  nodeid: " + parsed.getNodeid());
        System.out.println("  fixed_x: " + parsed.getFixedX());
        System.out.println("  grid_id: " + parsed.getGridId());
        System.out.println("  采样点总数: " + parsed.getSamplesCount());
        System.out.println("  samples[0]: " + parsed.getSamples(0));
        System.out.println("  samples[500]: " + parsed.getSamples(500));
        System.out.println("  samples[999]: " + parsed.getSamples(999));
    }
}
