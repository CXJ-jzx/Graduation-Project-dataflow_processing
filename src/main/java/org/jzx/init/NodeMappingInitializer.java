package org.jzx.init;

import org.jzx.proto.NodeConfigProto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * 坐标初始化工具
 * 功能：
 * 1. 扫描 dataset/ 目录，从 .seis 文件中读取真实 nodeid
 * 2. 按公式计算固定坐标和网格 ID
 * 3. 生成 config/node_mapping.pb 和 config/grid_mapping.pb
 */
public class NodeMappingInitializer {
    private static final Logger LOG = LoggerFactory.getLogger(NodeMappingInitializer.class);

    // 坐标计算参数
    private static final int TOTAL_NODES = 2000;
    private static final int COLS = 50;           // 50 列
    private static final int ROWS = 40;           // 40 行
    private static final int SPACING = 100;       // 节点间距
    private static final int GRID_COLS = 10;      // 网格列数
    private static final int GRID_ROWS = 10;      // 网格行数
    private static final int NODES_PER_GRID_COL = 5;  // 每个网格包含 5 列节点
    private static final int NODES_PER_GRID_ROW = 4;  // 每个网格包含 4 行节点

    // .seis 文件格式常量
    private static final int FILE_HEADER_SIZE = 1024;  // 文件头大小
    private static final int BLOCK_SIZE = 3048;        // 数据块大小
    private static final int METADATA_SIZE = 48;       // 元数据大小
    private static final int NODEID_OFFSET = 12;       // nodeid 在元数据中的偏移

    public static void main(String[] args) {
        try {
            LOG.info("========== 开始坐标初始化 ==========");

            // Step 1: 扫描 dataset 目录，从 .seis 文件读取 nodeid
            List<Integer> nodeIds = scanDatasetAndExtractNodeIds();
            LOG.info("扫描到 {} 个节点", nodeIds.size());

            if (nodeIds.size() != TOTAL_NODES) {
                LOG.warn("警告: 预期 {} 个节点，实际扫描到 {} 个", TOTAL_NODES, nodeIds.size());
            }

            // Step 2: 计算坐标和网格映射
            Map<Integer, NodeInfo> nodeMapping = new HashMap<>();
            Map<String, List<Integer>> gridMapping = new HashMap<>();

            for (int rank = 0; rank < nodeIds.size(); rank++) {
                int nodeid = nodeIds.get(rank);

                // 计算固定坐标
                int fixed_x = (rank % COLS) * SPACING;
                int fixed_y = (rank / COLS) * SPACING;

                // 计算网格 ID
                int grid_col = (rank % COLS) / NODES_PER_GRID_COL;
                int grid_row = (rank / COLS) / NODES_PER_GRID_ROW;
                String grid_id = grid_row + "_" + grid_col;

                // 构造 NodeInfo
                NodeInfo nodeInfo = NodeInfo.newBuilder()
                        .setNodeid(nodeid)
                        .setFixedX(fixed_x)
                        .setFixedY(fixed_y)
                        .setGridId(grid_id)
                        .build();

                nodeMapping.put(nodeid, nodeInfo);

                // 添加到网格映射
                gridMapping.computeIfAbsent(grid_id, k -> new ArrayList<>()).add(nodeid);

                // 每 200 个节点打印一次进度
                if ((rank + 1) % 200 == 0) {
                    LOG.info("已处理 {}/{} 个节点", rank + 1, nodeIds.size());
                }
            }

            LOG.info("坐标计算完成，共 {} 个节点，{} 个网格", nodeMapping.size(), gridMapping.size());

            // Step 3: 生成 NodeMapping Protobuf
            NodeMapping.Builder nodeMappingBuilder = NodeMapping.newBuilder();
            for (NodeInfo nodeInfo : nodeMapping.values()) {
                nodeMappingBuilder.addNodes(nodeInfo);
            }
            NodeMapping nodeMappingProto = nodeMappingBuilder.build();

            // Step 4: 生成 GridMappingList Protobuf
            GridMappingList.Builder gridMappingBuilder = GridMappingList.newBuilder();
            for (Map.Entry<String, List<Integer>> entry : gridMapping.entrySet()) {
                GridInfo gridInfo = GridInfo.newBuilder()
                        .setGridId(entry.getKey())
                        .addAllNodeIds(entry.getValue())
                        .setTotalNodeCount(entry.getValue().size())
                        .build();
                gridMappingBuilder.addGrids(gridInfo);
            }
            GridMappingList gridMappingProto = gridMappingBuilder.build();

            // Step 5: 写入文件
            File configDir = new File("config");
            if (!configDir.exists()) {
                configDir.mkdirs();
                LOG.info("创建 config/ 目录");
            }

            File nodeMappingFile = new File("config/node_mapping.pb");
            try (FileOutputStream fos = new FileOutputStream(nodeMappingFile)) {
                nodeMappingProto.writeTo(fos);
                LOG.info("写入 node_mapping.pb 成功，文件大小: {} 字节", nodeMappingFile.length());
            }

            File gridMappingFile = new File("config/grid_mapping.pb");
            try (FileOutputStream fos = new FileOutputStream(gridMappingFile)) {
                gridMappingProto.writeTo(fos);
                LOG.info("写入 grid_mapping.pb 成功，文件大小: {} 字节", gridMappingFile.length());
            }

            // Step 6: 验证统计
            printStatistics(nodeMapping, gridMapping);

            LOG.info("========== 坐标初始化完成 ==========");

        } catch (Exception e) {
            LOG.error("坐标初始化失败", e);
            System.exit(1);
        }
    }

    /**
     * 扫描 dataset 目录，从 .seis 文件中读取 nodeid
     */
    private static List<Integer> scanDatasetAndExtractNodeIds() throws Exception {
        File datasetDir = new File("dataset");
        if (!datasetDir.exists() || !datasetDir.isDirectory()) {
            throw new RuntimeException("dataset/ 目录不存在，请先准备数据集");
        }

        File[] folders = datasetDir.listFiles(File::isDirectory);
        if (folders == null || folders.length == 0) {
            throw new RuntimeException("dataset/ 目录为空");
        }

        List<Integer> nodeIds = new ArrayList<>();
        int successCount = 0;
        int failCount = 0;

        for (File folder : folders) {
            try {
                // 查找该文件夹下的 .seis 文件
                File[] seisFiles = folder.listFiles((dir, name) -> name.endsWith(".seis"));
                if (seisFiles == null || seisFiles.length == 0) {
                    LOG.warn("文件夹 {} 中未找到 .seis 文件", folder.getName());
                    failCount++;
                    continue;
                }

                // 取第一个 .seis 文件
                File seisFile = seisFiles[0];
                int nodeid = extractNodeIdFromSeisFile(seisFile);
                nodeIds.add(nodeid);
                successCount++;

                if (successCount % 200 == 0) {
                    LOG.info("已扫描 {} 个节点...", successCount);
                }

            } catch (Exception e) {
                LOG.error("处理文件夹 {} 失败: {}", folder.getName(), e.getMessage());
                failCount++;
            }
        }

        LOG.info("扫描完成: 成功 {} 个，失败 {} 个", successCount, failCount);

        // 按 nodeid 升序排列（重要！rank 依赖排序）
        Collections.sort(nodeIds);
        return nodeIds;
    }

    /**
     * 从 .seis 文件中读取 nodeid（字节偏移 12-15）
     */
    private static int extractNodeIdFromSeisFile(File seisFile) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(seisFile, "r")) {
            // 检查文件大小
            if (raf.length() < FILE_HEADER_SIZE + METADATA_SIZE) {
                throw new RuntimeException("文件大小不足: " + seisFile.getName());
            }

            // 跳过 1024 字节文件头
            raf.seek(FILE_HEADER_SIZE);

            // 读取前 48 字节元数据
            byte[] metadata = new byte[METADATA_SIZE];
            int bytesRead = raf.read(metadata);
            if (bytesRead != METADATA_SIZE) {
                throw new RuntimeException("元数据读取不完整: " + seisFile.getName());
            }

            // 解析 nodeid（偏移 12-15，小端序）
            ByteBuffer buffer = ByteBuffer.wrap(metadata).order(ByteOrder.LITTLE_ENDIAN);
            int nodeid = buffer.getInt(NODEID_OFFSET);

            return nodeid;
        }
    }

    /**
     * 打印统计信息
     */
    private static void printStatistics(Map<Integer, NodeInfo> nodeMapping,
                                        Map<String, List<Integer>> gridMapping) {
        LOG.info("========== 统计信息 ==========");
        LOG.info("节点总数: {}", nodeMapping.size());
        LOG.info("网格总数: {}", gridMapping.size());

        // 坐标范围
        int minX = Integer.MAX_VALUE, maxX = Integer.MIN_VALUE;
        int minY = Integer.MAX_VALUE, maxY = Integer.MIN_VALUE;
        for (NodeInfo node : nodeMapping.values()) {
            minX = Math.min(minX, node.getFixedX());
            maxX = Math.max(maxX, node.getFixedX());
            minY = Math.min(minY, node.getFixedY());
            maxY = Math.max(maxY, node.getFixedY());
        }
        LOG.info("坐标范围: X[{}, {}], Y[{}, {}]", minX, maxX, minY, maxY);

        // 网格节点数统计
        int minNodes = Integer.MAX_VALUE, maxNodes = Integer.MIN_VALUE;
        for (List<Integer> nodes : gridMapping.values()) {
            minNodes = Math.min(minNodes, nodes.size());
            maxNodes = Math.max(maxNodes, nodes.size());
        }
        LOG.info("每个网格节点数范围: [{}, {}]", minNodes, maxNodes);

        // 打印前 3 个网格的详细信息
        LOG.info("前 3 个网格示例:");
        gridMapping.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())  // 按 grid_id 排序
                .limit(3)
                .forEach(entry -> {
                    LOG.info("  网格 {}: {} 个节点, 前5个节点ID: {}",
                            entry.getKey(),
                            entry.getValue().size(),
                            entry.getValue().subList(0, Math.min(5, entry.getValue().size())));
                });

        // 打印前 5 个节点的详细信息
        LOG.info("前 5 个节点示例:");
        nodeMapping.values().stream()
                .sorted(Comparator.comparingInt(NodeInfo::getNodeid))
                .limit(5)
                .forEach(node -> {
                    LOG.info("  节点 {}: x={}, y={}, grid={}",
                            node.getNodeid(), node.getFixedX(), node.getFixedY(), node.getGridId());
                });
    }
}
