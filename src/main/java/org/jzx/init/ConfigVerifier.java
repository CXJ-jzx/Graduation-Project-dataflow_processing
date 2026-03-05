package org.jzx.init;

import org.jzx.proto.NodeConfigProto.*;

import java.io.FileInputStream;

/**
 * 验证生成的配置文件是否可正确反序列化
 */
public class ConfigVerifier {
    public static void main(String[] args) throws Exception {
        System.out.println("========== 验证 node_mapping.pb ==========");
        try (FileInputStream fis = new FileInputStream("config/node_mapping.pb")) {
            NodeMapping nodeMapping = NodeMapping.parseFrom(fis);
            System.out.println("节点总数: " + nodeMapping.getNodesCount());

            // 打印前 5 个节点
            for (int i = 0; i < Math.min(5, nodeMapping.getNodesCount()); i++) {
                NodeInfo node = nodeMapping.getNodes(i);
                System.out.printf("  节点 %d: x=%d, y=%d, grid=%s%n",
                        node.getNodeid(), node.getFixedX(), node.getFixedY(), node.getGridId());
            }
        }

        System.out.println("\n========== 验证 grid_mapping.pb ==========");
        try (FileInputStream fis = new FileInputStream("config/grid_mapping.pb")) {
            GridMappingList gridMapping = GridMappingList.parseFrom(fis);
            System.out.println("网格总数: " + gridMapping.getGridsCount());

            // 打印前 3 个网格
            for (int i = 0; i < Math.min(3, gridMapping.getGridsCount()); i++) {
                GridInfo grid = gridMapping.getGrids(i);
                System.out.printf("  网格 %s: %d 个节点, 前5个节点ID: %s%n",
                        grid.getGridId(),
                        grid.getTotalNodeCount(),
                        grid.getNodeIdsList().subList(0, Math.min(5, grid.getNodeIdsCount())));
            }
        }

        System.out.println("\n========== 验证通过 ==========");
    }
}
