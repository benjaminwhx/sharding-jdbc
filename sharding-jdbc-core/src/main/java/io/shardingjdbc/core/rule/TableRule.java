/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingjdbc.core.rule;

import com.google.common.base.Preconditions;
import io.shardingjdbc.core.keygen.KeyGenerator;
import io.shardingjdbc.core.routing.strategy.ShardingStrategy;
import lombok.Getter;
import lombok.ToString;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 表规则配置
 * 
 * @author zhangliang
 */
@Getter
@ToString
public final class TableRule {
    
    private final String logicTable;
    
    private final List<DataNode> actualDataNodes;
    
    private final ShardingStrategy databaseShardingStrategy;
    
    private final ShardingStrategy tableShardingStrategy;
    
    private final String generateKeyColumn;
    
    private final KeyGenerator keyGenerator;
    
    private final String logicIndex;
    
    public TableRule(final String logicTable, final List<String> actualDataNodes, final Map<String, DataSource> dataSourceMap,
                     final ShardingStrategy databaseShardingStrategy, final ShardingStrategy tableShardingStrategy, 
                     final String generateKeyColumn, final KeyGenerator keyGenerator, final String logicIndex) {
        this.logicTable = logicTable;
        // 获取真实的数据节点：List<db-tb>
        this.actualDataNodes = null == actualDataNodes || actualDataNodes.isEmpty() ? generateDataNodes(logicTable, dataSourceMap) : generateDataNodes(actualDataNodes, dataSourceMap);
        this.databaseShardingStrategy = databaseShardingStrategy;
        this.tableShardingStrategy = tableShardingStrategy;
        this.generateKeyColumn = generateKeyColumn;
        this.keyGenerator = keyGenerator;
        this.logicIndex = logicIndex;
    }

    /**
     * 根据逻辑表名生成分片数据节点集合
     * @note: 这个是多库单表时候调用。
     * @param logicTable
     * @param dataSourceMap
     * @return
     */
    private List<DataNode> generateDataNodes(final String logicTable, final Map<String, DataSource> dataSourceMap) {
        List<DataNode> result = new LinkedList<>();
        for (String each : dataSourceMap.keySet()) {
            result.add(new DataNode(each, logicTable));
        }
        return result;
    }

    /**
     * 根据逻辑表名生成分片数据节点集合
     * dataSourceMap的key一定要存在actualDataNodes中的数据源名字
     * @param actualDataNodes
     * @param dataSourceMap
     * @return
     */
    private List<DataNode> generateDataNodes(final List<String> actualDataNodes, final Map<String, DataSource> dataSourceMap) {
        List<DataNode> result = new LinkedList<>();
        for (String each : actualDataNodes) {
            Preconditions.checkArgument(DataNode.isValidDataNode(each), String.format("Invalid format for actual data nodes: '%s'", each));
            DataNode dataNode = new DataNode(each);
            Preconditions.checkArgument(dataSourceMap.containsKey(dataNode.getDataSourceName()), 
                    String.format("Cannot find data source name in sharding rule, invalid actual data node is: '%s'", each));
            result.add(dataNode);
        }
        return result;
    }
    
    /**
     * 获取真实的数据源名字
     *
     * @return 真实数据源名字
     */
    public Collection<String> getActualDatasourceNames() {
        Collection<String> result = new LinkedHashSet<>(actualDataNodes.size());
        for (DataNode each : actualDataNodes) {
            result.add(each.getDataSourceName());
        }
        return result;
    }
    
    /**
     * 根据数据源名字获取真实的表名
     *
     * @param targetDataSource 目标数据源名字
     * @return 真实的表名
     */
    public Collection<String> getActualTableNames(final String targetDataSource) {
        Collection<String> result = new LinkedHashSet<>(actualDataNodes.size());
        for (DataNode each : actualDataNodes) {
            if (targetDataSource.equals(each.getDataSourceName())) {
                result.add(each.getTableName());
            }
        }
        return result;
    }

    /**
     * 遍历actualDataNodes，返回真实的表索引
     * @param dataSourceName
     * @param actualTableName
     * @return
     */
    int findActualTableIndex(final String dataSourceName, final String actualTableName) {
        int result = 0;
        for (DataNode each : actualDataNodes) {
            if (each.getDataSourceName().equalsIgnoreCase(dataSourceName) && each.getTableName().equalsIgnoreCase(actualTableName)) {
                return result;
            }
            result++;
        }
        return -1;
    }
}
