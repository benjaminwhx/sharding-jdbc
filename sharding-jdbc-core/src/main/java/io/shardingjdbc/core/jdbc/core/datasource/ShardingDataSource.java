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

package io.shardingjdbc.core.jdbc.core.datasource;

import io.shardingjdbc.core.api.ConfigMapContext;
import io.shardingjdbc.core.constant.ShardingProperties;
import io.shardingjdbc.core.constant.ShardingPropertiesConstant;
import io.shardingjdbc.core.executor.ExecutorEngine;
import io.shardingjdbc.core.jdbc.adapter.AbstractDataSourceAdapter;
import io.shardingjdbc.core.jdbc.core.ShardingContext;
import io.shardingjdbc.core.jdbc.core.connection.ShardingConnection;
import io.shardingjdbc.core.rule.ShardingRule;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据源分片支持
 * 
 * @author zhangliang
 */
public class ShardingDataSource extends AbstractDataSourceAdapter implements AutoCloseable {
    
    private ShardingProperties shardingProperties;
    
    private ExecutorEngine executorEngine;
    
    private ShardingContext shardingContext;
    
    public ShardingDataSource(final ShardingRule shardingRule) throws SQLException {
        this(shardingRule, new ConcurrentHashMap<String, Object>(), new Properties());
    }

    /**
     * 入口：构建分片数据源
     * @param shardingRule
     * @param configMap 配置map，暂时没用
     * @param props
     * @throws SQLException
     */
    public ShardingDataSource(final ShardingRule shardingRule, final Map<String, Object> configMap, final Properties props) throws SQLException {
        // 1、调用父类构造方法，得到对应的数据源类型
        super(shardingRule.getDataSourceMap().values());

        // 2、存在配置Map放入ConfigMapContext中
        if (configMap != null && !configMap.isEmpty()) {
            ConfigMapContext.getInstance().getShardingConfig().putAll(configMap);
        }

        // 3、获取配置信息
        shardingProperties = new ShardingProperties(null == props ? new Properties() : props);
        int executorSize = shardingProperties.getValue(ShardingPropertiesConstant.EXECUTOR_SIZE);
        executorEngine = new ExecutorEngine(executorSize);
        boolean showSQL = shardingProperties.getValue(ShardingPropertiesConstant.SQL_SHOW);

        // 4、构造分片上下文
        shardingContext = new ShardingContext(shardingRule, getDatabaseType(), executorEngine, showSQL);
    }
    
    /**
     * 重新new一个分片数据源
     *
     * @param newShardingRule new sharding rule
     * @param newProps new sharding properties
     * @throws SQLException SQL exception
     */
    public void renew(final ShardingRule newShardingRule, final Properties newProps) throws SQLException {
        ShardingProperties newShardingProperties = new ShardingProperties(null == newProps ? new Properties() : newProps);
        int originalExecutorSize = shardingProperties.getValue(ShardingPropertiesConstant.EXECUTOR_SIZE);
        int newExecutorSize = newShardingProperties.getValue(ShardingPropertiesConstant.EXECUTOR_SIZE);
        // 线程池大小不同，重新new
        if (originalExecutorSize != newExecutorSize) {
            executorEngine.close();
            executorEngine = new ExecutorEngine(newExecutorSize);
        }
        boolean newShowSQL = newShardingProperties.getValue(ShardingPropertiesConstant.SQL_SHOW);
        shardingProperties = newShardingProperties;
        shardingContext = new ShardingContext(newShardingRule, getDatabaseType(), executorEngine, newShowSQL);
    }

    /**
     * 获取连接
     * @return
     * @throws SQLException
     */
    @Override
    public ShardingConnection getConnection() throws SQLException {
        return new ShardingConnection(shardingContext);
    }
    
    @Override
    public void close() {
        executorEngine.close();
    }
}
