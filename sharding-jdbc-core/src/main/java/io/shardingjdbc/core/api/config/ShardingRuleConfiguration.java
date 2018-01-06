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

package io.shardingjdbc.core.api.config;

import com.google.common.base.Preconditions;
import io.shardingjdbc.core.api.MasterSlaveDataSourceFactory;
import io.shardingjdbc.core.api.config.strategy.ShardingStrategyConfiguration;
import io.shardingjdbc.core.keygen.DefaultKeyGenerator;
import io.shardingjdbc.core.keygen.KeyGenerator;
import io.shardingjdbc.core.keygen.KeyGeneratorFactory;
import io.shardingjdbc.core.routing.strategy.ShardingStrategy;
import io.shardingjdbc.core.rule.ShardingRule;
import io.shardingjdbc.core.rule.TableRule;
import lombok.Getter;
import lombok.Setter;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

/**
 * 分片规则配置
 * 
 * @author zhangliang
 */
@Getter
@Setter
public class ShardingRuleConfiguration {

    /**
     * 默认的数据源名字
     */
    private String defaultDataSourceName;

    /**
     * 表规则配置
     */
    private Collection<TableRuleConfiguration> tableRuleConfigs = new LinkedList<>();
    
    private Collection<String> bindingTableGroups = new LinkedList<>();

    /**
     * 默认数据源分片策略配置
     */
    private ShardingStrategyConfiguration defaultDatabaseShardingStrategyConfig;
    /**
     * 默认表分片策略配置
     */
    private ShardingStrategyConfiguration defaultTableShardingStrategyConfig;

    private String defaultKeyGeneratorClass;
    
    private Collection<MasterSlaveRuleConfiguration> masterSlaveRuleConfigs = new LinkedList<>();
    
    /**
     * 构建分片规则
     *
     * @param dataSourceMap 数据源map
     * @return 分片规则
     * @throws SQLException SQL exception
     */
    public ShardingRule build(final Map<String, DataSource> dataSourceMap) throws SQLException {
        Preconditions.checkNotNull(dataSourceMap, "dataSources cannot be null.");
        Preconditions.checkArgument(!dataSourceMap.isEmpty(), "dataSources cannot be null.");
        processDataSourceMapWithMasterSlave(dataSourceMap);
        Collection<TableRule> tableRules = new LinkedList<>();
        // 1、构建表规则
        for (TableRuleConfiguration each : tableRuleConfigs) {
            tableRules.add(each.build(dataSourceMap));
        }

        // 2、构建默认的数据源和表的策略
        ShardingStrategy defaultDatabaseShardingStrategy = null == defaultDatabaseShardingStrategyConfig ? null : defaultDatabaseShardingStrategyConfig.build();
        ShardingStrategy defaultTableShardingStrategy = null == defaultTableShardingStrategyConfig ? null :  defaultTableShardingStrategyConfig.build();
        KeyGenerator keyGenerator = KeyGeneratorFactory.newInstance(null == defaultKeyGeneratorClass ? DefaultKeyGenerator.class.getName() : defaultKeyGeneratorClass);
        return new ShardingRule(dataSourceMap, defaultDataSourceName, tableRules, bindingTableGroups, defaultDatabaseShardingStrategy, defaultTableShardingStrategy, keyGenerator);
    }
    
    private void processDataSourceMapWithMasterSlave(final Map<String, DataSource> dataSourceMap) throws SQLException {
        for (MasterSlaveRuleConfiguration each : masterSlaveRuleConfigs) {
            dataSourceMap.put(each.getName(), MasterSlaveDataSourceFactory.createDataSource(dataSourceMap, each, Collections.<String, Object>emptyMap()));
            dataSourceMap.remove(each.getMasterDataSourceName());
            for (String slaveDataSourceName : each.getSlaveDataSourceNames()) {
                dataSourceMap.remove(slaveDataSourceName);
            }
        }
    }
}
