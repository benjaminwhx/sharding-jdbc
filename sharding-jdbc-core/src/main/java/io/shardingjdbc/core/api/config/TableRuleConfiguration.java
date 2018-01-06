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

import io.shardingjdbc.core.api.config.strategy.ShardingStrategyConfiguration;
import io.shardingjdbc.core.rule.TableRule;
import io.shardingjdbc.core.keygen.KeyGenerator;
import io.shardingjdbc.core.keygen.KeyGeneratorFactory;
import io.shardingjdbc.core.routing.strategy.ShardingStrategy;
import io.shardingjdbc.core.util.InlineExpressionParser;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

/**
 * Table rule configuration.
 * 
 * @author zhangiang
 */
@Getter
@Setter
public class TableRuleConfiguration {

    /**
     * 逻辑表
     */
    private String logicTable;

    /**
     * 真实的数据节点配置
     * 例如：ds_${0..1}.t_order_${0..1}表示数据源ds_0到ds_1下面的t_order_0到t_order_1表
     */
    private String actualDataNodes;

    /**
     * 数据源分片策略配置
     * 用来选取合适的分片策略
     */
    private ShardingStrategyConfiguration databaseShardingStrategyConfig;

    /**
     * 表分片策略配置
     * 用来选取合适的分片策略
     */
    private ShardingStrategyConfiguration tableShardingStrategyConfig;
    
    private String keyGeneratorColumnName;
    
    private String keyGeneratorClass;
    
    private String logicIndex;
    
    /**
     * 构建表规则
     *
     * @param dataSourceMap 数据源map
     * @return 表规则
     */
    public TableRule build(final Map<String, DataSource> dataSourceMap) {
        Preconditions.checkNotNull(logicTable, "Logic table cannot be null.");
        // 1、解析数据节点配置的表达式
        List<String> actualDataNodes = new InlineExpressionParser(this.actualDataNodes).evaluate();
        // 2、获得数据源和表的分片策略
        ShardingStrategy databaseShardingStrategy = null == databaseShardingStrategyConfig ? null : databaseShardingStrategyConfig.build();
        ShardingStrategy tableShardingStrategy = null == tableShardingStrategyConfig ? null : tableShardingStrategyConfig.build();
        // 3、获取key生成器
        KeyGenerator keyGenerator = !Strings.isNullOrEmpty(keyGeneratorColumnName) && !Strings.isNullOrEmpty(keyGeneratorClass) ? KeyGeneratorFactory.newInstance(keyGeneratorClass) : null;
        // 4、返回表规则
        return new TableRule(logicTable, actualDataNodes, dataSourceMap, databaseShardingStrategy, tableShardingStrategy, keyGeneratorColumnName, keyGenerator, logicIndex);
    }
}
