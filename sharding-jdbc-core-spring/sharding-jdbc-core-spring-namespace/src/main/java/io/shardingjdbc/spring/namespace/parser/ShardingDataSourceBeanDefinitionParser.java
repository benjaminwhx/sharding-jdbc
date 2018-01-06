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

package io.shardingjdbc.spring.namespace.parser;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.shardingjdbc.core.api.config.ShardingRuleConfiguration;
import io.shardingjdbc.core.api.config.TableRuleConfiguration;
import io.shardingjdbc.spring.datasource.SpringShardingDataSource;
import io.shardingjdbc.spring.namespace.constants.ShardingDataSourceBeanDefinitionParserTag;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 解析spring命名空间得到分片数据源配置信息
 * 
 * @author caohao
 */
public class ShardingDataSourceBeanDefinitionParser extends AbstractBeanDefinitionParser {
    
    @Override
    //CHECKSTYLE:OFF
    protected AbstractBeanDefinition parseInternal(final Element element, final ParserContext parserContext) {
    //CHECKSTYLE:ON

        // 构造SpringShardingDataSource
        BeanDefinitionBuilder factory = BeanDefinitionBuilder.rootBeanDefinition(SpringShardingDataSource.class);
        // 1、解析Map<String, DataSource> dataSourceMap
        factory.addConstructorArgValue(parseDataSources(element));
        // 2、解析ShardingRuleConfiguration shardingRuleConfig
        factory.addConstructorArgValue(parseShardingRuleConfig(element));
        // 3、解析Map<String, Object> configMap
        factory.addConstructorArgValue(parseConfigMap(element, parserContext, factory.getBeanDefinition()));
        // 4、解析Properties props
        factory.addConstructorArgValue(parseProperties(element, parserContext));
        factory.setDestroyMethodName("close");
        return factory.getBeanDefinition();
    }

    /**
     * 解析sharding:data-source的数据源Map
     * @param element
     * @return
     */
    private Map<String, RuntimeBeanReference> parseDataSources(final Element element) {
        // sharding:sharding-rule
        Element shardingRuleElement = DomUtils.getChildElementByTagName(element, ShardingDataSourceBeanDefinitionParserTag.SHARDING_RULE_CONFIG_TAG);
        // data-source-names 逗号分隔得到多个数据源name
        List<String> dataSources = Splitter.on(",").trimResults().splitToList(shardingRuleElement.getAttribute(ShardingDataSourceBeanDefinitionParserTag.DATA_SOURCE_NAMES_TAG));
        Map<String, RuntimeBeanReference> result = new ManagedMap<>(dataSources.size());
        for (String each : dataSources) {
            // RuntimeBeanReference就是将得到数据源的name和spring的bean对应起来
            result.put(each, new RuntimeBeanReference(each));
        }
        return result;
    }
    
    private BeanDefinition parseShardingRuleConfig(final Element element) {
        // sharding:sharding-rule
        Element shardingRuleElement = DomUtils.getChildElementByTagName(element, ShardingDataSourceBeanDefinitionParserTag.SHARDING_RULE_CONFIG_TAG);
        BeanDefinitionBuilder factory = BeanDefinitionBuilder.rootBeanDefinition(ShardingRuleConfiguration.class);
        parseDefaultDataSource(factory, shardingRuleElement);
        parseDefaultDatabaseShardingStrategy(factory, shardingRuleElement);
        parseDefaultTableShardingStrategy(factory, shardingRuleElement);
        factory.addPropertyValue("tableRuleConfigs", parseTableRulesConfig(shardingRuleElement));
        factory.addPropertyValue("bindingTableGroups", parseBindingTablesConfig(shardingRuleElement));
        parseKeyGenerator(factory, shardingRuleElement);
        return factory.getBeanDefinition();
    }
    
    private void parseKeyGenerator(final BeanDefinitionBuilder factory, final Element element) {
        String keyGeneratorClass = element.getAttribute(ShardingDataSourceBeanDefinitionParserTag.KEY_GENERATOR_CLASS);
        if (!Strings.isNullOrEmpty(keyGeneratorClass)) {
            factory.addPropertyValue("defaultKeyGeneratorClass", keyGeneratorClass);
        }
    }

    /**
     * 解析属性default-data-source-name的值
     * @param factory
     * @param element
     */
    private void parseDefaultDataSource(final BeanDefinitionBuilder factory, final Element element) {
        String defaultDataSource = element.getAttribute(ShardingDataSourceBeanDefinitionParserTag.DEFAULT_DATA_SOURCE_NAME_TAG);
        if (!Strings.isNullOrEmpty(defaultDataSource)) {
            factory.addPropertyValue("defaultDataSourceName", defaultDataSource);
        }
    }

    /**
     * 解析属性default-database-strategy-ref的值
     * @param factory
     * @param element
     */
    private void parseDefaultDatabaseShardingStrategy(final BeanDefinitionBuilder factory, final Element element) {
        String defaultDatabaseShardingStrategy = element.getAttribute(ShardingDataSourceBeanDefinitionParserTag.DEFAULT_DATABASE_STRATEGY_REF_ATTRIBUTE);
        if (!Strings.isNullOrEmpty(defaultDatabaseShardingStrategy)) {
            factory.addPropertyReference("defaultDatabaseShardingStrategyConfig", defaultDatabaseShardingStrategy);
        }
    }

    /**
     * 解析属性default-table-strategy-ref的值
     * @param factory
     * @param element
     */
    private void parseDefaultTableShardingStrategy(final BeanDefinitionBuilder factory, final Element element) {
        String defaultTableShardingStrategy = element.getAttribute(ShardingDataSourceBeanDefinitionParserTag.DEFAULT_TABLE_STRATEGY_REF_ATTRIBUTE);
        if (!Strings.isNullOrEmpty(defaultTableShardingStrategy)) {
            factory.addPropertyReference("defaultTableShardingStrategyConfig", defaultTableShardingStrategy);
        }
    }
    
    private List<BeanDefinition> parseTableRulesConfig(final Element element) {
        // table-rules
        Element tableRulesElement = DomUtils.getChildElementByTagName(element, ShardingDataSourceBeanDefinitionParserTag.TABLE_RULES_TAG);
        List<Element> tableRuleElements = DomUtils.getChildElementsByTagName(tableRulesElement, ShardingDataSourceBeanDefinitionParserTag.TABLE_RULE_TAG);
        List<BeanDefinition> result = new ManagedList<>(tableRuleElements.size());
        for (Element each : tableRuleElements) {
            result.add(parseTableRuleConfig(each));
        }
        return result;
    }
    
    private BeanDefinition parseTableRuleConfig(final Element tableElement) {
        BeanDefinitionBuilder factory = BeanDefinitionBuilder.rootBeanDefinition(TableRuleConfiguration.class);
        factory.addPropertyValue("logicTable", tableElement.getAttribute(ShardingDataSourceBeanDefinitionParserTag.LOGIC_TABLE_ATTRIBUTE));
        String actualDataNodes = tableElement.getAttribute(ShardingDataSourceBeanDefinitionParserTag.ACTUAL_DATA_NODES_ATTRIBUTE);
        if (!Strings.isNullOrEmpty(actualDataNodes)) {
            factory.addPropertyValue("actualDataNodes", actualDataNodes);
        }
        String databaseStrategy = tableElement.getAttribute(ShardingDataSourceBeanDefinitionParserTag.DATABASE_STRATEGY_REF_ATTRIBUTE);
        if (!Strings.isNullOrEmpty(databaseStrategy)) {
            factory.addPropertyReference("databaseShardingStrategyConfig", databaseStrategy);
        }
        String tableStrategy = tableElement.getAttribute(ShardingDataSourceBeanDefinitionParserTag.TABLE_STRATEGY_REF_ATTRIBUTE);
        if (!Strings.isNullOrEmpty(tableStrategy)) {
            factory.addPropertyReference("tableShardingStrategyConfig", tableStrategy);
        }
        String keyGeneratorColumnName = tableElement.getAttribute(ShardingDataSourceBeanDefinitionParserTag.GENERATE_KEY_COLUMN);
        if (!Strings.isNullOrEmpty(keyGeneratorColumnName)) {
            factory.addPropertyValue("keyGeneratorColumnName", keyGeneratorColumnName);
        }
        String keyGeneratorClass = tableElement.getAttribute(ShardingDataSourceBeanDefinitionParserTag.COLUMN_KEY_GENERATOR_CLASS);
        if (!Strings.isNullOrEmpty(keyGeneratorClass)) {
            factory.addPropertyValue("keyGeneratorClass", keyGeneratorClass);
        }
        String logicIndex = tableElement.getAttribute(ShardingDataSourceBeanDefinitionParserTag.LOGIC_INDEX);
        if (!Strings.isNullOrEmpty(logicIndex)) {
            factory.addPropertyValue("logicIndex", logicIndex);
        }
        return factory.getBeanDefinition();
    }
    
    private List<String> parseBindingTablesConfig(final Element element) {
        Element bindingTableRulesElement = DomUtils.getChildElementByTagName(element, ShardingDataSourceBeanDefinitionParserTag.BINDING_TABLE_RULES_TAG);
        if (null == bindingTableRulesElement) {
            return Collections.emptyList();
        }
        List<Element> bindingTableRuleElements = DomUtils.getChildElementsByTagName(bindingTableRulesElement, ShardingDataSourceBeanDefinitionParserTag.BINDING_TABLE_RULE_TAG);
        List<String> result = new LinkedList<>();
        for (Element bindingTableRuleElement : bindingTableRuleElements) {
            result.add(bindingTableRuleElement.getAttribute(ShardingDataSourceBeanDefinitionParserTag.LOGIC_TABLES_ATTRIBUTE));
        }
        return result;
    }
    
    private Map parseConfigMap(final Element element, final ParserContext parserContext, final BeanDefinition beanDefinition) {
        Element dataElement = DomUtils.getChildElementByTagName(element, ShardingDataSourceBeanDefinitionParserTag.CONFIG_MAP_TAG);
        return null == dataElement ? Collections.<String, Class<?>>emptyMap() : parserContext.getDelegate().parseMapElement(dataElement, beanDefinition);
    }
    
    private Properties parseProperties(final Element element, final ParserContext parserContext) {
        Element propsElement = DomUtils.getChildElementByTagName(element, ShardingDataSourceBeanDefinitionParserTag.PROPS_TAG);
        return null == propsElement ? new Properties() : parserContext.getDelegate().parsePropsElement(propsElement);
    }
}
