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

package io.shardingjdbc.core.constant;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 分片属性常量
 * 
 * @author gaohongtao
 * @author caohao
 */
@RequiredArgsConstructor
@Getter
public enum ShardingPropertiesConstant {
    
    /**
     * 开启或不开启显示SQL详细信息
     *
     * 打印SQL详细信息能更好的帮助开发者debug
     * 详细信息包括：逻辑SQL，解析上下文以及重写出真实的SQL集合
     * 指定这个属性将使得log打印到主题Sharding-JDBC-SQL中，日志级别为INFO
     * 默认：false
     */
    SQL_SHOW("sql.show", Boolean.FALSE.toString(), boolean.class),
    
    /**
     * worker线程最大大小
     *
     * 执行SQL Statement和PrepareStatement将使用线程池
     * 一个分片的数据源将使用独立的线程池，甚至同一个JVM的不同数据源都不共享线程池
     * 默认：和CPU核数相同
     */
    EXECUTOR_SIZE("executor.size", String.valueOf(Runtime.getRuntime().availableProcessors()), int.class);
    
    private final String key;
    
    private final String defaultValue;
    
    private final Class<?> type;
    
    /**
     * 找出指定key对应的实例对象
     * 
     * @param key 属性key
     * @return value enum, return {@code null} if not found
     */
    public static ShardingPropertiesConstant findByKey(final String key) {
        for (ShardingPropertiesConstant each : ShardingPropertiesConstant.values()) {
            if (each.getKey().equals(key)) {
                return each;
            }
        }
        return null;
    }
}
