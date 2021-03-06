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

package io.shardingjdbc.core.jdbc.core;

import io.shardingjdbc.core.rule.ShardingRule;
import io.shardingjdbc.core.constant.DatabaseType;
import io.shardingjdbc.core.executor.ExecutorEngine;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 分片运行时上下文
 * 
 * @author gaohongtao
 */
@RequiredArgsConstructor
@Getter
public final class ShardingContext {

    /**
     * 分片规则
     */
    private final ShardingRule shardingRule;

    /**
     * 数据源乐行
     */
    private final DatabaseType databaseType;

    /**
     * 任务执行引擎
     */
    private final ExecutorEngine executorEngine;

    /**
     * 是否打印sql用来debug
     */
    private final boolean showSQL;
}
