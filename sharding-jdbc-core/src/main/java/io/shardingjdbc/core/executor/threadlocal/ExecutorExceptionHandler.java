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

package io.shardingjdbc.core.executor.threadlocal;

import io.shardingjdbc.core.exception.ShardingJdbcException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

/**
 * Executor运行时异常处理器
 * 
 * @author zhangliang
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class ExecutorExceptionHandler {
    
    private static final ThreadLocal<Boolean> IS_EXCEPTION_THROWN = new ThreadLocal<>();
    
    /**
     * 设置发生异常时是否需要抛出去
     *
     * @param isExceptionThrown throw exception if error occur or not
     */
    public static void setExceptionThrown(final boolean isExceptionThrown) {
        ExecutorExceptionHandler.IS_EXCEPTION_THROWN.set(isExceptionThrown);
    }
    
    /**
     * 发生异常时获取是否要跑出去
     * 
     * @return throw exception if error occur or not
     */
    public static boolean isExceptionThrown() {
        return null == IS_EXCEPTION_THROWN.get() ? true : IS_EXCEPTION_THROWN.get();
    }
    
    /**
     * 异常处理
     * 
     * @param exception to be handled exception
     * @throws SQLException SQL exception
     */
    public static void handleException(final Exception exception) throws SQLException {
        if (isExceptionThrown()) {
            if (exception instanceof SQLException) {
                throw (SQLException) exception;
            }
            throw new ShardingJdbcException(exception);
        }
        log.error("exception occur: ", exception);
    }
}
