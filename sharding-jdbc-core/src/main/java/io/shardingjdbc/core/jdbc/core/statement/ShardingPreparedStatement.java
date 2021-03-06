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

package io.shardingjdbc.core.jdbc.core.statement;

import io.shardingjdbc.core.constant.SQLType;
import io.shardingjdbc.core.executor.type.batch.BatchPreparedStatementExecutor;
import io.shardingjdbc.core.executor.type.batch.BatchPreparedStatementUnit;
import io.shardingjdbc.core.executor.type.prepared.PreparedStatementExecutor;
import io.shardingjdbc.core.executor.type.prepared.PreparedStatementUnit;
import io.shardingjdbc.core.jdbc.adapter.AbstractShardingPreparedStatementAdapter;
import io.shardingjdbc.core.jdbc.core.connection.ShardingConnection;
import io.shardingjdbc.core.jdbc.core.resultset.GeneratedKeysResultSet;
import io.shardingjdbc.core.jdbc.core.resultset.ShardingResultSet;
import io.shardingjdbc.core.merger.MergeEngine;
import io.shardingjdbc.core.parsing.parser.context.GeneratedKey;
import io.shardingjdbc.core.parsing.parser.sql.dml.insert.InsertStatement;
import io.shardingjdbc.core.parsing.parser.sql.dql.select.SelectStatement;
import io.shardingjdbc.core.routing.PreparedStatementRoutingEngine;
import io.shardingjdbc.core.routing.SQLExecutionUnit;
import io.shardingjdbc.core.routing.SQLRouteResult;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import lombok.AccessLevel;
import lombok.Getter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * PreparedStatement that support sharding.
 * 
 * @author zhangliang
 * @author caohao
 */
@Getter
public final class ShardingPreparedStatement extends AbstractShardingPreparedStatementAdapter {
    
    private final ShardingConnection connection;
    
    private final int resultSetType;
    
    private final int resultSetConcurrency;
    
    private final int resultSetHoldability;
    
    private final PreparedStatementRoutingEngine routingEngine;
    
    private final List<BatchPreparedStatementUnit> batchStatementUnits = new LinkedList<>();
    
    private final List<List<Object>> parameterSets = new LinkedList<>();

    /**
     * 路由结束后的表达式集合
     */
    private final Collection<PreparedStatement> routedStatements = new LinkedList<>();
    
    @Getter(AccessLevel.NONE)
    private boolean returnGeneratedKeys;
    
    @Getter(AccessLevel.NONE)
    private SQLRouteResult routeResult;
    
    @Getter(AccessLevel.NONE)
    private ResultSet currentResultSet;

    /**
     * 构建分片preparedStatement
     * @param connection
     * @param sql
     */
    public ShardingPreparedStatement(final ShardingConnection connection, final String sql) {
        this(connection, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }
    
    public ShardingPreparedStatement(final ShardingConnection connection, final String sql, final int resultSetType, final int resultSetConcurrency) {
        this(connection, sql, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }
    
    public ShardingPreparedStatement(final ShardingConnection connection, final String sql, final int autoGeneratedKeys) {
        this(connection, sql);
        if (Statement.RETURN_GENERATED_KEYS == autoGeneratedKeys) {
            returnGeneratedKeys = true;
        }
    }
    
    public ShardingPreparedStatement(final ShardingConnection connection, final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) {
        this.connection = connection;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
        routingEngine = new PreparedStatementRoutingEngine(sql, connection.getShardingContext());
    }
    
    @Override
    public ResultSet executeQuery() throws SQLException {
        ResultSet result;
        try {
            Collection<PreparedStatementUnit> preparedStatementUnits = route();
            List<ResultSet> resultSets = new PreparedStatementExecutor(
                    getConnection().getShardingContext().getExecutorEngine(), routeResult.getSqlStatement().getType(), preparedStatementUnits, getParameters()).executeQuery();
            result = new ShardingResultSet(resultSets, new MergeEngine(resultSets, (SelectStatement) routeResult.getSqlStatement()).merge(), this);
        } finally {
            clearBatch();
        }
        currentResultSet = result;
        return result;
    }
    
    @Override
    public int executeUpdate() throws SQLException {
        try {
            Collection<PreparedStatementUnit> preparedStatementUnits = route();
            return new PreparedStatementExecutor(
                    getConnection().getShardingContext().getExecutorEngine(), routeResult.getSqlStatement().getType(), preparedStatementUnits, getParameters()).executeUpdate();
        } finally {
            clearBatch();
        }
    }
    
    @Override
    public boolean execute() throws SQLException {
        try {
            Collection<PreparedStatementUnit> preparedStatementUnits = route();
            return new PreparedStatementExecutor(
                    getConnection().getShardingContext().getExecutorEngine(), routeResult.getSqlStatement().getType(), preparedStatementUnits, getParameters()).execute();
        } finally {
            clearBatch();
        }
    }
    
    private Collection<PreparedStatementUnit> route() throws SQLException {
        Collection<PreparedStatementUnit> result = new LinkedList<>();
        // 路由引擎进行路由，返回路由的结果
        routeResult = routingEngine.route(getParameters());
        for (SQLExecutionUnit each : routeResult.getExecutionUnits()) {
            SQLType sqlType = routeResult.getSqlStatement().getType();
            Collection<PreparedStatement> preparedStatements;
            if (SQLType.DDL == sqlType) {
                preparedStatements = generatePreparedStatementForDDL(each);
            } else {
                // 返回单个PreparedStatement
                preparedStatements = Collections.singletonList(generatePreparedStatement(each));
            }
            routedStatements.addAll(preparedStatements);
            for (PreparedStatement preparedStatement : preparedStatements) {
                replaySetParameter(preparedStatement);
                // 封装结果
                result.add(new PreparedStatementUnit(each, preparedStatement));
            }
        }
        return result;
    }
    
    private Collection<PreparedStatement> generatePreparedStatementForDDL(final SQLExecutionUnit sqlExecutionUnit) throws SQLException {
        Collection<PreparedStatement> result = new LinkedList<>();
        Collection<Connection> connections = getConnection().getAllConnections(sqlExecutionUnit.getDataSource());
        for (Connection each : connections) {
            result.add(each.prepareStatement(sqlExecutionUnit.getSql(), resultSetType, resultSetConcurrency, resultSetHoldability));
        }
        return result;
    }
    
    private PreparedStatement generatePreparedStatement(final SQLExecutionUnit sqlExecutionUnit) throws SQLException {
        Connection connection = getConnection().getConnection(sqlExecutionUnit.getDataSource(), routeResult.getSqlStatement().getType());
        return returnGeneratedKeys ? connection.prepareStatement(sqlExecutionUnit.getSql(), Statement.RETURN_GENERATED_KEYS)
                : connection.prepareStatement(sqlExecutionUnit.getSql(), resultSetType, resultSetConcurrency, resultSetHoldability);
    }
    
    @Override
    public void clearBatch() throws SQLException {
        currentResultSet = null;
        clearParameters();
        batchStatementUnits.clear();
        parameterSets.clear();
    }
    
    @Override
    public void addBatch() throws SQLException {
        try {
            for (BatchPreparedStatementUnit each : routeBatch()) {
                each.getStatement().addBatch();
                each.mapAddBatchCount(parameterSets.size());
            }
            parameterSets.add(getParameters());
        } finally {
            currentResultSet = null;
            clearParameters();
        }
    }
    
    private List<BatchPreparedStatementUnit> routeBatch() throws SQLException {
        List<BatchPreparedStatementUnit> result = new ArrayList<>();
        routeResult = routingEngine.route(getParameters());
        for (SQLExecutionUnit each : routeResult.getExecutionUnits()) {
            BatchPreparedStatementUnit batchStatementUnit = getPreparedBatchStatement(each);
            replaySetParameter(batchStatementUnit.getStatement());
            result.add(batchStatementUnit);
        }
        return result;
    }
    
    private BatchPreparedStatementUnit getPreparedBatchStatement(final SQLExecutionUnit sqlExecutionUnit) throws SQLException {
        Optional<BatchPreparedStatementUnit> preparedBatchStatement = Iterators.tryFind(batchStatementUnits.iterator(), new Predicate<BatchPreparedStatementUnit>() {
            
            @Override
            public boolean apply(final BatchPreparedStatementUnit input) {
                return Objects.equals(input.getSqlExecutionUnit(), sqlExecutionUnit);
            }
        });
        if (preparedBatchStatement.isPresent()) {
            return preparedBatchStatement.get();
        }
        BatchPreparedStatementUnit result = new BatchPreparedStatementUnit(sqlExecutionUnit, generatePreparedStatement(sqlExecutionUnit));
        batchStatementUnits.add(result);
        return result;
    }
    
    @Override
    public int[] executeBatch() throws SQLException {
        try {
            return new BatchPreparedStatementExecutor(getConnection().getShardingContext().getExecutorEngine(), 
                    getConnection().getShardingContext().getDatabaseType(), routeResult.getSqlStatement().getType(), batchStatementUnits, parameterSets).executeBatch();
        } finally {
            clearBatch();
        }
    }
    
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        Optional<GeneratedKey> generatedKey = getGeneratedKey();
        if (returnGeneratedKeys && generatedKey.isPresent()) {
            return new GeneratedKeysResultSet(routeResult.getGeneratedKeys().iterator(), generatedKey.get().getColumn(), this);
        }
        if (1 == routedStatements.size()) {
            return routedStatements.iterator().next().getGeneratedKeys();
        }
        return new GeneratedKeysResultSet();
    }
    
    private Optional<GeneratedKey> getGeneratedKey() {
        if (null != routeResult && routeResult.getSqlStatement() instanceof InsertStatement) {
            return Optional.fromNullable(((InsertStatement) routeResult.getSqlStatement()).getGeneratedKey());
        }
        return Optional.absent();
    }
    
    @Override
    public ResultSet getResultSet() throws SQLException {
        if (null != currentResultSet) {
            return currentResultSet;
        }
        if (1 == routedStatements.size()) {
            currentResultSet = routedStatements.iterator().next().getResultSet();
            return currentResultSet;
        }
        List<ResultSet> resultSets = new ArrayList<>(routedStatements.size());
        for (PreparedStatement each : routedStatements) {
            resultSets.add(each.getResultSet());
        }
        currentResultSet = new ShardingResultSet(resultSets, new MergeEngine(resultSets, (SelectStatement) routeResult.getSqlStatement()).merge(), this);
        return currentResultSet;
    }
}
