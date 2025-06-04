/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.jdbc.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.JsonSerializable;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.json.WayangJsonObj;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.operators.JavaExecutionOperator;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.logging.log4j.LogManager;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.data.Tuple2;

import org.apache.commons.dbutils.DbUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This {@link Operator} converts {@link SqlQueryChannel}s to
 * {@link StreamChannel}s.
 */
public class SqlToStreamOperator<Input, Output> extends UnaryToUnaryOperator<Input, Output>
        implements JavaExecutionOperator, JsonSerializable {

    private final JdbcPlatformTemplate jdbcPlatform;

    /**
     * Creates a new instance.
     *
     * @param jdbcPlatform from which the SQL data comes
     * @param dataSetType  type of the {@link Record}s being transformed; see
     *                     {@link RecordType}
     */
    public SqlToStreamOperator(
            final JdbcPlatformTemplate jdbcPlatform,
            final DataSetType<Input> inputDataSetType,
            final DataSetType<Output> outputDataSetType) {
        super(inputDataSetType, outputDataSetType, false);
        this.jdbcPlatform = jdbcPlatform;
    }

    protected SqlToStreamOperator(final SqlToStreamOperator that) {
        super(that);
        this.jdbcPlatform = that.jdbcPlatform;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final JavaExecutor executor,
            final OptimizationContext.OperatorContext operatorContext) {
        // Cast the inputs and outputs.
        final SqlQueryChannel.Instance input = (SqlQueryChannel.Instance) inputs[0];
        final StreamChannel.Instance output = (StreamChannel.Instance) outputs[0];

        final JdbcPlatformTemplate producerPlatform = (JdbcPlatformTemplate) input.getChannel().getProducer().getPlatform();
        final Connection connection = producerPlatform
                .createDatabaseDescriptor(executor.getConfiguration())
                .createJdbcConnection();

        final Operator boundaryOperator = input.getChannel().getProducer().getOperator();

        final ResultSetIterator<Output> resultSetIterator = new ResultSetIterator<>(connection, input.getSqlQuery(), boundaryOperator instanceof JoinOperator);
        final Spliterator<Output> resultSetSpliterator = Spliterators.spliteratorUnknownSize(resultSetIterator, 0);
        final Stream<Output> resultSetStream = StreamSupport.stream(resultSetSpliterator, false).onClose(resultSetIterator::close);

        output.accept(resultSetStream);

        input.dispose();

        final ExecutionLineageNode queryLineageNode = new ExecutionLineageNode(operatorContext);
        queryLineageNode.add(LoadProfileEstimators.createFromSpecification(
                String.format("wayang.%s.sqltostream.load.query", this.jdbcPlatform.getPlatformId()),
                executor.getConfiguration()));
        queryLineageNode.addPredecessor(input.getLineage());
        final ExecutionLineageNode outputLineageNode = new ExecutionLineageNode(operatorContext);

        outputLineageNode.add(LoadProfileEstimators.createFromSpecification(
                String.format("wayang.%s.sqltostream.load.output", this.jdbcPlatform.getPlatformId()),
                executor.getConfiguration()));
        output.getLineage().addPredecessor(outputLineageNode);


        return queryLineageNode.collectAndMark();
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        return Collections.singletonList(this.jdbcPlatform.getSqlQueryChannelDescriptor());
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList(
                String.format("wayang.%s.sqltostream.load.query", this.jdbcPlatform.getPlatformId()),
                String.format("wayang.%s.sqltostream.load.output", this.jdbcPlatform.getPlatformId()));
    }

    /**
     * Exposes a {@link ResultSet} as an {@link Iterator}.
     */
    public static class ResultSetIterator<Output> implements Iterator<Output>, AutoCloseable {

        /**
         * Keeps around the {@link ResultSet} of the SQL query.
         */
        private ResultSet resultSet;

        private Statement statement;

        private Connection connection;

        /**
         * The next {@link Record} to be delivered via {@link #next()}.
         */
        private Output next;


        /**
         * Flag to know whether wrapping SqlQuery result into Tuple2 is needed.
         */
        private boolean needsTupleWrapping;

        /**
         * Creates a new instance.
         *
         * @param connection the JDBC connection on which to execute a SQL query
         * @param sqlQuery   the SQL query
         * @param needsTupleWrapping   flag indicating wrapping into Tuple2
         */
        public ResultSetIterator(
            final Connection connection,
            final String sqlQuery,
            final boolean needsTupleWrapping
        ) {
            try {
                // connection.setAutoCommit(false);
                this.connection = connection;
                this.statement = connection.createStatement();
                //TODO: REMOVE THIS IS ONLY FOR TESTING!!!!
                this.statement.setMaxRows(1_000_000);
                /*
                if (boundaryOperator instanceof JoinOperator) {
                }*/
                // st.setFetchSize(100000000);
                this.resultSet = this.statement.executeQuery(sqlQuery);
                this.needsTupleWrapping = needsTupleWrapping;
            } catch (final SQLException e) {
                this.close();
                throw new WayangException("Could not execute SQL. with query string: " + sqlQuery, e);
            }
            this.moveToNext();
        }

        /**
         * Moves this instance to the next {@link Record}.
         */
        private void moveToNext() {
            try {
                if (this.resultSet == null || !this.resultSet.next()) {
                    this.next = null;
                    this.close();
                } else {
                    final int recordWidth = this.resultSet.getMetaData().getColumnCount();
                    final Object[] values = new Object[recordWidth];
                    for (int i = 0; i < recordWidth; i++) {
                        values[i] = this.resultSet.getObject(i + 1);
                    }
                    if (this.needsTupleWrapping) {
                        this.next = (Output) new Tuple2<Record, Record>(new Record(values), new Record());
                    } else {
                        this.next = (Output) new Record(values);
                    }
                }
            } catch (final SQLException e) {
                this.next = null;
                this.close();
                throw new WayangException("Exception while iterating the result set.", e);
            }
        }

        @Override
        public boolean hasNext() {
            return this.next != null;
        }

        @Override
        public Output next() {
            final Output curNext = this.next;
            this.moveToNext();
            return curNext;
        }

        @Override
        public void close() {
            DbUtils.closeQuietly(this.connection);
            DbUtils.closeQuietly(this.resultSet);
            DbUtils.closeQuietly(this.statement);
        }
    }

    @Override
    public WayangJsonObj toJson() {
        return new WayangJsonObj().put("platform", this.jdbcPlatform.getClass().getCanonicalName());
    }

    @SuppressWarnings("unused")
    public static SqlToStreamOperator fromJson(final WayangJsonObj wayangJsonObj) {
        final String platformClassName = wayangJsonObj.getString("platform");
        final JdbcPlatformTemplate jdbcPlatform = ReflectionUtils.evaluate(platformClassName + ".getInstance()");
        return new SqlToStreamOperator(
            jdbcPlatform,
            DataSetType.createDefault(Record.class),
            DataSetType.createDefault(Record.class)
        );

    }

    @Override
    public boolean isConversion() {
        return true;
    }
}
