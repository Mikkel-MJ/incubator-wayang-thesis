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

import org.apache.spark.api.java.JavaRDD;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.JsonSerializable;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.json.WayangJsonObj;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.operators.SqlToStreamOperator.ResultSetIterator;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.operators.SparkExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.basic.operators.JoinOperator;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SqlToRddOperator extends UnaryToUnaryOperator<Record, Record>
        implements SparkExecutionOperator, JsonSerializable {

    private final JdbcPlatformTemplate jdbcPlatform;

    public SqlToRddOperator(final JdbcPlatformTemplate jdbcPlatform) {
        this(jdbcPlatform, DataSetType.createDefault(Record.class));
    }

    public SqlToRddOperator(final JdbcPlatformTemplate jdbcPlatform, final DataSetType<Record> dataSetType) {
        super(dataSetType, dataSetType, false);
        this.jdbcPlatform = jdbcPlatform;
    }

    protected SqlToRddOperator(final SqlToRddOperator that) {
        super(that);
        this.jdbcPlatform = that.jdbcPlatform;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        return Collections.singletonList(this.jdbcPlatform.getSqlQueryChannelDescriptor());
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final SparkExecutor executor,
            final OptimizationContext.OperatorContext operatorContext) {
        // Cast the inputs and outputs.
        final SqlQueryChannel.Instance input = (SqlQueryChannel.Instance) inputs[0];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final JdbcPlatformTemplate producerPlatform = (JdbcPlatformTemplate) input.getChannel().getProducer()
                .getPlatform();
        final Connection connection = producerPlatform
                .createDatabaseDescriptor(executor.getConfiguration())
                .createJdbcConnection();

        final Operator boundaryOperator = input.getChannel().getProducer().getOperator();

        //TODO: verify this is closed correctly or close in finally
        final ResultSetIterator<Record> resultSetIterator = new SqlToStreamOperator.ResultSetIterator<>(connection,
                input.getSqlQuery(), boundaryOperator instanceof JoinOperator);
        final Iterable<Record> resultSetIterable = () -> resultSetIterator;

        // Convert the ResultSet to a JavaRDD.
        final JavaRDD<Record> resultSetRDD = executor.sc.parallelize(
                StreamSupport.stream(resultSetIterable.spliterator(), false).onClose(resultSetIterator::close).collect(Collectors.toList()),
                executor.getNumDefaultPartitions());

        output.accept(resultSetRDD, executor);

        // TODO: Add load profile estimators
        final ExecutionLineageNode queryLineageNode = new ExecutionLineageNode(operatorContext);
        queryLineageNode.addPredecessor(input.getLineage());
        final ExecutionLineageNode outputLineageNode = new ExecutionLineageNode(operatorContext);
        output.getLineage().addPredecessor(outputLineageNode);

        return queryLineageNode.collectAndMark();
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    public WayangJsonObj toJson() {
        return null;
    }

    @Override
    public boolean isConversion() {
        return true;
    }
}
