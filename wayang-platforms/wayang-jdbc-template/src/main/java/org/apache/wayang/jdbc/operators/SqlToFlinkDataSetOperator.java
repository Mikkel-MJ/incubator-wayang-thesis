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
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.JsonSerializable;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.json.WayangJsonObj;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.apache.wayang.flink.execution.FlinkExecutor;
import org.apache.wayang.flink.operators.FlinkExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.MapOperator;

import org.apache.flink.api.java.DataSet;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.stream.Stream;

public class SqlToFlinkDataSetOperator<Input, Output> extends UnaryToUnaryOperator<Input, Output> implements FlinkExecutionOperator, JsonSerializable {

    private final JdbcPlatformTemplate jdbcPlatform;

    public SqlToFlinkDataSetOperator(
        JdbcPlatformTemplate jdbcPlatform,
        final DataSetType<Input> inputDataSetType,
        final DataSetType<Output> outputDataSetType
    ) {
        super(inputDataSetType, outputDataSetType, false);
        this.jdbcPlatform = jdbcPlatform;
    }

    protected SqlToFlinkDataSetOperator(SqlToFlinkDataSetOperator that) {
        super(that);
        this.jdbcPlatform = that.jdbcPlatform;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(this.jdbcPlatform.getSqlQueryChannelDescriptor());
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        // Cast the inputs and outputs.
        final SqlQueryChannel.Instance input = (SqlQueryChannel.Instance) inputs[0];
        final DataSetChannel.Instance output = (DataSetChannel.Instance) outputs[0];

        JdbcPlatformTemplate producerPlatform = (JdbcPlatformTemplate) input.getChannel().getProducer().getPlatform();
        final Connection connection = producerPlatform
                .createDatabaseDescriptor(flinkExecutor.getConfiguration())
                .createJdbcConnection();

        final Operator boundaryOperator = input.getChannel().getProducer().getOperator();

        Iterator<Output> resultSetIterator = new SqlToStreamOperator.ResultSetIterator<Output>(connection, input.getSqlQuery(), boundaryOperator);
        Iterable<Output> resultSetIterable = () -> resultSetIterator;
        //final Spliterator<Output> resultSetSpliterator = Spliterators.spliteratorUnknownSize(resultSetIterator, 0);
        final Stream<Output> resultSetStream = StreamSupport.stream(resultSetIterable.spliterator(), false);

        // Convert the ResultSet to a Flink DataSet.
        final Collection<Output> collected = resultSetStream.collect(Collectors.toList());
        System.out.println("SqlToFlink size: " + collected.size());
        System.out.println("FEE parallelism: " + flinkExecutor.fee.getParallelism());


        final DataSet<Output> resultSetDataSet = flinkExecutor.fee.fromCollection(
            collected
        ).setParallelism(flinkExecutor.fee.getParallelism());

        /*
        final DataSet<Record> resultSetDataSet = flinkExecutor.fee.fromParallelCollection(resultSetSpliterator, Record.class)
            .setParallelism(flinkExecutor.fee.getParallelism());
        */
        output.accept(resultSetDataSet, flinkExecutor);

        // TODO: Add load profile estimators
        ExecutionLineageNode queryLineageNode = new ExecutionLineageNode(operatorContext);
        queryLineageNode.addPredecessor(input.getLineage());
        ExecutionLineageNode outputLineageNode = new ExecutionLineageNode(operatorContext);
        output.getLineage().addPredecessor(outputLineageNode);

        return queryLineageNode.collectAndMark();
        //return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.flink.sql.dataset.load";
    }

    @Override
    public WayangJsonObj toJson() {
        return new WayangJsonObj().put("platform", this.jdbcPlatform.getClass().getCanonicalName());
    }

    @Override public boolean isConversion() {
        return true;
    }
}
