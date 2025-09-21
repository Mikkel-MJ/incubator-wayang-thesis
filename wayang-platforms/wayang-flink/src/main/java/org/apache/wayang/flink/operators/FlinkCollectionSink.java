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

package org.apache.wayang.flink.operators;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.apache.wayang.flink.execution.FlinkExecutor;
import org.apache.wayang.java.channels.CollectionChannel;
import com.esotericsoftware.kryo.Serializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.CollectionUtil;


import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;


/**
 * Converts {@link DataSetChannel} into a {@link CollectionChannel}
 */
public class FlinkCollectionSink<Type> extends UnaryToUnaryOperator<Type, Type>
        implements FlinkExecutionOperator  {
    public FlinkCollectionSink(DataSetType<Type> type) {
        super(type, type, false);
    }

    @Override
    public boolean isSink() {
        return true;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FlinkExecutor flinkExecutor, OptimizationContext.OperatorContext operatorContext) throws Exception {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final DataSetChannel.Instance input  = (DataSetChannel.Instance) inputs[0];
        final CollectionChannel.Instance output = (CollectionChannel.Instance) outputs[0];

        final DataSet<Type> dataSetInput = input.provideDataSet();
        TypeInformation<Type> type = dataSetInput.getType();

        TypeSerializerInputFormat<Type> in = new TypeSerializerInputFormat<>(type);

        Configuration config = flinkExecutor.getConfiguration();

        TypeSerializerOutputFormat<Type> out = new TypeSerializerOutputFormat<>();
        out.setOutputFilePath(new Path("file:///tmp/flink-data"));
        out.setWriteMode(WriteMode.OVERWRITE);

        dataSetInput.output(out);

        //dataSetInput.writeAsText("file:///tmp/flink-data", WriteMode.OVERWRITE);
        flinkExecutor.fee.execute();

        System.out.println("Finished writing to file");

        DataStream<Type> stream = flinkExecutor.sEnv.readFile(in, "file:///tmp/flink-data");

        System.out.println("Finished reading from file");


        List<Type> results = CollectionUtil.iteratorToList(stream.executeAndCollect());
        /*
        try (CloseableIterator<Type> it = stream.executeAndCollect()) {
            it.forEachRemaining(results::add);
        }*/

        System.out.println("Finished executing in Flink: " + results.size());
        System.out.println("Last: " + results.get(results.size() - 1));
        //output.accept(dataSetInput.collect());
        //output.accept(dataSetInput.first(10_000).collect());
        /*
        List<Type> results = new ArrayList<>();
        dataSetInput.output(new ListOutputFormat<>(results));
        flinkExecutor.fee.execute();
        System.out.println("Collected results: " + results.size());*/

        output.accept(results);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return true;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, 0, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, this.isSupportingBroadcastInputs(),
                inputCards -> inputCards[0]));
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.flink.collect.load";
    }

    @Override public boolean isConversion() {
        return true;
    }
}
