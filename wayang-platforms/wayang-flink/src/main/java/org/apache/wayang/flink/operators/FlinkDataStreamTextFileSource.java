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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.core.optimizer.OptimizationContext.OperatorContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataStreamChannel;
import org.apache.wayang.flink.execution.FlinkExecutor;

import java.util.Collection;
import java.util.List;

public class FlinkDataStreamTextFileSource extends TextFileSource implements FlinkExecutionOperator {

    public FlinkDataStreamTextFileSource(final TextFileSource that) {
        super(that);
    }

    public FlinkDataStreamTextFileSource(final String inputUrl) {
        super(inputUrl);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        throw new UnsupportedOperationException("Unimplemented method 'getSupportedInputChannels'");
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        throw new UnsupportedOperationException("Unimplemented method 'getSupportedOutputChannels'");
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(final ChannelInstance[] inputs,
            final ChannelInstance[] outputs, final FlinkExecutor flinkExecutor, final OperatorContext operatorContext)
            throws Exception {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final DataStreamChannel.Instance output = (DataStreamChannel.Instance) outputs[0];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final FileSource<String> fs = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(this.getInputUrl()))
                .build();

        final DataStream<String> dataStream = env.fromSource(fs, WatermarkStrategy.noWatermarks(),
                "FlinkDataStreamFileSource[" + this.getInputUrl() + "]");

        output.accept(dataStream);

        final ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
        prepareLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.flink.textfilesource.load.prepare", flinkExecutor.getConfiguration()));

        final ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
        mainLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.flink.textfilesource.load.main", flinkExecutor.getConfiguration()));
                
        output.getLineage().addPredecessor(mainLineageNode);

        return prepareLineageNode.collectAndMark();
    }

    @Override
    public boolean containsAction() {
        throw new UnsupportedOperationException("Unimplemented method 'containsAction'");
    }

}
