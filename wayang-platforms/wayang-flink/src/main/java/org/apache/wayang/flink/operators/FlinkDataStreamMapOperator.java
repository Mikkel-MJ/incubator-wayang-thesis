package org.apache.wayang.flink.operators;

import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.optimizer.OptimizationContext.OperatorContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataStreamChannel;
import org.apache.wayang.flink.execution.FlinkExecutor;

public class FlinkDataStreamMapOperator<I, O> extends MapOperator<I, O> implements FlinkExecutionOperator {

    public FlinkDataStreamMapOperator(final SerializableFunction<I, O> function, final Class<I> inputTypeClass, final Class<O> outputTypeClass) {
        super(function, inputTypeClass, outputTypeClass);
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
            final ChannelInstance[] outputs, final FlinkExecutor flinkExecutor, final OperatorContext operatorContext) throws Exception {
        final DataStreamChannel.Instance input = (DataStreamChannel.Instance) inputs[0];
        final DataStreamChannel.Instance output = (DataStreamChannel.Instance) outputs[0];

        final DataStream<I> stream = input.provideDataStream();
        final MapFunction<I, O> mapper = flinkExecutor.getCompiler().compile(this.functionDescriptor);
        final DataStream<O> outputStream = stream.map(mapper).returns(this.getOutputType().getDataUnitType().getTypeClass());

        output.accept(outputStream);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        throw new UnsupportedOperationException("Unimplemented method 'containsAction'");
    }

}
