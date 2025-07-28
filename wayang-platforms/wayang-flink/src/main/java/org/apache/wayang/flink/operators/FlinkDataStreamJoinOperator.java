package org.apache.wayang.flink.operators;

import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext.OperatorContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataStreamChannel;
import org.apache.wayang.flink.compiler.FunctionCompiler;
import org.apache.wayang.flink.execution.FlinkExecutor;

public class FlinkDataStreamJoinOperator<I0, I1, K> extends JoinOperator<I0, I1, K> implements FlinkExecutionOperator {
    private static final <I0, I1> RichCoFlatMapFunction<I0, I1, Tuple2<I0, I1>> joinFunction(
            final Class<I0> input0Class, final Class<I1> input1Class) {
        return new RichCoFlatMapFunction<I0, I1, Tuple2<I0, I1>>() {
            private transient ListState<I0> i0State;
            private transient ListState<I1> i1State;

            @Override
            public void open(final Configuration parameters) throws Exception {
                super.open(parameters);
                i0State = getRuntimeContext()
                        .getListState(new ListStateDescriptor<>("i0State", input0Class));
                i1State = getRuntimeContext()
                        .getListState(new ListStateDescriptor<>("i1State", input1Class));
            }

            @Override
            public void flatMap1(final I0 i0, final Collector<Tuple2<I0, I1>> out) throws Exception {
                final Iterable<I1> i1s = i1State.get();

                for (final I1 i1 : i1s) {
                    out.collect(new Tuple2<>(i0, i1));
                }
                i0State.add(i0);
            }

            @Override
            public void flatMap2(final I1 i1, final Collector<Tuple2<I0, I1>> out) throws Exception {
                final Iterable<I0> i0s = i0State.get();

                for (final I0 i0 : i0s) {
                    out.collect(new Tuple2<>(i0, i1));
                }
                i1State.add(i1);
            }
        };
    }

    public FlinkDataStreamJoinOperator(final SerializableFunction<I0, K> keyExtractor0,
            final SerializableFunction<I1, K> keyExtractor1, final Class<I0> input0Class, final Class<I1> input1Class,
            final Class<K> keyClass) {
        super(keyExtractor0, keyExtractor1, input0Class, input1Class, keyClass);
    }

    /**
     * Creates a new instance.
     */
    public FlinkDataStreamJoinOperator(final DataSetType<I0> inputType0,
            final DataSetType<I1> inputType1,
            final TransformationDescriptor<I0, K> keyDescriptor0,
            final TransformationDescriptor<I1, K> keyDescriptor1) {
        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    /**
     * Creates a new instance.
     */
    public FlinkDataStreamJoinOperator(
            final TransformationDescriptor<I0, K> keyDescriptor0,
            final TransformationDescriptor<I1, K> keyDescriptor1) {
        super(keyDescriptor0, keyDescriptor1);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        throw new UnsupportedOperationException("Unimplemented method 'getSupportedInputChannels'");
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(final ChannelInstance[] inputs,
            final ChannelInstance[] kputs, final FlinkExecutor flinkExecutor, final OperatorContext operatorContext)
            throws Exception {
        assert inputs.length == this.getNumInputs();
        assert kputs.length == this.getNumOutputs();

        final DataStreamChannel.Instance input0 = (DataStreamChannel.Instance) inputs[0];
        final DataStreamChannel.Instance input1 = (DataStreamChannel.Instance) inputs[1];
        final DataStreamChannel.Instance output = (DataStreamChannel.Instance) kputs[0];

        final DataStream<I0> dataStream0 = input0.provideDataStream();
        final DataStream<I1> dataStream1 = input1.provideDataStream();

        final DataStream<?> outputStream = dataStream0
                .connect(dataStream1)
                .keyBy(FunctionCompiler.compileKeySelector(keyDescriptor0),
                        FunctionCompiler.compileKeySelector(keyDescriptor1))
                .flatMap(FlinkDataStreamJoinOperator.joinFunction(
                        this.getInputType0().getDataUnitType().getTypeClass(),
                        this.getInputType1().getDataUnitType().getTypeClass()));

        output.accept(outputStream);

        return ExecutionOperator.modelLazyExecution(inputs, kputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        throw new UnsupportedOperationException("Unimplemented method 'containsAction'");
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        throw new UnsupportedOperationException("Unimplemented method 'getSupportedOutputChannels'");
    }
}
