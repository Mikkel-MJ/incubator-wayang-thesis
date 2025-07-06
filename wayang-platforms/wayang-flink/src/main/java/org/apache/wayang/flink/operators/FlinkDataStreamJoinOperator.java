package org.apache.wayang.flink.operators;

import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
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

    /*
     * inspired by Flink Cookbook
     */
    private static final <K, I0, I1> KeyedCoProcessFunction<K, I0, I1, Tuple2<I0, I1>> joinFunction(
            final Class<I0> input0Class, final Class<I1> input1Class) {
        return new KeyedCoProcessFunction<K, I0, I1, Tuple2<I0, I1>>() {
            private transient ValueState<I0> savedValue0State;
            private transient ValueState<I1> savedValue1State;

            @Override
            public void open(final Configuration parameters) throws Exception {
                super.open(parameters);
                savedValue0State = getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("savedValue0State", input0Class));
                savedValue1State = getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("savedValue1State", input1Class));
            }

            @Override
            public void processElement1(final I0 value,
                    final KeyedCoProcessFunction<K, I0, I1, Tuple2<I0, I1>>.Context ctx,
                    final Collector<Tuple2<I0, I1>> out) throws Exception {
                final I1 i1 = savedValue1State.value();
                if (i1 == null) {
                    savedValue0State.update(value);
                } else {
                    out.collect(new Tuple2<>(value, i1));
                }
            }

            @Override
            public void processElement2(final I1 value,
                    final KeyedCoProcessFunction<K, I0, I1, Tuple2<I0, I1>>.Context ctx,
                    final Collector<Tuple2<I0, I1>> out) throws Exception {
                final I0 i0 = savedValue0State.value();
                if (i0 == null) {
                    savedValue1State.update(value);
                } else {
                    out.collect(new Tuple2<>(i0, value));
                }
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
    public FlinkDataStreamJoinOperator(DataSetType<I0> inputType0,
                             DataSetType<I1> inputType1,
                             TransformationDescriptor<I0, K> keyDescriptor0,
                             TransformationDescriptor<I1, K> keyDescriptor1) {
        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
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
                .process(joinFunction(this.getInputType0().getDataUnitType().getTypeClass(),
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
