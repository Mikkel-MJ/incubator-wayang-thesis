package org.apache.wayang.flink.operators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.flink.channels.DataStreamChannel;
import org.apache.wayang.java.channels.CollectionChannel;

import org.junit.Test;

public class FlinkDataStreamTests extends FlinkOperatorTestBase {
    @Test
    public void textFileSourceTest() throws Exception {
        final String path = FlinkDataStreamTests.class.getResource("dataStreamTest.txt").getPath();

        final FlinkDataStreamTextFileSource collectionSource = new FlinkDataStreamTextFileSource(path);
        final DataStreamChannel.Instance output = this.createDataStreamChannelInstance();

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[] {};
        final ChannelInstance[] outputs = new ChannelInstance[] { output };

        // Execute.
        this.evaluate(collectionSource, inputs, outputs);

        final DataStream<String> stream = output.<String>provideDataStream();
        final Iterator<String> str = stream.executeAndCollect();

        final ArrayList<String> collection = new ArrayList<>();
        str.forEachRemaining(collection::add);

        assertTrue(collection.size() > 0);
    }

    @Test
    public void javaConversion() throws Exception {
        final String path = FlinkDataStreamTests.class.getResource("dataStreamTest.txt").getPath();

        final FlinkDataStreamTextFileSource collectionSource = new FlinkDataStreamTextFileSource(path);
        final DataStreamChannel.Instance sourceOutput = this.createDataStreamChannelInstance();

        // Set up the ChannelInstances.
        final ChannelInstance[] sourceInputs = new ChannelInstance[] {};
        final ChannelInstance[] sourceOutputs = new ChannelInstance[] { sourceOutput };

        // Execute.
        this.evaluate(collectionSource, sourceInputs, sourceOutputs);

        final FlinkDataStreamCollectionSink<String> collectionSink = new FlinkDataStreamCollectionSink<>(
                DataSetType.createDefault(String.class));
        final CollectionChannel.Instance sinkOutput = this.createCollectionChannelInstance();

        // Set up the ChannelInstances.
        final ChannelInstance[] sinkInputs = new ChannelInstance[] { sourceOutput };
        final ChannelInstance[] sinkOutputs = new ChannelInstance[] { sinkOutput };

        // Execute.
        this.evaluate(collectionSink, sinkInputs, sinkOutputs);

        assertTrue(sinkOutput.provideCollection().size() > 0);
    }

    @Test
    public void mapTest() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set up channels
        final DataStreamChannel.Instance input = this.createDataStreamChannelInstance();
        input.accept(env.fromData(1, 2, 3, 4));
        final DataStreamChannel.Instance output = this.createDataStreamChannelInstance();

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[] { input };
        final ChannelInstance[] outputs = new ChannelInstance[] { output };

        // Set up MapOperator
        final SerializableFunction<Integer, Integer> add = i -> i + 5;
        final FlinkDataStreamMapOperator<Integer, Integer> map = new FlinkDataStreamMapOperator<Integer, Integer>(add,
                Integer.class, Integer.class);

        // Execute.
        this.evaluate(map, inputs, outputs);

        final DataStream<Integer> stream = output.<Integer>provideDataStream();
        final Iterator<Integer> ints = stream.executeAndCollect();

        final ArrayList<Integer> collection = new ArrayList<>();
        ints.forEachRemaining(collection::add);

        assertTrue(collection.stream().allMatch(i -> i > 5));
    }

    @Test
    public void joinTest() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set up channels
        final DataStreamChannel.Instance input1 = this.createDataStreamChannelInstance();
        input1.accept(env.fromData(new Tuple2<>(1, "b"), new Tuple2<>(1, "c"), new Tuple2<>(2, "d"), new Tuple2<>(3, "e")));
        final DataStreamChannel.Instance input2 = this.createDataStreamChannelInstance();
        input2.accept(env.fromData(new Tuple2<>("x", 1), new Tuple2<>("y", 1), new Tuple2<>("z", 2), new Tuple2<>("w", 4)));

        final DataStreamChannel.Instance output = this.createDataStreamChannelInstance();

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[] { input1, input2 };
        final ChannelInstance[] outputs = new ChannelInstance[] { output };

        // Set up JoinOperator
        final ProjectionDescriptor<Tuple2<Integer, String>, Integer> left = new ProjectionDescriptor<>(
                DataUnitType.createBasicUnchecked(Tuple2.class),
                DataUnitType.createBasic(Integer.class),
                "field0");
        final ProjectionDescriptor<Tuple2<String, Integer>, Integer> right = new ProjectionDescriptor<>(
                DataUnitType.createBasicUnchecked(Tuple2.class),
                DataUnitType.createBasic(Integer.class),
                "field1");
        final FlinkDataStreamJoinOperator<Tuple2<Integer, String>, Tuple2<String, Integer>, Integer> join = new FlinkDataStreamJoinOperator<>(left, right);

        // Execute.
        this.evaluate(join, inputs, outputs);

        final DataStream<Tuple2<?,?>> stream = output.<Tuple2<?,?>>provideDataStream();
        final Iterator<Tuple2<?,?>> ints = stream.executeAndCollect();

        final ArrayList<Tuple2<?,?>> collection = new ArrayList<>();
        ints.forEachRemaining(collection::add);

        System.out.println("got result: " + collection);
        assertEquals(5, collection.size());
        assertTrue(collection.stream().anyMatch(res -> res.equals(new Tuple2<>(new Tuple2<>(1, "b"), new Tuple2<>("x", 1)))));
        assertTrue(collection.stream().anyMatch(res -> res.equals(new Tuple2<>(new Tuple2<>(1, "b"), new Tuple2<>("y", 1)))));
        assertTrue(collection.stream().anyMatch(res -> res.equals(new Tuple2<>(new Tuple2<>(1, "c"), new Tuple2<>("x", 1)))));
        assertTrue(collection.stream().anyMatch(res -> res.equals(new Tuple2<>(new Tuple2<>(1, "c"), new Tuple2<>("y", 1)))));
        assertTrue(collection.stream().anyMatch(res -> res.equals(new Tuple2<>(new Tuple2<>(2, "d"), new Tuple2<>("z", 2)))));
    }
}
