package org.apache.wayang.flink.operators;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.flink.channels.DataStreamChannel;
import org.apache.wayang.java.channels.CollectionChannel;
import org.junit.Test;

public class FlinkDataStreamTests extends FlinkOperatorTestBase {
    @Test
    public void singleOperatorTest() throws Exception {
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

        final FlinkDataStreamCollectionSink<String> collectionSink = new FlinkDataStreamCollectionSink<>(DataSetType.createDefault(String.class));
        final CollectionChannel.Instance sinkOutput = this.createCollectionChannelInstance();

        // Set up the ChannelInstances.
        final ChannelInstance[] sinkInputs = new ChannelInstance[] { sourceOutput };
        final ChannelInstance[] sinkOutputs = new ChannelInstance[] { sinkOutput };

        // Execute.
        this.evaluate(collectionSink, sinkInputs, sinkOutputs);

        assertTrue(sinkOutput.provideCollection().size() > 0);
    }
}
