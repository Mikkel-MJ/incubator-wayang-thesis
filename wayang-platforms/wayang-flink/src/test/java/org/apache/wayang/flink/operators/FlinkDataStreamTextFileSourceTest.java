package org.apache.wayang.flink.operators;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.flink.channels.DataStreamChannel;

import org.junit.Test;

public class FlinkDataStreamTextFileSourceTest extends FlinkOperatorTestBase {
    @Test
    public void singleOperatorTest() throws Exception {
        final String path = FlinkDataStreamTextFileSourceTest.class.getResource("dataStreamTest.txt").getPath();

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
}
