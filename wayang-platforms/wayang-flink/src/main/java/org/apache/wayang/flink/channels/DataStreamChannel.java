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

package org.apache.wayang.flink.channels;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.AbstractChannelInstance;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.flink.execution.FlinkExecutor;

import java.util.OptionalLong;

/**
 * Describes the situation where one {@link DataStream} is operated on,
 * producing a further {@link DataStream}.
 * <p>
 * <i>NB: We might be more specific: Distinguish between cached/uncached and
 * pipelined/aggregated.</i>
 * </p>
 */
public class DataStreamChannel extends Channel {

    /**
     * {@link ChannelInstance} implementation for {@link DataStream}s.
     */
    public class Instance extends AbstractChannelInstance {

        private DataStream<?> dataStream;

        private long size;

        public Instance(final FlinkExecutor executor,
                final OptimizationContext.OperatorContext producerOperatorContext,
                final int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        public void accept(final DataStream<?> dataStream) {
            this.dataStream = dataStream;
        }

        @SuppressWarnings("unchecked")
        public <T> DataStream<T> provideDataStream() {
            return (DataStream<T>) this.dataStream;
        }

        @Override
        public OptionalLong getMeasuredCardinality() {
            // TODO: size is always == 0
            return this.size == 0 ? super.getMeasuredCardinality() : OptionalLong.of(this.size);
        }

        @Override
        public DataStreamChannel getChannel() {
            return DataStreamChannel.this;
        }

        @Override
        protected void doDispose() {
            this.dataStream = null;
        }
    }

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(
            DataStreamChannel.class, true, false);

    public static final ChannelDescriptor DESCRIPTOR_MANY = new ChannelDescriptor(
            DataStreamChannel.class, true, false);

    public DataStreamChannel(final ChannelDescriptor descriptor, final OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
        assert descriptor == DESCRIPTOR || descriptor == DESCRIPTOR_MANY;
        this.markForInstrumentation();
    }

    private DataStreamChannel(final DataStreamChannel parent) {
        super(parent);
    }

    @Override
    public Channel copy() {
        return new DataStreamChannel(this);
    }

    @Override
    public Instance createInstance(final Executor executor,
            final OptimizationContext.OperatorContext producerOperatorContext,
            final int producerOutputIndex) {
        return new Instance((FlinkExecutor) executor, producerOperatorContext, producerOutputIndex);
    }
}
