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

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.api.common.io.statistics.BaseStatistics;

import java.io.IOException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import java.io.Serializable;
import java.util.Collection;

public class BatchedIteratorInputFormat<T> extends RichInputFormat<T, GenericInputSplit> {

    private final Supplier<Iterator<T>> iteratorSupplier;
    private final int batchSize;
    private transient Iterator<T> iterator;
    private transient int itemsRead;

    public BatchedIteratorInputFormat(Supplier<Iterator<T>> iteratorSupplier, int batchSize) {
        this.iteratorSupplier = iteratorSupplier;
        this.batchSize = batchSize;
    }

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public void openInputFormat() {}

    @Override
    public void closeInputFormat() {}

    @Override
    public void open(GenericInputSplit split) throws IOException {
        // Get a fresh iterator — assume it's partitionable or pre-partitioned if running in parallel
        this.iterator = iteratorSupplier.get();
        this.itemsRead = 0;
    }

    @Override
    public boolean reachedEnd() {
        return !iterator.hasNext() || itemsRead >= batchSize;
    }

    @Override
    public T nextRecord(T reuse) {
        if (!iterator.hasNext() || itemsRead >= batchSize) {
            return null;
        }
        itemsRead++;
        return iterator.next();
    }

    @Override
    public void close() {}

    @Override
    public GenericInputSplit[] createInputSplits(int numSplits) {
        // Adjust if you want to pre-split the data source
        GenericInputSplit[] splits = new GenericInputSplit[numSplits];
        for (int i = 0; i < numSplits; i++) {
            splits[i] = new GenericInputSplit(i, numSplits);
        }
        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(GenericInputSplit[] splits) {
        return new DefaultInputSplitAssigner(splits);
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }


    public static class SerializableIteratorSupplier<T> implements Supplier<Iterator<T>>, Serializable {
        private final Collection<T> collection;

        public SerializableIteratorSupplier(Collection<T> collection) {
            this.collection = collection;
        }

        @Override
        public Iterator<T> get() {
            return collection.iterator();
        }
    }
}
