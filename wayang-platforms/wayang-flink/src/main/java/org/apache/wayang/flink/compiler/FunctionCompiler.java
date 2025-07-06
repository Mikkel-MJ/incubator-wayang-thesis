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

package org.apache.wayang.flink.compiler;

import java.util.Collection;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.MapPartitionsDescriptor;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.flink.compiler.criterion.WayangConvergenceCriterion;
import org.apache.wayang.flink.execution.FlinkExecutionContext;

/**
 * A compiler translates Wayang functions into executable Flink functions.
 */
public final class FunctionCompiler {
    private FunctionCompiler () {}

    /**
     * Compile a transformation.
     *
     * @param descriptor describes the transformation
     * @param <I>        input type of the transformation
     * @param <O>        output type of the transformation
     * @return a compiled function
     */
    public static <I, O> MapFunction<I, O> compile(final TransformationDescriptor<I, O> descriptor) {
        // This is a dummy method but shows the intention of having something compilable
        // in the descriptors.
        final Function<I, O> function = descriptor.getJavaImplementation();
        return function::apply;
    }

    /**
     * Compile a transformation.
     *
     * @param flatMapDescriptor describes the transformation
     * @param <I>               input type of the transformation
     * @param <O>               output type of the transformation
     * @return a compiled function
     */
    public static <I, O> FlatMapFunction<I, O> compile(
            final FunctionDescriptor.SerializableFunction<I, Iterable<O>> flatMapDescriptor) {
        return (t, collector) -> flatMapDescriptor.apply(t).forEach(collector::collect);
    }

    /**
     * Compile a reduction.
     *
     * @param descriptor describes the transformation
     * @param <T>        input/output type of the transformation
     * @return a compiled function
     */
    public static <T> ReduceFunction<T> compile(final ReduceDescriptor<T> descriptor) {
        // This is a dummy method but shows the intention of having something compilable
        // in the descriptors.
        final BinaryOperator<T> reduceFunction = descriptor.getJavaImplementation();
        return new ReduceFunction<T>() {
            @Override
            public T reduce(final T t, final T t1) throws Exception {
                return reduceFunction.apply(t, t1);
            }
        };
    }

    public static <T> FilterFunction<T> compile(final FunctionDescriptor.SerializablePredicate<T> predicateDescriptor) {
        return t -> predicateDescriptor.test(t);
    }

    public static <T> OutputFormat<T> compile(final FunctionDescriptor.SerializableConsumer<T> consumerDescriptor) {
        return new OutputFormatConsumer<>(consumerDescriptor);
    }

    public static <T, K> KeySelector<T, K> compileKeySelector(final TransformationDescriptor<T, K> descriptor) {
        return new KeySelectorFunction<>(descriptor);
    }

    public static <T0, T1, O> CoGroupFunction<T0, T1, O> compileCoGroup() {
        return new FlinkCoGroupFunction<>();
    }

    public static <T> TextOutputFormat.TextFormatter<T> compileOutput(
            final TransformationDescriptor<T, String> formattingDescriptor) {
        final Function<T, String> format = formattingDescriptor.getJavaImplementation();
        return format::apply;
    }

    /**
     * Compile a partition transformation.
     *
     * @param descriptor describes the transformation
     * @param <I>        input type of the transformation
     * @param <O>        output type of the transformation
     * @return a compiled function
     */
    public static <I, O> MapPartitionFunction<I, O> compile(final MapPartitionsDescriptor<I, O> descriptor) {
        final Function<Iterable<I>, Iterable<O>> function = descriptor.getJavaImplementation();
        return (iterable, collector) -> function.apply(iterable).forEach(collector::collect);
    }

    public static <T> WayangConvergenceCriterion<T> compile(final PredicateDescriptor<Collection<T>> descriptor) {
        final FunctionDescriptor.SerializablePredicate<Collection<T>> predicate = descriptor.getJavaImplementation();
        return new WayangConvergenceCriterion<>(predicate);
    }

    public static <I, O> RichFlatMapFunction<I, O> compile(
            final FunctionDescriptor.ExtendedSerializableFunction<I, Iterable<O>> flatMapDescriptor,
            final FlinkExecutionContext exe) {

        return new RichFlatMapFunction<I, O>() {
            @Override
            public void open(final Configuration parameters) throws Exception {
                flatMapDescriptor.open(exe);
            }

            @Override
            public void flatMap(final I value, final Collector<O> out) throws Exception {
                flatMapDescriptor.apply(value).forEach(out::collect);
            }
        };
    }

    public static <I, O> RichMapFunction<I, O> compile(final TransformationDescriptor<I, O> mapDescriptor,
            final FlinkExecutionContext fex) {
        assert mapDescriptor instanceof FunctionDescriptor.ExtendedSerializableFunction;
        final FunctionDescriptor.ExtendedSerializableFunction<I, O> map = (FunctionDescriptor.ExtendedSerializableFunction) mapDescriptor
                .getJavaImplementation();
        return new RichMapFunction<I, O>() {
            @Override
            public O map(final I value) throws Exception {
                return map.apply(value);
            }

            @Override
            public void open(final Configuration parameters) throws Exception {
                map.open(fex);
            }
        };
    }

    public static <I, O> RichMapPartitionFunction<I, O> compile(final MapPartitionsDescriptor<I, O> descriptor,
            final FlinkExecutionContext fex) {
        final FunctionDescriptor.ExtendedSerializableFunction<Iterable<I>, Iterable<O>> function = (FunctionDescriptor.ExtendedSerializableFunction<Iterable<I>, Iterable<O>>) descriptor
                .getJavaImplementation();
        return new RichMapPartitionFunction<I, O>() {
            @Override
            public void mapPartition(final Iterable<I> iterable, final Collector<O> collector) throws Exception {
                function.apply(iterable).forEach(
                        element -> {
                            collector.collect(element);
                        });

            }

            @Override
            public void open(final Configuration parameters) throws Exception {
                function.open(fex);
            }
        };
    }
}
