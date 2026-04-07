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

package org.apache.wayang.api.sql.calcite.converter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlKind;
import org.apache.wayang.api.sql.calcite.converter.aggregatehelpers.KeyExtractor;
import org.apache.wayang.api.sql.calcite.rel.WayangAggregate;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableBinaryOperator;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.types.DataUnitType;

public class WayangAggregateVisitor extends WayangRelNodeVisitor<WayangAggregate> {

    WayangAggregateVisitor(final WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(final WayangAggregate wayangRelNode) {
        final Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(0));

        final List<AggregateCall> aggCalls = wayangRelNode.getAggCallList();
        final List<Integer> groupFields = new ArrayList<>(wayangRelNode.getGroupSet().asList());
        final int groupCount = wayangRelNode.getGroupCount();

        final List<SqlKind> aggKinds = new ArrayList<>();
        final List<Integer> aggArgs = new ArrayList<>();

        for (final AggregateCall call : aggCalls) {
            final SqlKind k = call.getAggregation().getKind();

            final int argIndex = call.getArgList().isEmpty() ? -1 : call.getArgList().get(0);

            aggKinds.add(k);
            aggArgs.add(argIndex);
        }

        final List<String> reductionFunctions = wayangRelNode.getNamedAggCalls().stream()
                .map(agg -> agg.left.getAggregation().getName()).collect(Collectors.toList());

        final List<String> fields = wayangRelNode.getInput().getRowType().getFieldList().stream()
                .map(RelDataTypeField::getName).collect(Collectors.toList());

        final List<String> aliases = wayangRelNode.getRowType().getFieldList().stream()
                .map(RelDataTypeField::getName).collect(Collectors.toList());

        final String[] reductionStatements = new String[reductionFunctions.size()];

        for (int i = 0; i < reductionStatements.length; i++) {
            reductionStatements[i] = reductionFunctions.get(i) + "(" + fields.get(i) + ") AS " + aliases.get(i);
        }

        final ReduceDescriptor<Record> reduceDescriptor = new ReduceDescriptor<>(
                new AggregateBinaryOperator(aggKinds),
                DataUnitType.createGrouped(Record.class),
                DataUnitType.createBasicUnchecked(Record.class));

        reduceDescriptor.withSqlImplementation(Arrays.stream(reductionStatements).collect(Collectors.joining(",")));

        final Operator aggregateOperator;

        if (groupCount > 0) {
            aggregateOperator = new ReduceByOperator<>(
                    new TransformationDescriptor<>(new KeyExtractor(new HashSet<>(groupFields)),
                            Record.class,
            final ReduceByOperator<Record, Object> reduceByOperator = new ReduceByOperator<>(
                    new TransformationDescriptor<>(new KeyExtractor(groupingFields), Record.class,
                            Object.class),
                    reduceDescriptor);
        } else {
            aggregateOperator = new GlobalReduceOperator<>(reduceDescriptor);
        }

        childOp.connectTo(0, aggregateOperator, 0);

        return aggregateOperator;
    }
}

class AggregateBinaryOperator implements SerializableBinaryOperator<Record> {
    private final List<SqlKind> aggKinds;
    private final Object[] state;

    public AggregateBinaryOperator(final List<SqlKind> aggKinds) {
        this.aggKinds = aggKinds;
        this.state = new Object[aggKinds.size()];

        for (int i = 0; i < aggKinds.size(); i++) {
            final SqlKind kind = aggKinds.get(i);

            switch (kind) {
                case MIN:
                case MAX:
                    break; 
                case COUNT:
                    //state[i] = 1;
                    break; 
                default:
                    throw new UnsupportedOperationException("Not implemented: " + kind);
            }
        }
    }

    @Override
    public Record apply(Record arg0, Record arg1) {
        for (int i = 0; i < aggKinds.size(); i++) {
            final SqlKind kind = aggKinds.get(i);

            switch (kind) {
                case MIN:
                    state[i] = SqlFunctions.leAny(arg0.getField(0), arg1.getField(0)) ? arg0.getField(0) : arg1.getField(0);
                    break;
                case MAX:
                    state[i] = SqlFunctions.geAny(arg0.getField(0), arg1.getField(0)) ? arg0.getField(0) : arg1.getField(0);
                    break; 
                case COUNT:

                    state[i] = state[i] instanceof Integer ? ((int) state[i]) : 1;
                    state[i] = ((int) state[i]) + 1;
                    break; 
                default:
                    throw new UnsupportedOperationException("Not implemented: " + kind);
            }
        }

        return new Record(state);
    }
} 