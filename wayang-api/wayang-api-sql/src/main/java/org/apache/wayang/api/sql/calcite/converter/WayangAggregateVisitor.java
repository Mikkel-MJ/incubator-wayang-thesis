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

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.wayang.api.sql.calcite.converter.aggregatehelpers.AddAggCols;
import org.apache.wayang.api.sql.calcite.converter.aggregatehelpers.AggregateFunction;
import org.apache.wayang.api.sql.calcite.converter.aggregatehelpers.GetResult;
import org.apache.wayang.api.sql.calcite.converter.aggregatehelpers.KeyExtractor;
import org.apache.wayang.api.sql.calcite.rel.WayangAggregate;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.types.DataUnitType;

public class WayangAggregateVisitor extends WayangRelNodeVisitor<WayangAggregate> {

    WayangAggregateVisitor(final Configuration configuration) {
        super(configuration);
    }

    @Override
    Operator visit(final WayangAggregate wayangRelNode) {
        final Operator childOp = WayangRelConverter.convert(wayangRelNode.getInput(), super.configuration);

        final List<AggregateCall> aggregateCalls = wayangRelNode.getAggCallList();
        final int groupCount = wayangRelNode.getGroupCount();
        final HashSet<Integer> groupingFields = new HashSet<>(wayangRelNode.getGroupSet().asSet());

        final ProjectionDescriptor<Record, Record> pd = new ProjectionDescriptor<>(
                new AddAggCols(aggregateCalls),
                Record.class, Record.class);

        // we create a null descriptor since postgres handles this internally
        pd.withSqlImplementation(null);

        final MapOperator<Record, Record> mapOperator = new MapOperator<>(pd);

        childOp.connectTo(0, mapOperator, 0);

        final Operator aggregateOperator;

        if (groupCount > 0) {
            aggregateOperator = new ReduceByOperator<>(
                    new TransformationDescriptor<>(new KeyExtractor(groupingFields), Record.class,
                            Object.class),
                    new ReduceDescriptor<>(new AggregateFunction(aggregateCalls),
                            DataUnitType.createGrouped(Record.class),
                            DataUnitType.createBasicUnchecked(Record.class)));
        } else {
            final ReduceDescriptor<Record> reduceDescriptor = new ReduceDescriptor<>(
                    new AggregateFunction(aggregateCalls),
                    DataUnitType.createGrouped(Record.class),
                    DataUnitType.createBasicUnchecked(Record.class));

            final List<String> reductionFunctions = wayangRelNode.getNamedAggCalls().stream()
                    .map(agg -> agg.left.getAggregation().getName()).collect(Collectors.toList());

            // may need to make the projection on the map operator above.
            final List<String> fields = wayangRelNode.getInput().getRowType().getFieldList().stream()
                    .map(RelDataTypeField::getName).collect(Collectors.toList());

            final List<String> aliases = wayangRelNode.getRowType().getFieldList().stream()
                    .map(RelDataTypeField::getName).collect(Collectors.toList());

            final String[] reductionStatements = new String[reductionFunctions.size()];

            for (int i = 0; i < reductionStatements.length; i++) {
                reductionStatements[i] = reductionFunctions.get(i) + "(." + fields.get(i) + ") AS " + aliases.get(i);
            }

            reduceDescriptor.withSqlImplementation(
                    Arrays.stream(reductionStatements).collect(Collectors.joining(",")));

            aggregateOperator = new GlobalReduceOperator<Record>(reduceDescriptor);
        }

        mapOperator.connectTo(0, aggregateOperator, 0);

        final List<Integer> orderedGroupingFields = groupingFields
                .stream()
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());

        final ProjectionDescriptor<Record, Record> pdAgg = new ProjectionDescriptor<>(
                new GetResult(aggregateCalls, orderedGroupingFields),
                Record.class, Record.class);

        // we create a null descriptor since postgres handles this internally
        pd.withSqlImplementation(null);

        final MapOperator<Record, Record> mapOperator2 = new MapOperator<>(pdAgg);

        aggregateOperator.connectTo(0, mapOperator2, 0);

        return mapOperator2;
    }
}