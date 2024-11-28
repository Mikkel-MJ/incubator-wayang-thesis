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

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.wayang.api.sql.calcite.converter.aggregatehelpers.AddAggCols;
import org.apache.wayang.api.sql.calcite.converter.aggregatehelpers.AggregateFunction;
import org.apache.wayang.api.sql.calcite.converter.aggregatehelpers.GetResult;
import org.apache.wayang.api.sql.calcite.converter.aggregatehelpers.KeyExtractor;
import org.apache.wayang.api.sql.calcite.rel.WayangAggregate;
import org.apache.wayang.api.sql.calcite.utils.CalciteSources;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.types.DataUnitType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WayangAggregateVisitor extends WayangRelNodeVisitor<WayangAggregate> {

    WayangAggregateVisitor(final WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(final WayangAggregate wayangRelNode) {
        final Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(0));
        Operator aggregateOperator;

        final List<AggregateCall> aggregateCalls = ((Aggregate) wayangRelNode).getAggCallList();
        final int groupCount = wayangRelNode.getGroupCount();
        final HashSet<Integer> groupingFields = new HashSet<>(wayangRelNode.getGroupSet().asSet());

        final Map<String, RelDataTypeField> unAliasedNamesMap = wayangRelNode.getNamedAggCalls()
                .stream()
                .collect(Collectors.toMap(
                        agg -> agg.right, // key: project column index
                        agg -> wayangRelNode
                                .getInput()
                                .getRowType()
                                .getFieldList()
                                .get(agg.left.getArgList().get(0))// value: unaliased name
                ));

        final Map<RelDataTypeField, String> columnToTableOrigin = CalciteSources
                .createColumnToTableOriginMap(wayangRelNode.getInput());

        final List<String> specifiedColumnNames = unAliasedNamesMap.entrySet().stream()
                .map(entry -> columnToTableOrigin.get(entry.getValue()) + "." + entry.getValue().getName())
                .collect(Collectors.toList());

        final ProjectionDescriptor<Record, Record> pd = new ProjectionDescriptor<>(new AddAggCols(aggregateCalls),
                Record.class, Record.class, specifiedColumnNames.toArray(String[]::new));

        final MapOperator<Record, Record> mapOperator = new MapOperator<>(pd);

        childOp.connectTo(0, mapOperator, 0);

        if (groupCount > 0) {
            ReduceByOperator<Record, Object> reduceByOperator = new ReduceByOperator<>(
                    new TransformationDescriptor<>(new KeyExtractor(groupingFields), Record.class, Object.class),
                    new ReduceDescriptor<>(new AggregateFunction(aggregateCalls),
                            DataUnitType.createGrouped(Record.class),
                            DataUnitType.createBasicUnchecked(Record.class)));

            aggregateOperator = reduceByOperator;
        } else {
            final ReduceDescriptor<Record> reduceDescriptor = new ReduceDescriptor<>(new AggregateFunction(aggregateCalls),
                    DataUnitType.createGrouped(Record.class),
                    DataUnitType.createBasicUnchecked(Record.class));

            final List<String> reductionFunctions = wayangRelNode.getNamedAggCalls().stream()
                    .map(agg -> agg.left.getAggregation().getName()).collect(Collectors.toList());

            final List<String> reductionStatements = new ArrayList<>();

            assert reductionFunctions.size() == specifiedColumnNames.size()
                    : "Expected that the amount of reduction functions in reduce statement was eqaul to the amount of used tables";

            // we have an assumption that the ordering is maintained between each list
            for (int i = 0; i < reductionFunctions.size(); i++) {
                reductionStatements.add(reductionFunctions.get(i) + "(" + specifiedColumnNames.get(i) + ")");
            }

            reduceDescriptor.withSqlImplementation(reductionStatements.stream().collect(Collectors.joining(",")));

            aggregateOperator = new GlobalReduceOperator<Record>(reduceDescriptor);
        }

        mapOperator.connectTo(0, aggregateOperator, 0);

        final ProjectionDescriptor<Record, Record> pdAgg = new ProjectionDescriptor<>(new GetResult(aggregateCalls, groupingFields),
                Record.class, Record.class, specifiedColumnNames.toArray(String[]::new));
        final MapOperator<Record, Record> mapOperator2 = new MapOperator<>(pdAgg);

        aggregateOperator.connectTo(0, mapOperator2, 0);
        return mapOperator2;

    }
}