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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.wayang.api.sql.calcite.converter.aggregatehelpers.AddAggCols;
import org.apache.wayang.api.sql.calcite.converter.aggregatehelpers.AggregateFunction;
import org.apache.wayang.api.sql.calcite.converter.aggregatehelpers.GetResult;
import org.apache.wayang.api.sql.calcite.converter.aggregatehelpers.KeyExtractor;
import org.apache.wayang.api.sql.calcite.rel.WayangAggregate;
import org.apache.wayang.api.sql.calcite.rel.WayangRel;
import org.apache.wayang.api.sql.calcite.utils.AliasFinder;
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

public class WayangAggregateVisitor extends WayangRelNodeVisitor<WayangAggregate> {

    WayangAggregateVisitor(final AliasFinder aliasFinder) {
        super(aliasFinder);
    }

    @Override
    Operator visit(final WayangAggregate wayangRelNode) {
        final Operator childOp = WayangRelConverter.convert(wayangRelNode.getInput(), super.aliasFinder);

        // fetch the indexes of colmuns affected, in calcite aggregates and projections
        // have their own catalog, we need to find the column indexes in the global
        // catalog

        final String[] aliasedFields;

        System.out.println("[WayangAggregateVisitor.rowType]: " + wayangRelNode.getRowType());

        if (wayangRelNode.getAggCallList().get(0).getAggregation().getSqlIdentifier() == null) {
            aliasedFields = CalciteSources.getAliasedFields((WayangRel) wayangRelNode.getInput());
        } else {
            aliasedFields = CalciteSources.getAliasedFields(wayangRelNode);
        }

        final List<AggregateCall> aggregateCalls = wayangRelNode.getAggCallList();
        final int groupCount = wayangRelNode.getGroupCount();
        final HashSet<Integer> groupingFields = new HashSet<>(wayangRelNode.getGroupSet().asSet());

        final ProjectionDescriptor<Record, Record> pd = new ProjectionDescriptor<>(
                new AddAggCols(aggregateCalls),
                Record.class, Record.class, aliasedFields);

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

            final List<String> reductionStatements = new ArrayList<>();

            assert reductionFunctions.size() == aliasedFields.length
                    : "Expected that the amount of reduction functions in reduce statement was equal to the amount of used tables";

            // we have an assumption that the ordering is maintained between each list
            for (int i = 0; i < reductionFunctions.size(); i++) {
                // unpacking alias
                final String[] unpackedAlias = aliasedFields[i].split(" AS ");

                if (aliasedFields.length == 2) {
                    reductionStatements.add(
                            reductionFunctions.get(i) + "(" + unpackedAlias[0] + ")" + " AS " + unpackedAlias[1]);
                } else {
                    reductionStatements.add(
                            reductionFunctions.get(i) + "(" + unpackedAlias[0] + ")");
                }
            }

            reduceDescriptor.withSqlImplementation(
                    reductionStatements.stream().collect(Collectors.joining(",")));

            aggregateOperator = new GlobalReduceOperator<Record>(reduceDescriptor);
        }

        mapOperator.connectTo(0, aggregateOperator, 0);

        final List<Integer> orderedGroupingFields = groupingFields
                .stream()
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());

        final ProjectionDescriptor<Record, Record> pdAgg = new ProjectionDescriptor<>(
                new GetResult(aggregateCalls, orderedGroupingFields),
                Record.class, Record.class, aliasedFields);

        final MapOperator<Record, Record> mapOperator2 = new MapOperator<>(pdAgg);

        aggregateOperator.connectTo(0, mapOperator2, 0);

        return mapOperator2;
    }
}