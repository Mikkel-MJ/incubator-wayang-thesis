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
import org.apache.wayang.api.sql.calcite.converter.AggregateHelpers.AddAggCols;
import org.apache.wayang.api.sql.calcite.converter.AggregateHelpers.AggregateFunction;
import org.apache.wayang.api.sql.calcite.converter.AggregateHelpers.GetResult;
import org.apache.wayang.api.sql.calcite.converter.AggregateHelpers.KeyExtractor;
import org.apache.wayang.api.sql.calcite.rel.WayangAggregate;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.types.DataUnitType;

import java.util.HashSet;
import java.util.List;

public class WayangAggregateVisitor extends WayangRelNodeVisitor<WayangAggregate> {

    WayangAggregateVisitor(WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(WayangAggregate wayangRelNode) {
        Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(0));
        Operator aggregateOperator;

        List<AggregateCall> aggregateCalls = ((Aggregate) wayangRelNode).getAggCallList();
        int groupCount = wayangRelNode.getGroupCount();
        HashSet<Integer> groupingFields = new HashSet<>(wayangRelNode.getGroupSet().asSet());

        MapOperator mapOperator = new MapOperator(
                new AddAggCols(aggregateCalls),
                Record.class,
                Record.class);
        childOp.connectTo(0, mapOperator, 0);

        if (groupCount > 0) {
            ReduceByOperator<Record, Object> reduceByOperator;
            System.out.println("Creating transform Descriptor (aggregate)");
            reduceByOperator = new ReduceByOperator<>(
                    new TransformationDescriptor<>(new KeyExtractor(groupingFields), Record.class, Object.class),
                    new ReduceDescriptor<>(new AggregateFunction(aggregateCalls),
                            DataUnitType.createGrouped(Record.class),
                            DataUnitType.createBasicUnchecked(Record.class)));
            aggregateOperator = reduceByOperator;
        } else {
            GlobalReduceOperator<Record> globalReduceOperator;
            globalReduceOperator = new GlobalReduceOperator<>(
                    new ReduceDescriptor<>(new AggregateFunction(aggregateCalls),
                            DataUnitType.createGrouped(Record.class),
                            DataUnitType.createBasicUnchecked(Record.class)));
            aggregateOperator = globalReduceOperator;
        }

        mapOperator.connectTo(0, aggregateOperator, 0);

        MapOperator mapOperator2 = new MapOperator(
                new GetResult(aggregateCalls, groupingFields),
                Record.class,
                Record.class);
        aggregateOperator.connectTo(0, mapOperator2, 0);
        return mapOperator2;

    }
}