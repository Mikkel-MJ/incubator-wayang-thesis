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

import java.io.Serializable;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.MapFunctionImpl;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.api.sql.calcite.utils.AliasFinder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.CartesianOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.plan.wayangplan.Operator;

public class WayangCrossJoinVisitor extends WayangRelNodeVisitor<WayangJoin> implements Serializable {

    WayangCrossJoinVisitor(final WayangRelConverter wayangRelConverter, final AliasFinder aliasFinder) {
        super(wayangRelConverter, aliasFinder);
    }

    @Override
    Operator visit(final WayangJoin wayangRelNode) {
        final Operator childOpLeft = wayangRelConverter.convert(wayangRelNode.getInput(0), super.aliasFinder);
        final Operator childOpRight = wayangRelConverter.convert(wayangRelNode.getInput(1), super.aliasFinder);
        final CartesianOperator<Record, Record> join = new CartesianOperator<Record,Record>(Record.class, Record.class);

        childOpLeft.connectTo(0, join, 0);
        childOpRight.connectTo(0, join, 1);

        final SerializableFunction<Tuple2, Record> mp = new MapFunctionImpl();

        final ProjectionDescriptor<Tuple2, Record> pd = new ProjectionDescriptor<Tuple2, Record>(
                mp, Tuple2.class, Record.class);

        final MapOperator<Tuple2, Record> mapOperator = new MapOperator<Tuple2, Record>(pd);

        join.connectTo(0, mapOperator, 0);

        return mapOperator;
    }

}
