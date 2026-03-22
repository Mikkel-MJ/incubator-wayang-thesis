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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.wayang.api.sql.calcite.converter.calltrees.Node;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.JoinCallTreeFactory;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.JoinFlattenResult;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.api.sql.calcite.utils.AliasFinder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.JoinKeyDescriptor;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.util.ReflectionUtils;

public class WayangJoinVisitor extends WayangRelNodeVisitor<WayangJoin> {

    WayangJoinVisitor(final WayangRelConverter wayangRelConverter, final AliasFinder aliasFinder) {
        super(wayangRelConverter, aliasFinder);
    }

    @Override
    Operator visit(final WayangJoin wayangRelNode) {

        System.out.println("join fields: "  + wayangRelNode.getRowType().getFieldList());
        final Operator childOpLeft = wayangRelConverter.convert(wayangRelNode.getInput(0), aliasFinder);
        final Operator childOpRight = wayangRelConverter.convert(wayangRelNode.getInput(1), aliasFinder);

        final RexNode condition = wayangRelNode.getCondition();
        final RexCall call = (RexCall) condition;

        final List<Integer> keys = call.getOperands().stream()
                .map(RexInputRef.class::cast)
                .map(RexInputRef::getIndex)
                .collect(Collectors.toList());

        assert (keys.size() == 2) : "Amount of keys found in join was not 2, got: " + keys.size();

        if (!condition.isA(SqlKind.EQUALS)) {
            throw new UnsupportedOperationException("Only equality joins supported");
        }

        final List<String> leftProjectionAliases = wayangRelNode.getLeft().getRowType().getFieldNames();
        final List<String> leftProjection = wayangRelNode.getRowType().getFieldNames().stream()
                .limit(leftProjectionAliases.size()).collect(Collectors.toList());
        final List<String> rightProjectionAliases = wayangRelNode.getRight().getRowType().getFieldNames();
        final List<String> rightProjection = wayangRelNode.getRowType().getFieldNames().stream()
                .skip(leftProjectionAliases.size()).collect(Collectors.toList());

        System.out.println("[JoinVisitor]: left field list get name " + wayangRelNode.getLeft().getRowType().getFieldList().stream().map(field -> field.getName()).collect(Collectors.toList()));
        System.out.println("[JoinVisitor]: left field names " + wayangRelNode.getLeft().getRowType().getFieldNames());
        System.out.println("[JoinVisitor]: right field list get name " + wayangRelNode.getRowType().getFieldList().stream().map(field -> field.getName()).collect(Collectors.toList()));
        System.out.println("[JoinVisitor]: right field names " + wayangRelNode.getRight().getRowType().getFieldNames());
        System.out.println("[JoinVisitor]: field list get name " + wayangRelNode.getRight().getRowType().getFieldList().stream().map(field -> field.getName()).collect(Collectors.toList()));
        System.out.println("[JoinVisitor]: field names " + wayangRelNode.getRowType().getFieldNames());
        System.out.println("[JoinVisitor]: join: " + wayangRelNode);
        System.out.println("[JoinVisitor]: left proj aliases: " + leftProjectionAliases);
        System.out.println("[JoinVisitor]: left proj: " + leftProjection);
        System.out.println("[JoinVisitor]:right proj aliases: " + rightProjectionAliases);
        System.out.println("[JoinVisitor]: right proj: " + rightProjection);

        assert leftProjectionAliases.size() == leftProjection.size();
        assert rightProjectionAliases.size() == rightProjection.size();

        final JoinCallTreeFactory factory = new JoinCallTreeFactory();
        final Node joinCallTree = factory.fromRexNode(wayangRelNode.getCondition());
        final SerializableFunction<Record, Record> javaImpl = rec -> new Record(joinCallTree.evaluate(rec));
        final SerializableFunction<List<String>, String> createSqlFunc = fields -> joinCallTree.createSqlString(fields);

        final JoinKeyDescriptor leftKeyDescriptor = new JoinKeyDescriptor(javaImpl, leftProjection,
                leftProjectionAliases, createSqlFunc);
        final JoinKeyDescriptor righKeyDescriptor = new JoinKeyDescriptor(javaImpl, rightProjection,
                rightProjectionAliases, createSqlFunc);

        final JoinOperator<Record, Record, Record> join = new JoinOperator<>(
                leftKeyDescriptor,
                righKeyDescriptor);

        // call connectTo on both operators (left and right)
        childOpLeft.connectTo(0, join, 0);
        childOpRight.connectTo(0, join, 1);

        // Join returns Tuple2 - map to a Record
        final ProjectionDescriptor<Tuple2<Record, Record>, Record> pd = new ProjectionDescriptor<>(
                new JoinFlattenResult(),
                ReflectionUtils.specify(Tuple2.class),
                Record.class,
                wayangRelNode.getRowType().getFieldNames().toArray(String[]::new));

        final MapOperator<Tuple2<Record, Record>, Record> mapOperator = new MapOperator<>(pd);

        join.connectTo(0, mapOperator, 0);

        return mapOperator;
    }
}