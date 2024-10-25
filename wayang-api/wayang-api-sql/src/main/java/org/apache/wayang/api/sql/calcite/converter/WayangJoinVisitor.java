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

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.KeyExtractor;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.KeyIndex;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.MapFunctionImpl;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.api.sql.calcite.utils.SqlField;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.plan.wayangplan.Operator;

import scala.Function3;
import scala.Tuple3;

public class WayangJoinVisitor extends WayangRelNodeVisitor<WayangJoin> implements Serializable {

    WayangJoinVisitor(final WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(final WayangJoin wayangRelNode) {
        final Operator childOpLeft = wayangRelConverter.convert(wayangRelNode.getInput(0));
        final Operator childOpRight = wayangRelConverter.convert(wayangRelNode.getInput(1));

        final RexNode condition = ((Join) wayangRelNode).getCondition();

        if (!condition.isA(SqlKind.EQUALS)) {
            throw new UnsupportedOperationException("Only equality joins supported but got: " + condition.getKind()
                    + " from relNode: " + wayangRelNode + ", with inputs: " + wayangRelNode.getInputs());
        }

        final int leftKeyIndex = condition.accept(new KeyIndex(false, Child.LEFT));
        final int rightKeyIndex = condition.accept(new KeyIndex(false, Child.RIGHT));

        // init join
        JoinOperator<Record, Record, SqlField> join = this.determineKeyExtractionDirection().tupled()
                .andThen(this.getJoinOperator().tupled())
                .apply(new Tuple3<>(leftKeyIndex, rightKeyIndex, wayangRelNode));

        // call connectTo on both operators (left and right)
        childOpLeft.connectTo(0, join, 0);
        childOpRight.connectTo(0, join, 1);

        // Join returns Tuple2 - map to a Record
        String[] joinTableNames = ((Join) wayangRelNode).getRowType().getFieldNames().toArray(String[]::new);
        Class<Tuple2<Record, Record>> clazz = (Class<Tuple2<Record, Record>>) new Tuple2<Record, Record>(null, null)
                .getClass(); // the things we do for type coercion in java...

        SerializableFunction<Tuple2<Record, Record>, Record> mp = new MapFunctionImpl();
        ProjectionDescriptor<Tuple2<Record, Record>, Record> pd = new ProjectionDescriptor<Tuple2<Record, Record>, Record>(
                mp, clazz, Record.class, joinTableNames);

        
        final MapOperator<Tuple2<Record, Record>, Record> mapOperator = new MapOperator<Tuple2<Record, Record>, Record>(pd);

        join.connectTo(0, mapOperator, 0);

        return mapOperator;
    }

    /**
     * This method determines how key extraction works due to cases where the right
     * table in a join might have a larger table index
     * than the left, calls {@link #getJoinOperator} to create the underlying join.
     * 
     * @param leftKeyIndex  key index of left table
     * @param rightKeyIndex key index of right table
     * @return a {@link JoinOperator} with {@link KeyExtractors} set
     * @throws UnsupportedOperationException in cases where both table indexes are
     *                                       the same,
     *                                       in practice I am not sure if this
     *                                       should be supported
     */
    protected Function3<Integer, Integer, WayangJoin, Tuple3<Integer, Integer, WayangJoin>> determineKeyExtractionDirection() {
        return (leftKeyIndex, rightKeyIndex, wayangRelNode) -> {
            switch (leftKeyIndex.compareTo(rightKeyIndex)) {
                case 1: // left greater than
                {
                    final int newLeftKeyIndex = leftKeyIndex - wayangRelNode.getInput(0).getRowType().getFieldCount();
                    return new Tuple3<>(rightKeyIndex, newLeftKeyIndex, wayangRelNode);
                }
                case -1: // left lesser than
                {
                    final int newRightKeyIndex = rightKeyIndex - wayangRelNode.getInput(0).getRowType().getFieldCount();
                    return new Tuple3<>(leftKeyIndex, newRightKeyIndex, wayangRelNode);
                }
                default: // both equal
                    throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * This method handles the {@link JoinOperator} creation, also creating the
     * {@link KeyExtractor} and subsequent {@link ProjectionDescriptor}
     * used in the join.
     * 
     * @param wayangRelNode
     * @param leftKeyIndex
     * @param rightKeyIndex
     * @return a {@link JoinOperator} with {@link KeyExtractors} set
     */
    protected Function3<Integer, Integer, WayangJoin, JoinOperator<Record, Record, SqlField>> getJoinOperator() {
        return (leftKeyIndex, rightKeyIndex, wayangRelNode) -> {
            if (wayangRelNode.getInputs().size() != 2)
                throw new UnsupportedOperationException("Join had an unexpected amount of inputs, found: "
                        + wayangRelNode.getInputs().size() + ", expected: 2");

            final String[] leftTableNames = wayangRelNode.getInput(0)
                    .getRowType()
                    .getFieldNames()
                    .toArray(String[]::new);

            final String[] rightTableNames = wayangRelNode.getInput(1)
                    .getRowType()
                    .getFieldNames()
                    .toArray(String[]::new);

            final ProjectionDescriptor<Record, SqlField> leftProjectionDescriptor = new ProjectionDescriptor<>(
                    new KeyExtractor<>(leftKeyIndex),
                    Record.class, SqlField.class, leftTableNames);

            final ProjectionDescriptor<Record, SqlField> righProjectionDescriptor = new ProjectionDescriptor<>(
                    new KeyExtractor<>(rightKeyIndex),
                    Record.class, SqlField.class, rightTableNames);

            final JoinOperator<Record, Record, SqlField> join = new JoinOperator<>(
                    leftProjectionDescriptor,
                    righProjectionDescriptor);

            return join;
        };
    }

    // Helpers
    public static enum Child {
        LEFT, RIGHT
    }
}
