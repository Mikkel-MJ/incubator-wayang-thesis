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

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import org.apache.wayang.api.sql.calcite.converter.joinhelpers.JoinFlattenResult;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.KeyExtractor;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.KeyIndex;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.JoinKeyDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.util.ReflectionUtils;

public class WayangJoinVisitor extends WayangRelNodeVisitor<WayangJoin> implements Serializable {

    // Helpers
    public enum Child {
        LEFT, RIGHT
    }

    /**
     * This method determines how key extraction works due to cases where the right
     * table in a join might have a larger table index
     * than the left.
     *
     * @param leftKeyIndex  key index of left table
     * @param rightKeyIndex key index of right table
     * @return a {@link JoinOperator} with {@link KeyExtractors} set
     * @throws UnsupportedOperationException in cases where both table indexes are
     *                                       the same,
     *                                       in practice I am not sure if this
     *                                       should be supported
     */
    protected static scala.Tuple2<Integer, Integer> determineKeyExtractionDirection(final Integer leftKeyIndex,
            final Integer rightKeyIndex, final WayangJoin wayangRelNode) {
        switch (leftKeyIndex.compareTo(rightKeyIndex)) {
            case 1: // left greater than
            {
                final int newLeftKeyIndex = leftKeyIndex
                        - wayangRelNode.getInput(0).getRowType().getFieldCount();
                return new scala.Tuple2<>(rightKeyIndex, newLeftKeyIndex);
            }
            case -1: // left lesser than
            {
                final int newRightKeyIndex = rightKeyIndex
                        - wayangRelNode.getInput(0).getRowType().getFieldCount();
                return new scala.Tuple2<>(leftKeyIndex, newRightKeyIndex);
            }
            default: // both equal
                throw new UnsupportedOperationException();
        }
    }

    /**
     * This method handles the {@link JoinOperator} creation, used in conjunction
     * with:
     * {@link #determineKeyExtractionDirection(Integer, Integer, WayangJoin)}
     *
     * @param wayangRelNode
     * @param leftKeyIndex
     * @param rightKeyIndex
     * @return a {@link JoinOperator} with {@link KeyExtractors} set
     */
    protected static JoinOperator<Record, Record, Record> getJoinOperator(final Integer leftKeyIndex,
            final Integer rightKeyIndex,
            final WayangJoin wayangRelNode, final String leftFieldName, final String rightFieldName,
            final String[] leftColumns, final String[] leftColumnAliases, final String[] rightColumns,
            final String[] rightColumnAliases) {
        if (wayangRelNode.getInputs().size() != 2)
            throw new UnsupportedOperationException("Join had an unexpected amount of inputs, found: "
                    + wayangRelNode.getInputs().size() + ", expected: 2");

        final JoinKeyDescriptor<Record, Record> jkd0 = new JoinKeyDescriptor<>(new KeyExtractor<>(leftKeyIndex)
                .withRowType(
                        wayangRelNode.getLeft().getRowType().toString(),
                        wayangRelNode.toString(),
                        wayangRelNode.getLeft().toString(),
                        wayangRelNode.getRight().toString()),
                Record.class,
                Record.class,
                leftColumns, leftColumnAliases, leftKeyIndex);

        final JoinKeyDescriptor<Record, Record> jkd1 = new JoinKeyDescriptor<>(new KeyExtractor<>(leftKeyIndex)
                .withRowType(
                        wayangRelNode.getLeft().getRowType().toString(),
                        wayangRelNode.toString(),
                        wayangRelNode.getLeft().toString(),
                        wayangRelNode.getRight().toString()),
                Record.class,
                Record.class,
                rightColumns, rightColumnAliases, rightKeyIndex);

        final TransformationDescriptor<Record, Record> leftProjectionDescriptor = jkd0
                .withSqlImplementation(leftFieldName);
        final TransformationDescriptor<Record, Record> righProjectionDescriptor = jkd1
                .withSqlImplementation(rightFieldName);

        return new JoinOperator<>(
                leftProjectionDescriptor,
                righProjectionDescriptor);
    }

    WayangJoinVisitor(final Configuration configuration) {
        super(configuration);
    }

    @Override
    Operator visit(final WayangJoin wayangRelNode) {
        final Operator childOpLeft = WayangRelConverter.convert(wayangRelNode.getInput(0), configuration);
        final Operator childOpRight = WayangRelConverter.convert(wayangRelNode.getInput(1), configuration);

        final RexNode condition = wayangRelNode.getCondition();

        if (!condition.isA(SqlKind.EQUALS)) {
            throw new UnsupportedOperationException(
                    "Only equality joins supported but got: " + condition.getKind()
                            + " from relNode: " + wayangRelNode + ", with inputs: "
                            + wayangRelNode.getInputs() + ", joinType: "
                            + wayangRelNode.getJoinType());
        }

        final int leftKeyIndex = condition.accept(new KeyIndex(false, Child.LEFT));
        final int rightKeyIndex = condition.accept(new KeyIndex(false, Child.RIGHT));

        // init join
        final scala.Tuple2<Integer, Integer> keyExtractor = WayangJoinVisitor
                .determineKeyExtractionDirection(leftKeyIndex, rightKeyIndex, wayangRelNode);

        // we get the names and aliases for the columns included in the join filter.
        final RelDataTypeField leftField = wayangRelNode.getLeft().getRowType().getFieldList()
                .get(keyExtractor._1());
        final RelDataTypeField rightField = wayangRelNode.getRight().getRowType().getFieldList()
                .get(keyExtractor._2());

        final String leftFieldName = leftField.getName();
        final String rightFieldName = rightField.getName();

        final String[] leftProjection = wayangRelNode.getLeft().getRowType().getFieldList().stream()
                .map(RelDataTypeField::getName).toArray(String[]::new);

        final String[] leftProjectionAliases = wayangRelNode.getRowType().getFieldList().stream()
                .map(RelDataTypeField::getName).limit(leftProjection.length).toArray(String[]::new);

        final String[] rightProjection = wayangRelNode.getRight().getRowType().getFieldList().stream()
                .map(RelDataTypeField::getName).toArray(String[]::new);

        final String[] rightProjectionAliases = wayangRelNode.getRowType().getFieldList().stream()
                .map(RelDataTypeField::getName).skip(leftProjection.length).toArray(String[]::new);

        final JoinOperator<Record, Record, Record> join = WayangJoinVisitor.getJoinOperator(
                keyExtractor._1(),
                keyExtractor._2(),
                wayangRelNode,
                leftFieldName,
                rightFieldName,
                leftProjection,
                leftProjectionAliases,
                rightProjection,
                rightProjectionAliases);

        System.out.println("[WayangJoinVisitor.kd0]: " + join.getKeyDescriptor0().getSqlImplementation());
        System.out.println("[WayangJoinVisitor.kd1]: " + join.getKeyDescriptor1().getSqlImplementation());

        childOpLeft.connectTo(0, join, 0);
        childOpRight.connectTo(0, join, 1);

        final ProjectionDescriptor<Tuple2<Record, Record>, Record> projectionDescriptor = new ProjectionDescriptor<Tuple2<Record, Record>, Record>(
                new JoinFlattenResult(),
                ReflectionUtils.specify(Tuple2.class),
                Record.class);

        System.out.println("[WayangJoinVisitor.rowType]: " + wayangRelNode.getRowType());
        System.out.println("[WayangJoinVisitor.rowTypeL]: " + wayangRelNode.getLeft().getRowType());
        System.out.println("[WayangJoinVisitor.rowTypeR]: " + wayangRelNode.getRight().getRowType());

        // explicitly set sql impl to null since flattening operators happen
        // automatically in joins
        projectionDescriptor.withSqlImplementation(null);

        System.out.println("[WayangJoinVisitor.projectionDescriptor]: " + projectionDescriptor.getSqlImplementation());

        // Join returns Tuple2 - map to a Record
        final MapOperator<Tuple2<Record, Record>, Record> mapOperator = new MapOperator<Tuple2<Record, Record>, Record>(
                projectionDescriptor);

        join.connectTo(0, mapOperator, 0);

        return mapOperator;
    }
}
