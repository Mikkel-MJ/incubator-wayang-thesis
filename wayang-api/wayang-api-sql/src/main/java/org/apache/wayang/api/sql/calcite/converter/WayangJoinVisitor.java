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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import org.apache.wayang.api.sql.calcite.converter.joinhelpers.JoiningTableExtractor;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.KeyExtractor;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.KeyIndex;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.MapFunctionImpl;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.api.sql.calcite.utils.AliasFinder;
import org.apache.wayang.api.sql.calcite.utils.CalciteSources;
import org.apache.wayang.api.sql.calcite.utils.SqlField;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.plan.wayangplan.Operator;

public class WayangJoinVisitor extends WayangRelNodeVisitor<WayangJoin> implements Serializable {

    WayangJoinVisitor(final WayangRelConverter wayangRelConverter, final AliasFinder aliasFinder) {
        super(wayangRelConverter, aliasFinder);
    }

    @Override
    Operator visit(final WayangJoin wayangRelNode) {
        final Operator childOpLeft = wayangRelConverter.convert(wayangRelNode.getInput(0), super.aliasFinder);
        final Operator childOpRight = wayangRelConverter.convert(wayangRelNode.getInput(1), super.aliasFinder);

        final RexNode condition = ((Join) wayangRelNode).getCondition();

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
        final scala.Tuple2<Integer, Integer> keyExtractor = this.determineKeyExtractionDirection(leftKeyIndex,
                rightKeyIndex, wayangRelNode);

        final RelDataTypeField leftField = wayangRelNode.getLeft().getRowType().getFieldList()
                .get(keyExtractor._1());
        final RelDataTypeField rightField = wayangRelNode.getRight().getRowType().getFieldList()
                .get(keyExtractor._2());

        // get left col name
        final String leftFieldName = leftField.getName();

        // right col name
        final String rightFieldName = rightField.getName();

        // fetch table names from joining columns
        final Map<RelDataTypeField, String> columnToOriginMapLeft = CalciteSources
                .createColumnToTableOriginMap(wayangRelNode.getInput(0));
        final Map<RelDataTypeField, String> columnToOriginMapRight = CalciteSources
                .createColumnToTableOriginMap(wayangRelNode.getInput(1));

        final String leftTableName = columnToOriginMapLeft.get(leftField);
        final String rightTableName = columnToOriginMapRight.get(rightField);

        final JoiningTableExtractor leftVisitor = new JoiningTableExtractor(true);
        leftVisitor.visit(wayangRelNode.getLeft(), wayangRelNode.getId(), null);

        final JoiningTableExtractor rightVisitor = new JoiningTableExtractor(true);
        rightVisitor.visit(wayangRelNode.getRight(), wayangRelNode.getId(), null);

        // TODO: prolly breaks on bushy joins
        final String joiningTableName = leftVisitor.getName() instanceof String
                ? leftVisitor.getName()
                : rightVisitor.getName();

        // remove calcite unique integer identifiers in table.column0 type of names
        final List<String> catalog = CalciteSources.getSqlColumnNames(wayangRelNode);

        final String cleanedLeftFieldName = CalciteSources
                .findSqlName(leftTableName + "." + leftFieldName, catalog).split("\\.")[1];
        final String cleanedRightFieldName = CalciteSources
                .findSqlName(rightTableName + "." + rightFieldName, catalog).split("\\.")[1];

        // find the sql table alias for the left and right table, so it can be used in
        // the transform descriptor, this depends when each statement shows up in the
        // condition if the leftKeyIndex is less than the right key index this is the
        // normal case,
        // where JOIN x AS x' ON x'.x* = y'.y*, else we need to switch
        final String leftTableAlias = leftKeyIndex < rightKeyIndex
                ? aliasFinder.columnIndexToTableName.get(leftKeyIndex)
                : aliasFinder.columnIndexToTableName.get(rightKeyIndex);
        final String rightTableAlias = leftKeyIndex < rightKeyIndex
                ? aliasFinder.columnIndexToTableName.get(rightKeyIndex)
                : aliasFinder.columnIndexToTableName.get(leftKeyIndex);

        // if join is joining the LHS of a join condition "JOIN left ON left = right"
        // then we pick the first case, otherwise the 2nd "JOIN right ON left = right"
        final JoinOperator<Record, Record, SqlField> join = joiningTableName == leftTableName
                ? this.getJoinOperator(keyExtractor._1(), keyExtractor._2(),
                        wayangRelNode,
                        leftTableName + " AS " + leftTableAlias,
                        cleanedLeftFieldName,
                        rightTableAlias,
                        cleanedRightFieldName)
                : this.getJoinOperator(keyExtractor._1(), keyExtractor._2(),
                        wayangRelNode,
                        rightTableName + " AS " + rightTableAlias,
                        cleanedRightFieldName,
                        leftTableAlias,
                        cleanedLeftFieldName);

        childOpLeft.connectTo(0, join, 0);
        childOpRight.connectTo(0, join, 1);

        // jdbc usage move l8r
        final String[] joinTableNames = { leftTableName + "." + leftFieldName,
                rightTableName + "." + rightFieldName };

        // Join returns Tuple2 - map to a Record
        final SerializableFunction<Tuple2, Record> mp = new MapFunctionImpl();

        final ProjectionDescriptor<Tuple2, Record> pd = new ProjectionDescriptor<Tuple2, Record>(
                mp, Tuple2.class, Record.class, joinTableNames);

        final MapOperator<Tuple2, Record> mapOperator = new MapOperator<Tuple2, Record>(pd);

        join.connectTo(0, mapOperator, 0);

        return mapOperator;
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
    protected scala.Tuple2<Integer, Integer> determineKeyExtractionDirection(final Integer leftKeyIndex,
            final Integer rightKeyIndex, final WayangJoin wayangRelNode) {

        switch (leftKeyIndex.compareTo(rightKeyIndex)) {
            case 1: // left greater than
            {
                final int newLeftKeyIndex = leftKeyIndex
                        - wayangRelNode.getInput(0).getRowType().getFieldCount();
                // (leftTableGetter, rightTableGetter)
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
    protected JoinOperator<Record, Record, SqlField> getJoinOperator(final Integer leftKeyIndex,
            final Integer rightKeyIndex,
            final WayangJoin wayangRelNode, final String leftTableName, final String leftFieldName,
            final String rightTableName, final String rightFieldName) {
        if (wayangRelNode.getInputs().size() != 2)
            throw new UnsupportedOperationException("Join had an unexpected amount of inputs, found: "
                    + wayangRelNode.getInputs().size() + ", expected: 2");

        final TransformationDescriptor<Record, SqlField> leftProjectionDescriptor = new ProjectionDescriptor<>(
                new KeyExtractor<>(leftKeyIndex),
                Record.class, SqlField.class, leftFieldName)
                .withSqlImplementation(Optional.ofNullable(leftTableName).orElse(""), leftFieldName);

        final TransformationDescriptor<Record, SqlField> rightProjectionDescriptor = new ProjectionDescriptor<>(
                new KeyExtractor<>(rightKeyIndex),
                Record.class, SqlField.class, rightFieldName)
                .withSqlImplementation(Optional.ofNullable(rightTableName).orElse(""), rightFieldName);

        final JoinOperator<Record, Record, SqlField> join = new JoinOperator<>(
                leftProjectionDescriptor,
                rightProjectionDescriptor);

        return join;
    }

    // Helpers
    public static enum Child {
        LEFT, RIGHT
    }
}
