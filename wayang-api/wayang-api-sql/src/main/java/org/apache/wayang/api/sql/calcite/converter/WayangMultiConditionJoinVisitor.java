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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import org.apache.wayang.api.sql.calcite.converter.joinhelpers.JoiningTableExtractor;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.MultiKeyExtractor;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.MultiMapFunctionImpl;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.api.sql.calcite.utils.AliasFinder;
import org.apache.wayang.api.sql.calcite.utils.CalciteSources;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.plan.wayangplan.Operator;

public class WayangMultiConditionJoinVisitor extends WayangRelNodeVisitor<WayangJoin> implements Serializable {

    /**
     * Visitor that visits join statements that has multiple conditions like: AND(=($1,$2),=($2,$3))
     * Note that this doesnt support nway joins or multijoins.
     * @param wayangRelConverter
     * @param aliasFinder
     */
    WayangMultiConditionJoinVisitor(final WayangRelConverter wayangRelConverter, final AliasFinder aliasFinder) {
        super(wayangRelConverter, aliasFinder);
    }

    @Override
    Operator visit(final WayangJoin wayangRelNode) {
        final Operator childOpLeft = wayangRelConverter.convert(wayangRelNode.getInput(0), super.aliasFinder);
        final Operator childOpRight = wayangRelConverter.convert(wayangRelNode.getInput(1), super.aliasFinder);

        final RexNode condition = ((Join) wayangRelNode).getCondition();

        final RexCall call = (RexCall) condition;

        final List<RexCall> subConditions = call.operands.stream()
                .map(RexCall.class::cast)
                .collect(Collectors.toList());

        // calcite generates the RexInputRef indexes via looking at the union
        // field list of the left and right input of a join.
        // since the left input is always the first in this joined field list
        // we can eagerly get the fields in the left input
        final List<RexInputRef> leftTableInputRefs = subConditions.stream()
                .map(sub -> sub.getOperands().stream()
                        .map(RexInputRef.class::cast)
                        .min((left, right) -> Integer.compare(left.getIndex(), right.getIndex()))
                        .get())
                .collect(Collectors.toList());

        final Integer[] leftTableKeyIndexes = leftTableInputRefs.stream()
                .map(RexInputRef::getIndex)
                .toArray(Integer[]::new);

        // for the right table input refs, the indexes are offset by the amount of rows
        // in the left
        // input to the join
        final List<RexInputRef> rightTableInputRefs = subConditions.stream()
                .map(sub -> sub.getOperands().stream()
                        .map(RexInputRef.class::cast)
                        .max((left, right) -> Integer.compare(left.getIndex(), right.getIndex()))
                        .get())
                .collect(Collectors.toList());

        final Integer[] rightTableKeyIndexes = rightTableInputRefs.stream()
                .map(RexInputRef::getIndex)
                .map(key -> key - wayangRelNode.getLeft().getRowType().getFieldCount()) // apply offset
                .toArray(Integer[]::new);

        // fetch table names from joining columns
        final Map<RelDataTypeField, String> columnToOriginMapLeft = CalciteSources
                .createColumnToTableOriginMap(wayangRelNode.getInput(0));
        final Map<RelDataTypeField, String> columnToOriginMapRight = CalciteSources
                .createColumnToTableOriginMap(wayangRelNode.getInput(1));

        final JoiningTableExtractor leftVisitor = new JoiningTableExtractor(true);
        leftVisitor.visit(wayangRelNode.getLeft(), wayangRelNode.getId(), null);

        final JoiningTableExtractor rightVisitor = new JoiningTableExtractor(true);
        rightVisitor.visit(wayangRelNode.getRight(), wayangRelNode.getId(), null);

        // TODO: prolly breaks on bushy joins
        final String joiningTableName = leftVisitor.getName() instanceof String
                ? leftVisitor.getName()
                : rightVisitor.getName();

        final List<RelDataTypeField> leftFields = Arrays.stream(leftTableKeyIndexes)
                .map(key -> wayangRelNode.getLeft().getRowType().getFieldList().get(key))
                .collect(Collectors.toList());

        final List<RelDataTypeField> rightFields = Arrays.stream(rightTableKeyIndexes)
                .map(key -> wayangRelNode.getRight().getRowType().getFieldList().get(key))
                .collect(Collectors.toList());

        // since we assume that tables have a representation in a join like: {leftInput,
        // RightInput}
        // and multicondition joins only representing two tables we should be able to
        // optimistically get the table names for the left and right input by using any
        // field
        // from either input.
        final String leftTableName = columnToOriginMapLeft.get(
                wayangRelNode.getLeft().getRowType().getFieldList().get(0));
        final String rightTableName = columnToOriginMapRight
                .get(wayangRelNode.getRight().getRowType().getFieldList().get(0));

        final List<String> leftCatalog = CalciteSources.getSqlColumnNames(wayangRelNode.getLeft());
        final List<String> rightCatalog = CalciteSources.getSqlColumnNames(wayangRelNode.getRight());

        final List<String> cleanedLeftFieldNames = leftFields.stream()
                .map(leftField -> CalciteSources
                        .findSqlName(CalciteSources.tableNameOriginOf(wayangRelNode.getLeft(), leftField.getIndex())
                                + "." + leftField.getName(), leftCatalog))
                .collect(Collectors.toList());

        final List<String> cleanedRightFieldNames = rightFields.stream()
                .map(rightField -> CalciteSources
                        .findSqlName(CalciteSources.tableNameOriginOf(wayangRelNode.getRight(), rightField.getIndex())
                                + "." + rightField.getName(), rightCatalog))
                .collect(Collectors.toList());

        final String cleanLeftFieldString = cleanedLeftFieldNames.stream().collect(Collectors.joining(","));
        final String cleanRightFieldString = cleanedRightFieldNames.stream().collect(Collectors.joining(","));

        // if join is joining the LHS of a join condition "JOIN left ON left = right"
        // then we pick the first case, otherwise the 2nd "JOIN right ON left = right"
        final JoinOperator<Record, Record, Record> join = joiningTableName == leftTableName
                ? this.getJoinOperator(
                        leftTableKeyIndexes,
                        rightTableKeyIndexes,
                        wayangRelNode,
                        leftTableName + " AS " + leftTableName,
                        cleanLeftFieldString,
                        rightTableName,
                        cleanRightFieldString)
                : this.getJoinOperator(
                        leftTableKeyIndexes,
                        rightTableKeyIndexes,
                        wayangRelNode,
                        rightTableName + " AS " + rightTableName,
                        cleanRightFieldString,
                        leftTableName,
                        cleanLeftFieldString);

        childOpLeft.connectTo(0, join, 0);
        childOpRight.connectTo(0, join, 1);

        final String[] joinTableNames = Stream.concat(cleanedLeftFieldNames.stream(), cleanedRightFieldNames.stream())
                .toArray(String[]::new);

        // { leftTableName + "." + leftFieldName, rightTableName + "." + rightFieldName
        // };

        // Join returns Tuple2 - map to a Record
        final SerializableFunction<Tuple2<Record, Record>, Record> mp = new MultiMapFunctionImpl();
        final ProjectionDescriptor<Tuple2<Record, Record>, Record> pd = new ProjectionDescriptor<Tuple2<Record, Record>, Record>(
                mp, (Class<Tuple2<Record, Record>>) (Class<?>) Tuple2.class, Record.class, joinTableNames);

        final MapOperator<Tuple2<Record, Record>, Record> mapOperator = new MapOperator<Tuple2<Record, Record>, Record>(
                pd);

        join.connectTo(0, mapOperator, 0);

        return mapOperator;
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
    protected JoinOperator<Record, Record, Record> getJoinOperator(final Integer[] leftKeyIndexes,
            final Integer[] rightKeyIndexes,
            final WayangJoin wayangRelNode, final String leftTableName, final String leftFieldNames,
            final String rightTableName, final String rightFieldNames) {
        if (wayangRelNode.getInputs().size() != 2)
            throw new UnsupportedOperationException("Join had an unexpected amount of inputs, found: "
                    + wayangRelNode.getInputs().size() + ", expected: 2");

        // todo: output type is incorrect
        final TransformationDescriptor<Record, Record> leftProjectionDescriptor = new ProjectionDescriptor<Record, Record>(
                new MultiKeyExtractor(leftKeyIndexes),
                Record.class, Record.class, leftFieldNames)
                .withSqlImplementation(Optional.ofNullable(leftTableName).orElse(""), leftFieldNames);

        final TransformationDescriptor<Record, Record> rightProjectionDescriptor = new ProjectionDescriptor<Record, Record>(
                new MultiKeyExtractor(rightKeyIndexes),
                Record.class, Record.class, rightFieldNames)
                .withSqlImplementation(Optional.ofNullable(rightTableName).orElse(""), rightFieldNames);

        final JoinOperator<Record, Record, Record> join = new JoinOperator<>(
                leftProjectionDescriptor,
                rightProjectionDescriptor);

        return join;
    }
}
