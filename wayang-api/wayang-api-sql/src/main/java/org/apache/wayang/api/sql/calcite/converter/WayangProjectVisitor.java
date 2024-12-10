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
 *
 */

package org.apache.wayang.api.sql.calcite.converter;

import org.apache.wayang.api.sql.calcite.converter.projecthelpers.MapFunctionImpl;
import org.apache.wayang.api.sql.calcite.rel.WayangProject;
import org.apache.wayang.api.sql.calcite.utils.AliasFinder;
import org.apache.wayang.api.sql.calcite.utils.CalciteSources;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

public class WayangProjectVisitor extends WayangRelNodeVisitor<WayangProject> {
    WayangProjectVisitor(final WayangRelConverter wayangRelConverter, AliasFinder aliasFinder) {
        super(wayangRelConverter, aliasFinder);
    }

    @Override
    Operator visit(final WayangProject wayangRelNode) {
        final Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(0), super.aliasFinder);

        final Map<RelDataTypeField, String> fieldToTableOrigin = CalciteSources.createColumnToTableOriginMap(wayangRelNode);

        final List<RelDataTypeField> projectFields = wayangRelNode // get columns to be projected
                .getRowType()
                .getFieldList();

        System.out.println("input field list: " + wayangRelNode.getInput().getRowType().getFieldList());

        final Map<String, String> unAliasedNamesMap = wayangRelNode.getNamedProjects()
            .stream()
            .collect(Collectors.toMap(
                    project -> project.right, // key: project column index
                    project -> wayangRelNode
                        .getInput()
                        .getRowType()
                        .getFieldNames()
                        .get(project.left.hashCode())// value: unaliased name
            ));
        List<Integer> columnIndexes = wayangRelNode.getProjects().stream().map(proj -> proj.hashCode()).collect(Collectors.toList());
        List<RelDataTypeField> fields = columnIndexes.stream().map(colindex -> wayangRelNode.getInput().getRowType().getFieldList().get(colindex)).collect(Collectors.toList());
        System.out.println("proj oclumn indexes: " + columnIndexes);
        System.out.println("proj fields: " + fields);
        System.out.println("aliased: " + fields.stream().map(field -> aliasFinder.columnIndexToTableName.get(field.getIndex())).collect(Collectors.toList()));

        final List<String> catalog = CalciteSources.getSqlColumnNames(wayangRelNode);

        System.out.println("project catalog: " + catalog);
        final String[] projectNames = projectFields 
            .stream()
            .map(field -> fieldToTableOrigin.get(field) + "." + unAliasedNamesMap.get(field.getName()))
            .map(badName -> CalciteSources.findSqlName(badName, catalog))
            .toArray(String[]::new);

        final List<RexNode> projects = ((Project) wayangRelNode).getProjects();

        final ProjectionDescriptor<Record, Record> pd = new ProjectionDescriptor<>(
                new MapFunctionImpl(projects),
                Record.class,
                Record.class,
                projectNames // names of projected columns
        );
        
        final MapOperator<Record, Record> projection = new MapOperator<Record, Record>(pd);

        childOp.connectTo(0, projection, 0);

        return projection;
    }

    public static Object evaluateRexCall(final Record record, final RexCall rexCall) {
        if (rexCall == null) {
            return null;
        }

        // Get the operator and operands
        final SqlOperator operator = rexCall.getOperator();
        final List<RexNode> operands = rexCall.getOperands();

        if (operator == SqlStdOperatorTable.PLUS) {
            // Handle addition
            return evaluateNaryOperation(record, operands, Double::sum);
        } else if (operator == SqlStdOperatorTable.MINUS) {
            // Handle subtraction
            return evaluateNaryOperation(record, operands, (a, b) -> a - b);
        } else if (operator == SqlStdOperatorTable.MULTIPLY) {
            // Handle multiplication
            return evaluateNaryOperation(record, operands, (a, b) -> a * b);
        } else if (operator == SqlStdOperatorTable.DIVIDE) {
            // Handle division
            return evaluateNaryOperation(record, operands, (a, b) -> a / b);
        } else {
            return null;
        }
    }

    public static Object evaluateNaryOperation(final Record record, final List<RexNode> operands,
            final BinaryOperator<Double> operation) {
        if (operands.isEmpty()) {
            return null;
        }

        final List<Double> values = new ArrayList<>();

        for (int i = 0; i < operands.size(); i++) {
            final Number val = (Number) evaluateRexNode(record, operands.get(i));
            if (val == null) {
                return null;
            }
            values.add(val.doubleValue());
        }

        Object result = values.get(0);
        // Perform the operation with the remaining operands
        for (int i = 1; i < operands.size(); i++) {
            result = operation.apply((double) result, values.get(i));
        }

        return result;
    }

    public static Object evaluateRexNode(final Record record, final RexNode rexNode) {
        if (rexNode instanceof RexCall) {
            // Recursively evaluate a RexCall
            return evaluateRexCall(record, (RexCall) rexNode);
        } else if (rexNode instanceof RexLiteral) {
            // Handle literals (e.g., numbers)
            final RexLiteral literal = (RexLiteral) rexNode;
            return literal.getValue();
        } else if (rexNode instanceof RexInputRef) {
            return record.getField(((RexInputRef) rexNode).getIndex());
        } else {
            return null; // Unsupported or unknown expression
        }
    }
}