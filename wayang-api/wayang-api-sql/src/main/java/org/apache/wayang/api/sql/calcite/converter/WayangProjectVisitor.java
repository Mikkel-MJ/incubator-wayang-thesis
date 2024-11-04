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

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.wayang.api.sql.calcite.converter.projecthelpers.MapFunctionImpl;
import org.apache.wayang.api.sql.calcite.rel.WayangProject;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BinaryOperator;

    public class WayangProjectVisitor extends WayangRelNodeVisitor<WayangProject> {
    WayangProjectVisitor(final WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(final WayangProject wayangRelNode) {
        final Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(0));

        final String[] projectNames = wayangRelNode.getNamedProjects()
            .stream()
            .map(tup -> tup.right)
            .toArray(String[]::new);

        RelColumnOrigin rco = wayangRelNode.getCluster().getMetadataQuerySupplier().get().getColumnOrigin(wayangRelNode, 0);
        System.out.println("rco name: " + rco.getOriginTable().getQualifiedName());
        final List<RexNode> projects = ((Project) wayangRelNode).getProjects();

        System.out.println("this conv: " + wayangRelNode.getConvention());
        System.out.println("this map: "  + wayangRelNode.getMapping());
        System.out.println("this perm: " + wayangRelNode.getPermutation());
        System.out.println("this matr: " + wayangRelNode.getCluster().getPlanner().getMaterializations()); 
        System.out.println("this table ref: " + wayangRelNode.getTable());
        System.out.println("this row type "   + wayangRelNode.getRowType());
        System.out.println("this row type fields: " + wayangRelNode.getRowType().getFieldNames());
        System.out.println("this row type field comp: " + wayangRelNode.getRowType().getComponentType());
        System.out.println("this row type field list item: " + wayangRelNode.getRowType().getFieldList().get(0));
        System.out.println("this row type field list: " + wayangRelNode.getRowType().getFieldList());
        System.out.println("this row type " + wayangRelNode.getRowType().getFieldList().get(0));
        System.out.println("inputs: " + wayangRelNode.getInputs());
        System.out.println("input 0: " + wayangRelNode.getInput(0));
        System.out.println("input 0, input 0: " + wayangRelNode.getInput(0).getInput(0));
        System.out.println("input 0 table: " + wayangRelNode.getInput(0).getTable());
        System.out.println("input 0 rowtype: " + wayangRelNode.getInput(0).getRowType());
        System.out.println("input 0 fieldnames: " + wayangRelNode.getInput(0).getRowType().getFieldNames());
        System.out.println("projects left: " + wayangRelNode.getNamedProjects().get(0).left);
        System.out.println("projects 0: " + wayangRelNode.getNamedProjects().get(0));
        System.out.println("this table: " + wayangRelNode.getTable());
        System.out.println("this: " + this);
        System.out.println("project names : " + projectNames);
        System.out.println("projects 0 type: " + wayangRelNode.getProjects().get(0).getType());
        System.out.println("proj out ");
        System.out.println(wayangRelNode.getProjects().get(0).getType().getFullTypeString());
        System.out.println(wayangRelNode.getProjects().get(0).getType().getComponentType());
        System.out.println(wayangRelNode.getProjects().get(0).getType().getFamily());
        System.out.println(wayangRelNode.getProjects().get(0).getType().getIntervalQualifier());
        System.out.println(wayangRelNode.getProjects().get(0).getType().getKeyType());
        System.out.println(wayangRelNode.getProjects().get(0).getType().getPrecedenceList());
        System.out.println(wayangRelNode.getProjects().get(0).getType().getFieldList());
        System.out.println(wayangRelNode.getProjects().get(0).getType().getSqlIdentifier());
        System.out.println(wayangRelNode.getProjects().get(0).getType().getSqlTypeName());
        System.out.println("input table: " + wayangRelNode.getInput().getTable());
        System.out.println("child op name: " + childOp.getName());
        System.out.println("child op: " + childOp);
        System.out.println("child op inputs: " + childOp.getAllInputs());
        System.out.println("child op inputs: " + childOp.getAllOutputs());
        
        final ProjectionDescriptor<Record, Record> pd = new ProjectionDescriptor<>(
            new MapFunctionImpl(projects), 
            Record.class, 
            Record.class, 
            projectNames //names of projected columns
        );

        final MapOperator<Record, Record> projection  = new MapOperator<Record, Record>(pd);
    
        System.out.println("in node " + wayangRelNode);
        Arrays.stream(projection.getAllInputs()).forEach(slot -> System.out.println("proj in " + slot));
        Arrays.stream(projection.getAllOutputs()).forEach(slot -> System.out.println("proj out " + slot));
        Arrays.stream(childOp.getAllInputs()).forEach(slot -> System.out.println("child in " + slot));
        Arrays.stream(childOp.getAllOutputs()).forEach(slot -> System.out.println("child out " + slot));

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