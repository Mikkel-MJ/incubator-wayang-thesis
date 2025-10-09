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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.DoubleBinaryOperator;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import org.apache.wayang.api.sql.calcite.converter.projecthelpers.ProjectMapFuncImpl;
import org.apache.wayang.api.sql.calcite.rel.WayangProject;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.wayangplan.Operator;

public class WayangProjectVisitor extends WayangRelNodeVisitor<WayangProject> {
    public static Object evaluateRexCall(final Record rec, final RexCall rexCall) {
        if (rexCall == null) {
            return null;
        }

        // Get the operator and operands
        final SqlOperator operator = rexCall.getOperator();
        final List<RexNode> operands = rexCall.getOperands();

        if (operator == SqlStdOperatorTable.PLUS) {
            // Handle addition
            return evaluateNaryOperation(rec, operands, Double::sum);
        } else if (operator == SqlStdOperatorTable.MINUS) {
            // Handle subtraction
            return evaluateNaryOperation(rec, operands, (a, b) -> a - b);
        } else if (operator == SqlStdOperatorTable.MULTIPLY) {
            // Handle multiplication
            return evaluateNaryOperation(rec, operands, (a, b) -> a * b);
        } else if (operator == SqlStdOperatorTable.DIVIDE) {
            // Handle division
            return evaluateNaryOperation(rec, operands, (a, b) -> a / b);
        } else {
            return null;
        }
    }

    public static Object evaluateNaryOperation(final Record rec, final List<RexNode> operands,
            final DoubleBinaryOperator operation) {
        if (operands.isEmpty()) {
            return null;
        }

        final List<Double> values = new ArrayList<>();

        for (int i = 0; i < operands.size(); i++) {
            final Number val = (Number) evaluateRexNode(rec, operands.get(i));
            if (val == null) {
                return null;
            }
            values.add(val.doubleValue());
        }

        Object result = values.get(0);
        // Perform the operation with the remaining operands
        for (int i = 1; i < operands.size(); i++) {
            result = operation.applyAsDouble((Double) result, values.get(i));
        }

        return result;
    }

    public static Object evaluateRexNode(final Record rec, final RexNode rexNode) {
        if (rexNode instanceof RexCall) {
            // Recursively evaluate a RexCall
            return evaluateRexCall(rec, (RexCall) rexNode);
        } else if (rexNode instanceof RexLiteral) {
            // Handle literals (e.g., numbers)
            final RexLiteral literal = (RexLiteral) rexNode;
            return literal.getValue();
        } else if (rexNode instanceof RexInputRef) {
            return rec.getField(((RexInputRef) rexNode).getIndex());
        } else {
            return null; // Unsupported or unknown expression
        }
    }

    WayangProjectVisitor(final Configuration configuration) {
        super(configuration);
    }

    @Override
    Operator visit(final WayangProject wayangRelNode) {
        final Operator childOp = WayangRelConverter.convert(wayangRelNode.getInput(0), super.configuration);

        // list of projects passed to the serializable function, for java & others usage
        final List<RexNode> projects = wayangRelNode.getProjects();
        final ProjectMapFuncImpl impl = new ProjectMapFuncImpl(projects);

        final ProjectionDescriptor<Record, Record> pd = new ProjectionDescriptor<>(
                impl,
                Record.class,
                Record.class);

        final List<String> fields = impl.getCallTrees().stream().map(node -> node.toString(wayangRelNode.getInput().getRowType().getFieldList())).collect(Collectors.toList());
        final List<String> aliases = wayangRelNode.getRowType().getFieldNames();

        final String[] aliasedFields = new String[fields.size()];

        for (int i = 0; i < aliasedFields.length; i++) {
            aliasedFields[i] = fields.get(i) + " AS " + aliases.get(i);
        }

        pd.withSqlImplementation(Arrays.stream(aliasedFields).collect(Collectors.joining(",")));

        final MapOperator<Record, Record> projection = new MapOperator<>(pd);

        childOp.connectTo(0, projection, 0);

        return projection;
    }
}
