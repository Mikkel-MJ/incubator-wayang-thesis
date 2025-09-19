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
import java.util.List;
import java.util.function.DoubleBinaryOperator;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import org.apache.wayang.api.sql.calcite.converter.projecthelpers.MapFunctionImpl;
import org.apache.wayang.api.sql.calcite.rel.WayangProject;
import org.apache.wayang.api.sql.calcite.utils.AliasFinder;
import org.apache.wayang.api.sql.calcite.utils.CalciteSources;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.MapOperator;
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

    WayangProjectVisitor(final WayangRelConverter wayangRelConverter, final AliasFinder aliasFinder) {
        super(wayangRelConverter, aliasFinder);
    }

    @Override
    Operator visit(final WayangProject wayangRelNode) {
        System.out.println("is an identity mapping:" + RexUtil.isIdentity(wayangRelNode.getProjects(), wayangRelNode.getInput().getRowType()));

        final RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
        final String sampleSql = converter.visitRoot(wayangRelNode).asSelect().toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();

        final RelToSqlConverter converter2 = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
        final String sampleSql2 = converter2.visitRoot(wayangRelNode.getInput()).asSelect().toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();

        System.out.println("[WayangProjectVisitor.queryDiff]: " + sampleSql.replace(sampleSql2, ""));
        System.out.println("[WayangProjectVisitor.sqlInput]: " + sampleSql2);
        System.out.println("[WayangProjectVisitor.sqlSample]: " + sampleSql);

        System.out.println("[WayangProjectVisitor.table]: " + wayangRelNode.getTable());
        System.out.println("[WayangProjectVisitor.inputTableRefs]: " + wayangRelNode.getInput().getCluster().getMetadataQuery().getTableReferences(wayangRelNode.getInput()));
        System.out.println("[WayangProjectVisitor.projections]: " + wayangRelNode.getNamedProjects());
        System.out.println("[WayangProjectVisitor.mapping]: " + wayangRelNode.getMapping());
        System.out.println("[WayangProjectVisitor.permutation]: " + wayangRelNode.getPermutation());

        final List<Integer> originIndexes = wayangRelNode.getRowType().getFieldList().stream().map(
            field -> wayangRelNode.getCluster().getMetadataQuery().getColumnOrigin(wayangRelNode, field.getIndex())
        ).map(RelColumnOrigin::getOriginColumnOrdinal).collect(Collectors.toList());

        final List<RelOptTable> originTables = wayangRelNode.getRowType().getFieldList().stream().map(
            field -> wayangRelNode.getCluster().getMetadataQuery().getColumnOrigin(wayangRelNode, field.getIndex())
        ).map(RelColumnOrigin::getOriginTable).collect(Collectors.toList());

        final List<List<String>> originSetTableNames = wayangRelNode.getRowType().getFieldList().stream().map(
            field -> wayangRelNode.getCluster().getMetadataQuery().getColumnOrigins(wayangRelNode, field.getIndex())
        ).map(set -> set.stream().map(RelColumnOrigin::getOriginTable).map(RelOptTable::getQualifiedName).map(list -> list.get(1)).collect(Collectors.toList())).collect(Collectors.toList());

        final List<String> originTableNames = wayangRelNode.getRowType().getFieldList().stream().map(
            field -> wayangRelNode.getCluster().getMetadataQuery().getColumnOrigin(wayangRelNode, field.getIndex())
        ).map(RelColumnOrigin::getOriginTable).map(RelOptTable::getQualifiedName).map(list -> list.get(1)).collect(Collectors.toList());

        System.out.println("[WayangProjectVisitor.tablerefs]: " + wayangRelNode.getCluster().getMetadataQuery().getTableReferences(wayangRelNode));
        System.out.println("[WayangProjectVisitor.tablerefs indexes]: " + wayangRelNode.getCluster().getMetadataQuery().getTableReferences(wayangRelNode).stream().map(RelTableRef::getEntityNumber).collect(Collectors.toList()));
        System.out.println("[WayangProjectVisitor.originSetTableNames]: " + originSetTableNames);
        System.out.println("[WayangProjectVisitor.originTableNames]: " + originTableNames);

        final ArrayList<RelDataTypeField> originColumns = new ArrayList<>();
        for (int i = 0; i < originIndexes.size(); i++) {
            int index = originIndexes.get(i);
            RelOptTable table = originTables.get(i);

            originColumns.add(table.getRowType().getFieldList().get(index));
        }

        System.out.println("[WayangProjectVisitor.originColumns]: " + originColumns);
        System.out.println("[WayangProjectVisitor.columns]: " + wayangRelNode.getRowType().getFieldList());


        final Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(0), super.aliasFinder);

        // projections on fields:
        System.out.println("[WayangProjectVisitor.dealiasedFields]: " + List.of(CalciteSources.getUnaliasedFields(wayangRelNode)));

        final String[] unaliasedFieldsNames = CalciteSources.getUnaliasedFields(wayangRelNode);
        final String[] unaliasedTableNames  = CalciteSources.getOriginalTableFromColumn(wayangRelNode);
        final String[] aliasedFieldNames    = new String[wayangRelNode.getRowType().getFieldCount()];

        for (int i = 0; i < aliasedFieldNames.length; i++) {
            final String originalName = unaliasedFieldsNames[i];
            final String alias = wayangRelNode.getRowType().getFieldNames().get(i);
            final String originalTableName = unaliasedTableNames[i];

            aliasedFieldNames[i] = originalTableName + "." + originalName + " AS " + alias;
        }

        // list of projects passed to the serializable function, for java & others usage
        final List<RexNode> projects = wayangRelNode.getProjects();

        final ProjectionDescriptor<Record, Record> pd = new ProjectionDescriptor<>(
                new MapFunctionImpl(projects),
                Record.class,
                Record.class,
                aliasedFieldNames // names of projected columns
        );

        final MapOperator<Record, Record> projection = new MapOperator<>(pd);

        childOp.connectTo(0, projection, 0);

        return projection;
    }
}
