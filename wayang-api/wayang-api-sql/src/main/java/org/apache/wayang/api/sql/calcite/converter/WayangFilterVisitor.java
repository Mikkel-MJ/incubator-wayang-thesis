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

import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import org.apache.wayang.api.sql.calcite.converter.filterhelpers.ColumnIndexExtractor;
import org.apache.wayang.api.sql.calcite.converter.filterhelpers.FilterPredicateImpl;
import org.apache.wayang.api.sql.calcite.converter.filterhelpers.FunctionExtractor;
import org.apache.wayang.api.sql.calcite.rel.WayangFilter;
import org.apache.wayang.api.sql.calcite.utils.AliasFinder;
import org.apache.wayang.api.sql.calcite.utils.CalciteSources;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WayangFilterVisitor extends WayangRelNodeVisitor<WayangFilter> implements Serializable {
    WayangFilterVisitor(final WayangRelConverter wayangRelConverter, final AliasFinder aliasFinder) {
        super(wayangRelConverter, aliasFinder);
    }

    @Override
    Operator visit(final WayangFilter wayangRelNode) {
        final Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(0), super.aliasFinder);
        final RexNode condition = ((Filter) wayangRelNode).getCondition();

        final List<Integer> affectedColumnIndexes = condition.accept(new ColumnIndexExtractor(true));

        final List<RelDataTypeField> affectedColumns = affectedColumnIndexes.stream()
                .map(index -> wayangRelNode.getRowType().getFieldList().get(index))
                .collect(Collectors.toList());

        final Map<RelDataTypeField, String> columnToTableOrigin = CalciteSources
                .createColumnToTableOriginMap(wayangRelNode);

        final List<String> catalog = CalciteSources.getSqlColumnNames(wayangRelNode);

        final String[] tableSpecifiedColumns = affectedColumns.stream() // get affected filter columns
                .map(col -> {
                    final String dirtyName = columnToTableOrigin.get(col) + "." + col.getName();
                    final String cleanedName = CalciteSources.findSqlName(dirtyName, catalog);
                    final String aliasedName = aliasFinder.columnIndexToTableName.get(col.getIndex()) + "."
                            + cleanedName.split("\\.")[1];
                    return aliasedName;
                }) // map to table.col name
                .toArray(String[]::new);

        final PredicateDescriptor<Record> pd = new PredicateDescriptor<>(new FilterPredicateImpl(condition),
                Record.class);

        final String extractedFilterFunctions = condition
                .accept(new FunctionExtractor(true, affectedColumnIndexes, tableSpecifiedColumns));

        System.out.println("extracted filter functions: " + extractedFilterFunctions);
        pd.withSqlImplementation(extractedFilterFunctions);
        final FilterOperator<Record> filter = new FilterOperator<>(pd);

        childOp.connectTo(0, filter, 0);

        return filter;
    }

    /** for quick sanity check **/
    public static final EnumSet<SqlKind> SUPPORTED_OPS = EnumSet.of(SqlKind.AND, SqlKind.OR,
            SqlKind.EQUALS, SqlKind.NOT_EQUALS,
            SqlKind.LESS_THAN, SqlKind.GREATER_THAN,
            SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN_OR_EQUAL,
            SqlKind.NOT, SqlKind.LIKE, SqlKind.IS_NOT_NULL, SqlKind.IS_NULL);
}
