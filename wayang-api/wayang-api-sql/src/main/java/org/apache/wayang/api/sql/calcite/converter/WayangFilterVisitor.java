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
import java.util.EnumSet;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.wayang.api.sql.calcite.converter.filterhelpers.FilterPredicateImpl;
import org.apache.wayang.api.sql.calcite.rel.WayangFilter;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;

public class WayangFilterVisitor extends WayangRelNodeVisitor<WayangFilter> implements Serializable {
    /** for quick sanity check **/
    public static final EnumSet<SqlKind> SUPPORTED_OPS = EnumSet.of(SqlKind.AND, SqlKind.OR,
            SqlKind.EQUALS, SqlKind.NOT_EQUALS,
            SqlKind.LESS_THAN, SqlKind.GREATER_THAN,
            SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN_OR_EQUAL,
            SqlKind.NOT, SqlKind.LIKE, SqlKind.IS_NOT_NULL, SqlKind.IS_NULL, SqlKind.SEARCH);

    WayangFilterVisitor(final Configuration configuration) {
        super(configuration);
    }

    @Override
    Operator visit(final WayangFilter wayangRelNode) {
        final Operator childOp = WayangRelConverter.convert(wayangRelNode.getInput(0), super.configuration);
        final RexNode condition = wayangRelNode.getCondition();
        final FilterPredicateImpl impl = new FilterPredicateImpl(condition);

        final PredicateDescriptor<Record> pd = new PredicateDescriptor<>(
                impl,
                Record.class);

        pd.withSqlImplementation(impl.getCallTree().toString(wayangRelNode.getRowType().getFieldList()));

        final FilterOperator<Record> filter = new FilterOperator<>(pd);

        childOp.connectTo(0, filter, 0);

        return filter;
    }
}
