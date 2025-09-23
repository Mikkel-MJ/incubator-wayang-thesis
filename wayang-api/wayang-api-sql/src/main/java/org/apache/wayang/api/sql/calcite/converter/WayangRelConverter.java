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

import org.apache.wayang.api.sql.calcite.rel.*;
import org.apache.wayang.api.sql.calcite.utils.AliasFinder;
import org.apache.wayang.core.plan.wayangplan.Operator;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;

public final class WayangRelConverter {

    protected WayangRelConverter () { }

    public static Operator convert(final RelNode node, AliasFinder aliasFinder) {
        if(node instanceof WayangTableScan) {
            return new WayangTableScanVisitor(aliasFinder).visit((WayangTableScan)node);
        } else if (node instanceof WayangProject) {
            return new WayangProjectVisitor(aliasFinder).visit((WayangProject) node);
        } else if (node instanceof WayangFilter) {
            return new WayangFilterVisitor(aliasFinder).visit((WayangFilter) node);
        } else if (node instanceof WayangJoin && ((WayangJoin) node).getCondition().isAlwaysTrue()) {
            return new WayangCrossJoinVisitor(aliasFinder).visit((WayangJoin) node);
        } else if (node instanceof WayangJoin && ((WayangJoin) node).getCondition().isA(SqlKind.AND)) {
            return new WayangMultiConditionJoinVisitor(aliasFinder).visit((WayangJoin) node);
        }else if (node instanceof WayangJoin) {
            return new WayangJoinVisitor(aliasFinder).visit((WayangJoin) node);
        } else if (node instanceof WayangAggregate) {
            return new WayangAggregateVisitor(aliasFinder).visit((WayangAggregate) node);
        }
        throw new IllegalStateException("Operator translation not supported yet " + node);
    }
}
