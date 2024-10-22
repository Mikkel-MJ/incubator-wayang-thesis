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

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.KeyExtractor;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.KeyIndex;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.MapFunctionImpl;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.api.sql.calcite.utils.SqlField;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;

public class WayangJoinVisitor extends WayangRelNodeVisitor<WayangJoin> implements Serializable {

    WayangJoinVisitor(final WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(final WayangJoin wayangRelNode) {
        final Operator childOpLeft = wayangRelConverter.convert(wayangRelNode.getInput(0));
        final Operator childOpRight = wayangRelConverter.convert(wayangRelNode.getInput(1));

        final RexNode condition = ((Join) wayangRelNode).getCondition();

        if (!condition.isA(SqlKind.EQUALS)) {
            throw new UnsupportedOperationException("Only equality joins supported but got: " + condition.getKind() + " from relNode: " + wayangRelNode + ", with inputs: " + wayangRelNode.getInputs());
        } 
        
        int leftKeyIndex = condition.accept(new KeyIndex(false, Child.LEFT));
        int rightKeyIndex = condition.accept(new KeyIndex(false, Child.RIGHT));

        //init join
        JoinOperator<Record, Record, SqlField> join;
        //calculate offsets
        if (leftKeyIndex > rightKeyIndex) { //if the table index on the left is larger than the right
            leftKeyIndex -= wayangRelNode.getInput(0).getRowType().getFieldCount();

            join = new JoinOperator<Record, Record, SqlField>(
                new TransformationDescriptor<Record, SqlField>(new KeyExtractor<SqlField>(rightKeyIndex), Record.class, SqlField.class),
                new TransformationDescriptor<Record, SqlField>(new KeyExtractor<SqlField>(leftKeyIndex), Record.class, SqlField.class)
            );

        } else if (rightKeyIndex > leftKeyIndex) {//standard case 
            rightKeyIndex -= wayangRelNode.getInput(0).getRowType().getFieldCount();
        
            join = new JoinOperator<>(
                new TransformationDescriptor<Record, SqlField>(new KeyExtractor<SqlField>(leftKeyIndex), Record.class, SqlField.class),
                new TransformationDescriptor<Record, SqlField>(new KeyExtractor<SqlField>(rightKeyIndex), Record.class, SqlField.class)
            );
        } else {
            throw new UnsupportedOperationException("Could not compute offset for condition: " + condition + " left key index: " + leftKeyIndex + " right key index: " + rightKeyIndex);
        }

        //call connectTo on both operators (left and right)
        childOpLeft.connectTo(0, join, 0);
        childOpRight.connectTo(0, join, 1);
        
        // Join returns Tuple2 - map to a Record
        final MapOperator<Tuple2, Record> mapOperator = new MapOperator(
                new MapFunctionImpl(),
                Tuple2.class,
                Record.class
        );

        join.connectTo(0, mapOperator, 0);

        return mapOperator;
    }

    // Helpers
    public static enum Child {
        LEFT, RIGHT
    }
}
