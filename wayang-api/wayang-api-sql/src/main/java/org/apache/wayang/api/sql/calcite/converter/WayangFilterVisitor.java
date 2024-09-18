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
import java.util.Optional;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.wayang.api.sql.calcite.rel.WayangFilter;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;

import java.lang.StackWalker.Option;
import java.util.EnumSet;

public class WayangFilterVisitor extends WayangRelNodeVisitor<WayangFilter> {
    WayangFilterVisitor(WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(WayangFilter wayangRelNode) {

        Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(0));

        RexNode condition = ((Filter) wayangRelNode).getCondition();

        FilterOperator<Record> filter = new FilterOperator(
                new FilterPredicateImpl(condition),
                Record.class
        );

        childOp.connectTo(0,filter,0);

        return filter;
    }

    private class FilterPredicateImpl implements FunctionDescriptor.SerializablePredicate<Record> {

        private final RexNode condition;

        private FilterPredicateImpl(RexNode condition) {
            this.condition = condition;
        }

        @Override
        public boolean test(Record record) {
            return condition.accept(new EvaluateFilterCondition(true, record));
        }
    }

    private class EvaluateFilterCondition extends RexVisitorImpl<Boolean> {

        final Record record;
        protected EvaluateFilterCondition(boolean deep, Record record) {
            super(deep);
            this.record = record;
        }

        @Override
        public Boolean visitCall(RexCall call) {
            SqlKind kind = call.getKind();

            if(!kind.belongsTo(SUPPORTED_OPS)) {
                throw new IllegalStateException("Cannot handle this filter predicate yet");
            }

            RexNode leftOperand = call.getOperands().get(0);
            RexNode rightOperand = call.getOperands().get(1);

            switch(kind){
                case AND:
                    return leftOperand.accept(this) && rightOperand.accept(this);
                case OR:
                    return leftOperand.accept(this) || rightOperand.accept(this);
                default:
                    return eval(record, kind, leftOperand, rightOperand);
            }
        }

        public boolean eval(Record record, SqlKind kind, RexNode leftOperand, RexNode rightOperand) {
            if(leftOperand instanceof RexInputRef && rightOperand instanceof RexLiteral) {
                RexInputRef rexInputRef = (RexInputRef) leftOperand;
                int index = rexInputRef.getIndex();
                Optional<?> field = Optional.ofNullable(record.getField(index));
                RexLiteral rexLiteral = (RexLiteral) rightOperand;
                
                switch (kind) {
                    case LIKE:
                        return this.like(field, rexLiteral);
                    case GREATER_THAN:
                        return isGreaterThan(field, rexLiteral);
                    case LESS_THAN:
                        return isLessThan(field, rexLiteral);
                    case EQUALS:
                        return isEqualTo(field, rexLiteral);
                    case GREATER_THAN_OR_EQUAL:
                        return isGreaterThan(field, rexLiteral) || isEqualTo(field, rexLiteral);
                    case LESS_THAN_OR_EQUAL:
                        return isLessThan(field, rexLiteral) || isEqualTo(field, rexLiteral);
                    default:
                        throw new IllegalStateException("Predicate not supported yet");
                }
            } else if (leftOperand instanceof RexInputRef && rightOperand instanceof RexInputRef) {  //filters with column a = column b
                RexInputRef leftRexInputRef = (RexInputRef) leftOperand;
                int leftIndex = leftRexInputRef.getIndex();
                RexInputRef righRexInputRef = (RexInputRef) rightOperand;
                int rightIndex = righRexInputRef.getIndex();

                Optional<?> leftField = Optional.ofNullable(record.getField(leftIndex));
                Optional<?> rightField = Optional.ofNullable(record.getField(rightIndex));

                switch (kind) {
                    case EQUALS:
                        return isEqualTo(leftField, rightField);
                    default:
                        throw new IllegalStateException("Predicate not supported yet");
                }
            }
            else {
                throw new IllegalStateException("Predicate not supported yet");
            }
        }

        private boolean like(Optional<?> o, RexLiteral toCompare) {       
            String unwrapped = o.map(s -> (String) s).orElse("");
            boolean isMatch = SqlFunctions.like(unwrapped, toCompare
                .toString()
                .replace("'", "") //the calcite sqlToRegex api needs input w/o 's
            );

            return isMatch;
        }

        private boolean isGreaterThan(Optional<?> o, RexLiteral rexLiteral) {
            Object unwrapped = o.orElseThrow(() -> new IllegalStateException("isGreaterThan not supported for null objects, object was: " + o + ", rexLiteral was: " + rexLiteral));
            return ((Comparable) unwrapped).compareTo(rexLiteral.getValueAs(unwrapped.getClass())) > 0;
        }

        private boolean isLessThan(Optional<?> o, RexLiteral rexLiteral) {
            Object unwrapped = o.orElseThrow(() -> new IllegalStateException("isLessThan not supported for null objects, object was: " + o + ", rexLiteral was: " + rexLiteral));
            return ((Comparable) unwrapped).compareTo(rexLiteral.getValueAs(unwrapped.getClass())) < 0;
        }

        private boolean isEqualTo(Optional<?> o, RexLiteral rexLiteral) {
            try {
                if(o.isEmpty() && rexLiteral.isNull()) return true;
                if(o.isPresent()) return ((Comparable) o.get()).compareTo(rexLiteral.getValueAs(o.get().getClass())) == 0;
                return false;
            } catch (Exception e) {
                throw new IllegalStateException("Predicate not supported yet, something went wrong when computing an isEqualTo predicate, object: " + o + " rexLiteral: " + rexLiteral + " rexLiteral kind: " + rexLiteral.getKind() + " rexLiteral type: " + rexLiteral.getType());
            }
        }

        private boolean isEqualTo(Optional<?> o, Optional<?> o2) {
            try {
                if(o.isEmpty() && o2.isEmpty()) return true;
                if(o.isPresent() && o2.isPresent()) return ((Comparable) o.get()).compareTo(o2.get()) == 0;
                return false;
            } catch (Exception e) {
                throw new IllegalStateException("Predicate not supported yet, something went wrong when computing an isEqualTo predicate, object: " + o + " object2: " + o2);
            }
        }
    }

    /**for quick sanity check **/
    private static final EnumSet<SqlKind> SUPPORTED_OPS =
            EnumSet.of(SqlKind.AND, SqlKind.OR,
                    SqlKind.EQUALS, SqlKind.NOT_EQUALS,
                    SqlKind.LESS_THAN, SqlKind.GREATER_THAN,
                    SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN_OR_EQUAL, SqlKind.LIKE);
}
