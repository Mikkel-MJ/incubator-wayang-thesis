package org.apache.wayang.api.sql.calcite.converter.filterhelpers;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;

public class FilterPredicateImpl implements FunctionDescriptor.SerializablePredicate<Record> {
    class FilterCallTreeFactory implements CallTreeFactory {
        public SerializableFunction<List<Object>, Object> deriveOperation(final SqlKind kind) {
            return new SerializableFunction<List<Object>, Object>() {
                @Override
                public Object apply(List<Object> input) {
                    switch (kind) {
                        case NOT:
                            return !(boolean) input.get(0);
                        case IS_NOT_NULL:
                            return !isEqualTo(input.get(0), null);
                        case IS_NULL:
                            return isEqualTo(input.get(0), null);
                        case LIKE:
                            return like((String) input.get(0), (String) input.get(1));
                        case NOT_EQUALS:
                            return !isEqualTo(input.get(0), input.get(1));
                        case EQUALS:
                            return isEqualTo(input.get(0), input.get(1));
                        case GREATER_THAN:
                            return isGreaterThan(input.get(0), input.get(1));
                        case LESS_THAN:
                            return isLessThan(input.get(0), input.get(1));
                        case GREATER_THAN_OR_EQUAL:
                            return isGreaterThan(input.get(0), input.get(1)) || isEqualTo(input.get(0), input.get(1));
                        case LESS_THAN_OR_EQUAL:
                            return isLessThan(input.get(0), input.get(1)) || isEqualTo(input.get(0), input.get(1));
                        case AND:
                            return input.stream().allMatch(obj -> Boolean.class.cast(obj).booleanValue());
                        case OR:
                            return input.stream().anyMatch(obj -> Boolean.class.cast(obj).booleanValue());
                        case MINUS:
                            return widenToDouble.apply(input.get(0)) - widenToDouble.apply(input.get(1));
                        case PLUS:
                            return widenToDouble.apply(input.get(0)) + widenToDouble.apply(input.get(1));
                        case SEARCH:
                            if (input.get(0) instanceof ImmutableRangeSet) {
                                ImmutableRangeSet<?> range = (ImmutableRangeSet<?>) input.get(0);
                                if (!(input.get(1) instanceof Comparable)) {
                                    throw new AssertionError("field is not comparable: " + input.get(1).getClass());
                                }
                                Comparable field = ensureComparable.apply(input.get(1));
                                Comparable left = ensureComparable.apply(range.span().lowerEndpoint());
                                Comparable right = ensureComparable.apply(range.span().upperEndpoint());
                                Range<Comparable> newRange = Range.closed(left, right);
                                return newRange.contains(field);
                            } else if (input.get(1) instanceof ImmutableRangeSet) {
                                final ImmutableRangeSet<?> range = (ImmutableRangeSet<?>) input.get(1);
                                assert input.get(0) == null || input.get(0) instanceof Comparable : "left input should be null or comparable.";

                                final Comparable field = ensureComparable.apply(input.get(0));
                                final Comparable left = ensureComparable.apply(range.span().lowerEndpoint());
                                final Comparable right = ensureComparable.apply(range.span().upperEndpoint());
                                final Range<Comparable> newRange = Range.closed(left, right);

                                if (field == null) return false;
                                return newRange.contains(field);
                            } else {
                                throw new UnsupportedOperationException("No range set found in SARG, input1: "
                                        + input.get(0).getClass() + ", input2: " + input.get(1).getClass());
                            }
                        default:
                            throw new UnsupportedOperationException("Kind not supported: " + kind);
                    }
                }
            };
        }

        /**
         * Java equivalent of SQL like clauses
         */
        private boolean like(final String s1, final String s2) {
            return SqlFunctions.like(s1, s2);
        }

        private boolean isGreaterThan(final Object o1, final Object o2) {
            if (o1 == null && o2 == null) return false;
            if (o1 == null) return false;
            if (o2 == null) return true;
            //System.out.println("[FilterPred.gt]: o1 " + o1 + " and o2, " + o2);
            return ensureComparable.apply(o1).compareTo(ensureComparable.apply(o2)) > 0;
        }

        private boolean isLessThan(final Object o1, final Object o2) {
            if (o1 == null && o2 == null) return false;
            if (o1 == null) return true;
            if (o2 == null) return false;
            return ensureComparable.apply(o1).compareTo(ensureComparable.apply(o2)) < 0;
        }

        private boolean isEqualTo(final Object o1, final Object o2) {
            if (o1 == null || o2 == null) return o1 == o2;
            return Objects.equals(ensureComparable.apply(o1), ensureComparable.apply(o2));
        }
    }

    private final Node callTree;

    final SerializableFunction<Object, Double> widenToDouble = new SerializableFunction<Object, Double>() {
        @Override
        public Double apply(Object field) {
            if (field instanceof Number) {
                return ((Number) field).doubleValue();
            } else if (field instanceof Date) {
                return (double) ((Date) field).getTime();
            } else if (field instanceof Calendar) {
                return (double) ((Calendar) field).getTime().getTime();
            } else {
                throw new UnsupportedOperationException("Could not widen to double, field class: " + field.getClass());
            }
        }
    };

    final SerializableFunction<Object, Comparable> ensureComparable = new SerializableFunction<Object, Comparable>() {
        @Override
        public Comparable apply(Object field) {
            if (field instanceof Number) {
                return ((Number) field).doubleValue();
            } else if (field instanceof Date) {
                return (double) ((Date) field).getTime();
            } else if (field instanceof Calendar) {
                return (double) ((Calendar) field).getTime().getTime();
            } else if (field instanceof String) {
                return (String) field;
            } else if (field instanceof NlsString) {
                return ((NlsString) field).getValue();
            } else if (field instanceof Character) {
                return field.toString();
            } else if (field instanceof DateString) {
                return (double) ((DateString) field).getMillisSinceEpoch();
            } else if (field == null) {
                return null;
            } else {
                throw new UnsupportedOperationException(
                        "Type not supported in filter comparisons yet: " + field.getClass());
            }
        }
    };

    public FilterPredicateImpl(final RexNode condition) {
        this.callTree = new FilterCallTreeFactory().fromRexNode(condition);
    }

    @Override
    public boolean test(final Record rec) {
        //System.out.println("[FilterPred.rec]: " + rec);
        return (boolean) callTree.evaluate(rec);
    }
}

