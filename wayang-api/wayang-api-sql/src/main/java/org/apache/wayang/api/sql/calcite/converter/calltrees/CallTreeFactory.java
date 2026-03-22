package org.apache.wayang.api.sql.calcite.converter.calltrees;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Range;

/**
 * AST of the {@link RexCall} arithmetic, composed into serializable nodes;
 * {@link Call}, {@link InputRef}, {@link Literal}
 */
public interface CallTreeFactory extends Serializable {
    public default Node fromRexNode(final RexNode node) {
        if (node instanceof RexCall) {
            final RexCall call = (RexCall) node;
            return new Call(call, this);
        } else if (node instanceof RexInputRef) {
            final RexInputRef inputRef = (RexInputRef) node;
            return new InputRef(inputRef);
        } else if (node instanceof RexLiteral) {
            final RexLiteral literal = (RexLiteral) node;
            return new Literal(literal);
        } else {
            throw new UnsupportedOperationException("Unsupported RexNode in filter condition: " + node);
        }
    }

    /**
     * Derives the java operator for a given {@link SqlKind}, and turns it into a
     * serializable function
     *
     * @param kind {@link SqlKind} from {@link RexCall} SqlOperator
     * @return a serializable function of +, -, * or /
     * @throws UnsupportedOperationException on unrecognized {@link SqlKind}
     */
    public SerializableFunction<List<Object>, Object> deriveOperation(SqlKind kind);
}

class Call implements Node {
    private final List<Node> operands;
    final List<SqlKind> operandTypes;
    final SerializableFunction<List<Object>, Object> operation;
    final SqlKind kind;

    protected Call(final RexCall call, final CallTreeFactory tree) {
        operands = call.getOperands().stream().map(tree::fromRexNode).collect(Collectors.toList());
        operandTypes = call.getOperands().stream().map(RexNode::getKind).collect(Collectors.toList());
        kind = call.getKind();

        operation = tree.deriveOperation(kind);
    }

    @Override
    public Object evaluate(final Record rec) {
        return operation.apply(
                operands.stream()
                        .map(op -> op.evaluate(rec))
                        .collect(Collectors.toList()));
    }

    @Override
    public String createSqlString(final List<String> fieldNames) {
        if (operands.size() == 1) {
            if (kind == SqlKind.IS_NULL) {
                return operands.get(0).createSqlString(fieldNames) + " IS NULL";
            } else {
                return kind.sql + "(" + operands.get(0).createSqlString(fieldNames) + ")";
            }
        } else if (operands.size() == 2) {
            switch (kind) {
                case TIMES:
                    return operands.get(0).createSqlString(fieldNames) + " * "
                            + operands.get(1).createSqlString(fieldNames);
                case SEARCH:
                    final String columnSql = operands.get(0).createSqlString(fieldNames);

                    final Object value = ((Literal) operands.get(1)).value;
                    if (value instanceof ImmutableRangeSet) {
                        final ImmutableRangeSet<?> rangeSet = (ImmutableRangeSet<?>) value;

                        final Range<?> span = rangeSet.span();
                        final Object lower = span.lowerEndpoint();
                        final Object upper = span.upperEndpoint();

                        return columnSql + " BETWEEN " + sqlObjToString(lower) + " AND " + sqlObjToString(upper);
                    } else if (value instanceof ImmutableSortedSet) {
                        final ImmutableSortedSet<Range> ranges = (ImmutableSortedSet<Range>) value;
                        final String points = ranges.stream().map(
                                span -> sqlObjToString("'" + sqlObjToString(span.lowerEndpoint())) + "','"
                                        + sqlObjToString(span.upperEndpoint()) + "'")
                                .collect(Collectors.joining(","));
                        final String clause = columnSql + " IN (" + points + ")";
                        System.out.println("span points clasue sorted set: " + clause);
                        return clause;
                    } else {
                        throw new UnsupportedOperationException("type not supported: " + value.getClass());
                    }
                default:
                    return operands.get(0).createSqlString(fieldNames) + " " + kind.sql + " "
                            + operands.get(1).createSqlString(fieldNames);
            }

        } else if (operands.size() > 2) {
            return operands.stream().map(op -> op.createSqlString(fieldNames))
                    .collect(Collectors.joining(" " + kind.sql + " "));
        }

        return kind.sql;
    }

    private static String sqlObjToString(final Object obj) {
        if (obj instanceof NlsString)
            return ((NlsString) obj).getValue();
        return obj.toString();
    }
}

class Literal implements Node {
    final Serializable value;

    Literal(final RexLiteral literal) {
        System.out.println("RexLiteral type:" + literal.getTypeName());
        System.out.println("RexLiteral: " + literal);
        switch (literal.getTypeName()) {
            case DATE:
                value = literal.getValueAs(Calendar.class);
                break;
            case INTEGER:
                value = literal.getValueAs(Double.class);
                break;
            case INTERVAL_DAY:
                value = literal.getValueAs(BigDecimal.class).doubleValue();
                break;
            case DECIMAL:
                value = literal.getValueAs(BigDecimal.class).doubleValue();
                break;
            case CHAR:
                value = literal.getValueAs(String.class);
                break;
            case SARG:
                final Sarg<?> sarg = literal.getValueAs(Sarg.class);
                assert sarg.rangeSet instanceof Serializable : "Sarg RangeSet was not serializable.";
                if (sarg.isPoints()) {
                    // point based ranged like 'IN ('x', 'y', 'z')''
                    value = (Serializable) sarg.rangeSet.asRanges();
                } else {
                    // range based queries like 'BETWEEN x AND y'
                    value = (ImmutableRangeSet<?>) sarg.rangeSet;
                }
                System.out.println("sarg literal: " + sarg);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Literal conversion to Java not implemented, type: " + literal.getTypeName());
        }

    }

    @Override
    public Object evaluate(final Record rec) {
        return value;
    }

    @Override
    public String createSqlString(final List<String> fieldNames) {
        if (value instanceof String) {
            return "\'" + ((String) value) + "\'";
        } else {
            return value.toString();
        }
    }
}

class InputRef implements Node {
    private final int key;

    InputRef(final RexInputRef inputRef) {
        this.key = inputRef.getIndex();
    }

    @Override
    public Object evaluate(final Record rec) {
        return rec.getField(key);
    }

    @Override
    public String createSqlString(final List<String> fieldNames) {
        System.out.println("field names using key " + key + ": " + fieldNames);
        System.out.println("got key " + key + ": " + fieldNames.get(key));
        return fieldNames.get(key);
    }
}
