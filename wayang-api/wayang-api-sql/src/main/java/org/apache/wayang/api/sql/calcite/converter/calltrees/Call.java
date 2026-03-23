package org.apache.wayang.api.sql.calcite.converter.calltrees;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Range;

public final class Call implements Node {
    private static String sqlObjToString(final Object obj) {
        if (obj instanceof NlsString)
            return ((NlsString) obj).getValue();
        return obj.toString();
    }

    private final List<Node> operands;
    final List<SqlKind> operandTypes;
    final SerializableFunction<List<Object>, Object> operation;
    final SqlKind kind;
    
    public Call(final RexCall call, final CallTreeFactory tree) {
        this.operands = call.getOperands().stream().map(tree::fromRexNode).collect(Collectors.toList());
        this.operandTypes = call.getOperands().stream().map(RexNode::getKind).collect(Collectors.toList());
        this.kind = call.getKind();
        this.operation = tree.deriveOperation(kind);
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
}