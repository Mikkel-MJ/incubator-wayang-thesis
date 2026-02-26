package org.apache.wayang.api.sql.calcite.converter.filterhelpers;

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
import org.apache.calcite.util.Sarg;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

import com.google.common.collect.ImmutableRangeSet;

/**
 * AST of the {@link RexCall} arithmetic, composed into serializable nodes;
 * {@link Call}, {@link InputRef}, {@link Literal}
 */
interface CallTreeFactory extends Serializable {
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

interface Node extends Serializable {
    public Object evaluate(final Record rec);

    public String createSqlString(final List<String> fieldNames);
}

class Call implements Node {
    private final List<Node> operands;
    final List<SqlKind> operandTypes;
    final SerializableFunction<List<Object>, Object> operation;
    final SqlKind kind;

    protected Call(final RexCall call, final CallTreeFactory tree) {
        operands = call.getOperands().stream().map(tree::fromRexNode).collect(Collectors.toList());
        operandTypes = call.getOperands().stream().map(op -> op.getKind()).collect(Collectors.toList());
        kind = call.getKind();
        System.out.println("call: " + kind);
        System.out.println("operandTypes: " + operandTypes
        );

        System.out.println(call.getOperands().stream().map(op -> op.getType().getSqlTypeName().getName()).collect(Collectors.toList()));
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
            return kind.sql + "(" + operands.get(0).createSqlString(fieldNames) + ")";
        } else if (operands.size() == 2) {
            return operands.get(0).createSqlString(fieldNames) + " " + kind.sql + " "
                    + operands.get(1).createSqlString(fieldNames);
        }
        return kind.sql;
    }
}

class Literal implements Node {
    final Serializable value;

    Literal(final RexLiteral literal) {
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
                value = (ImmutableRangeSet<?>) sarg.rangeSet;
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
        return value instanceof String ? "\'" + ((String) value) + "\'" : value.toString();
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
        return fieldNames.get(key);
    }
}
