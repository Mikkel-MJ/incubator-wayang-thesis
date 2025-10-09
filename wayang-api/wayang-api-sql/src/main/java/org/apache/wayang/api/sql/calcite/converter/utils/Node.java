package org.apache.wayang.api.sql.calcite.converter.utils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;

public interface Node extends Serializable {
    public Object evaluate(final Record rec);

    public String toString(final List<RelDataTypeField> fieldList);
}

final class Call implements Node {
    private final List<Node> operands;
    private final SerializableFunction<List<Object>, Object> operation;
    private final SqlKind kind;

    protected Call(final RexCall call, final CallTreeFactory tree) {
        operands = call.getOperands().stream().map(tree::fromRexNode).collect(Collectors.toList());
        operation = tree.deriveOperation(call.getKind());
        this.kind = call.getKind();
    }

    @Override
    public String toString(final List<RelDataTypeField> fieldList) {
        switch (kind) {
            case NOT:
                return "NOT (" + operands.get(0).toString(fieldList) + ")";
            case SEARCH:
                return operands.get(0).toString(fieldList) + " IN (" + operands.get(1).toString(fieldList) + ")";
            case IS_NULL:
                return operands.get(0).toString(fieldList) + " IS NULL";
            default:
                System.out.println("call with kind: " + kind);
                return operands.get(0).toString(fieldList) + " " + kind.sql + " " +
                        operands.get(1).toString(fieldList);
        }
    }

    public List<Node> getOperands() {
        return operands;
    }

    @Override
    public Object evaluate(final Record rec) {
        return operation.apply(
                operands.stream()
                        .map(op -> op.evaluate(rec))
                        .collect(Collectors.toList()));
    }
}

final class Literal implements Node {
    private final Serializable value;

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
    public String toString(final List<RelDataTypeField> fieldList) {
        if (value instanceof ImmutableRangeSet<?>) {
            return ((ImmutableRangeSet<?>) value).asRanges().stream()
                    .map(Range::lowerEndpoint)
                    .map(obj -> {
                        if (obj instanceof NlsString) {
                            return "'" + ((NlsString) obj).getValue() + "'";
                        } else if (obj instanceof Number) {
                            return obj.toString();
                        }
                        throw new UnsupportedOperationException(
                                "WHERE IN clauses not supported with type: " + obj.getClass());
                    })
                    .collect(Collectors.joining(","));
        }

        return value instanceof Number ? value.toString() : "'" + value.toString() + "'";
    }

    @Override
    public Object evaluate(final Record rec) {
        return value;
    }
}

final class InputRef implements Node {
    private final int key;

    InputRef(final RexInputRef inputRef) {
        System.out.println("inputref: " + this + "rex: " + inputRef);
        this.key = inputRef.getIndex();
    }

    @Override
    public String toString(final List<RelDataTypeField> fieldList) {
        System.out.println("this: " + this);
        System.out.println("fieldlIst: " + fieldList);
        System.out.println("key: " + key);
        return fieldList.get(key).getName();
    }

    @Override
    public Object evaluate(final Record rec) {
        return rec.getField(key);
    }
}
