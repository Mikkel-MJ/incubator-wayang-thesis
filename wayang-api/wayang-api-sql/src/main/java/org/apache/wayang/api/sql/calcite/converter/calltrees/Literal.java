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
                if (sarg.isPoints()) {
                    // point based ranged like 'IN ('x', 'y', 'z')''
                    value = (Serializable) sarg.rangeSet.asRanges();
                } else {
                    // range based queries like 'BETWEEN x AND y'
                    value = (ImmutableRangeSet<?>) sarg.rangeSet;
                }
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
