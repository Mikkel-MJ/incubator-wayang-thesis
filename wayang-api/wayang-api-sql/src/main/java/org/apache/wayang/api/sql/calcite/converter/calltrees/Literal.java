package org.apache.wayang.api.sql.calcite.converter.calltrees;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimestampString;

import org.apache.wayang.basic.data.Record;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

public final class Literal implements Node {
    final Serializable value;
    final String sqlString;

    public Literal(final RexLiteral literal) {
        switch (literal.getTypeName()) {
            case DATE:
                value = literal.getValueAs(Calendar.class);
                throw new UnsupportedOperationException("literal: " + literal);
            case INTEGER:
                value = literal.getValueAs(Double.class);
                sqlString = literal.getValueAs(Integer.class).toString();
                break;
            case INTERVAL_DAY:
                value = literal.getValueAs(BigDecimal.class).doubleValue();
                throw new UnsupportedOperationException("literal: " + literal);
            case DECIMAL:
                value = literal.getValueAs(BigDecimal.class).doubleValue();
                sqlString = value.toString();
                break;
            case CHAR:
                value = literal.getValueAs(String.class);
                sqlString = value.toString();
                break;
            case TIMESTAMP:
                value = literal.getValueAs(Long.class);

                if (literal.getValue() instanceof GregorianCalendar) {
                    sqlString = sqlStringFromCalendar((GregorianCalendar) literal.getValue());
                } else {
                    throw new UnsupportedOperationException("TIMESTAMP type not supported: " + literal.getValue().getClass());
                }
                break;
            case SARG:
                final Sarg<?> sarg = literal.getValueAs(Sarg.class);

                if (sarg.isPoints()) {
                    // point based ranged like 'IN ('x', 'y', 'z')'
                    value = (Serializable) makeSargSerializable(sarg).rangeSet.asRanges();
                    sqlString = sarg.rangeSet.asRanges().stream().map(obj -> obj.toString()).collect(Collectors.joining(","));
                } else {
                    // range based queries like 'BETWEEN x AND y'
                    value = (ImmutableRangeSet<?>) makeSargSerializable(sarg).rangeSet;
                    sqlString = sarg.rangeSet.asRanges().stream().map(obj -> "'" + obj.lowerEndpoint() + "' AND '" + obj.upperEndpoint() + "'").collect(Collectors.joining(","));
                }

                assert value instanceof Serializable;
                break;
            default:
                throw new UnsupportedOperationException(
                        "Literal conversion to Java not implemented, type: " + literal.getTypeName());
        }
    }


    private static String sqlStringFromCalendar(final GregorianCalendar cal) {
        final Instant instant = cal.toInstant();
        final LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        return "TIMESTAMP '" + ldt.toString().replace('T', ' ') + "'";
    }


    public static Sarg<?> makeSargSerializable(final Sarg<?> sarg) {
        final RangeSet<?> original = sarg.rangeSet;
        final ImmutableRangeSet.Builder builder = ImmutableRangeSet.builder();

        for (final Object r : original.asRanges()) {
            final Range<?> range = (Range<?>) r;
            final Comparable<?> lowerBound = convertToSerializable(range.lowerEndpoint());
            final Comparable<?> higherBound = convertToSerializable(range.upperEndpoint());

            assert lowerBound instanceof Serializable;
            assert higherBound instanceof Serializable;

            builder.add(Range.closed(lowerBound, higherBound));
        }
        final ImmutableRangeSet<Comparable> newSet = builder.build();

        return Sarg.of(false, newSet);
    }

    public static Comparable convertToSerializable(final Object v) {
        if (v == null)
            return null;

        if (v instanceof NlsString) {
            return ((NlsString) v).getValue();
        }

        if (v instanceof TimestampString) {
            final long ms = ((TimestampString) v).getMillisSinceEpoch();
            return ms;
        }

        if (v instanceof Number) {
            return ((Number) v).doubleValue();
        }

        if (v instanceof Boolean)
            return (Boolean) v;

        if (v instanceof String) {
            return (Comparable) v;
        }

        throw new UnsupportedOperationException("unsupported type: " + v.getClass());
    }

    @Override
    public Object evaluate(final Record rec) {
        return value;
    }

    @Override
    public String createSqlString(final List<String> fieldNames) {
        return sqlString;
    }
}
