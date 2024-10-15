package org.apache.wayang.api.sql.calcite.converter.AggregateHelpers;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.wayang.api.sql.calcite.converter.CalciteSerialization.CalciteAggSerializable;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class GetResult extends CalciteAggSerializable implements FunctionDescriptor.SerializableFunction<Record, Record> {
    private final HashSet<Integer> groupingfields;

    public GetResult(final List<AggregateCall> aggregateCalls, final HashSet<Integer> groupingfields) {
        super(aggregateCalls.toArray(AggregateCall[]::new));
        this.groupingfields = groupingfields;
    }

    @Override
    public Record apply(final Record record) {
        final List<AggregateCall> aggregateCalls = Arrays.asList(serializables);

        final int l = record.size();
        final int outputRecordSize = aggregateCalls.size() + groupingfields.size();
        final Object[] resValues = new Object[outputRecordSize];

        int i = 0;
        int j = 0;
        for (i = 0; j < groupingfields.size(); i++) {
            if (groupingfields.contains(i)) {
                resValues[j] = record.getField(i);
                j++;
            }
        }

        i = l - aggregateCalls.size() - 1;
        for (final AggregateCall aggregateCall : aggregateCalls) {
            final String name = aggregateCall.getAggregation().getName();
            if (name.equals("AVG")) {
                resValues[j] = record.getDouble(i) / record.getDouble(l - 1);
            } else {
                resValues[j] = record.getField(i);
            }
            j++;
            i++;
        }

        return new Record(resValues);
    }
}