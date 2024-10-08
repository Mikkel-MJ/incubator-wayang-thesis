package org.apache.wayang.api.sql.calcite.converter.AggregateHelpers;

import java.util.List;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.wayang.api.sql.calcite.converter.CalciteSerialization.CalciteSerializable;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class AddAggCols extends CalciteSerializable implements FunctionDescriptor.SerializableFunction<Record, Record> {
    private final transient List<AggregateCall> aggregateCalls;

    public AddAggCols(final List<AggregateCall> aggregateCalls) {
        super(aggregateCalls.toArray(AggregateCall[]::new));
        this.aggregateCalls = aggregateCalls;
    }

    @Override
    public Record apply(final Record record) {
        final int l = record.size();
        final int newRecordSize = l + aggregateCalls.size() + 1;
        final Object[] resValues = new Object[newRecordSize];
        int i;
        for (i = 0; i < l; i++) {
            resValues[i] = record.getField(i);
        }
        for (final AggregateCall aggregateCall : aggregateCalls) {
            final String name = aggregateCall.getAggregation().getName();
            if (name.equals("COUNT")) {
                resValues[i] = 1;
            } else {
                resValues[i] = record.getField(aggregateCall.getArgList().get(0));
            }
            i++;
        }
        resValues[newRecordSize - 1] = 1;
        return new Record(resValues);
    }
}