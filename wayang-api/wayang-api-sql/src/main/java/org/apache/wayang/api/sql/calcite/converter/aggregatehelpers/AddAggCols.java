package org.apache.wayang.api.sql.calcite.converter.aggregatehelpers;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.rel.core.AggregateCall;

import org.apache.wayang.api.sql.calcite.converter.calciteserialisation.CalciteAggSerializable;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class AddAggCols extends CalciteAggSerializable implements FunctionDescriptor.SerializableFunction<Record, Record> {
    public AddAggCols(final List<AggregateCall> aggregateCalls) {
        super(aggregateCalls.toArray(AggregateCall[]::new));
    }

    @Override
    public Record apply(final Record record) {
        final List<AggregateCall> aggregateCalls = Arrays.asList(super.serializables);

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
