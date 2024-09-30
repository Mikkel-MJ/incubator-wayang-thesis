package org.apache.wayang.api.sql.calcite.converter.AggregateHelpers;

import java.util.List;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class AddAggCols implements FunctionDescriptor.SerializableFunction<Record, Record> {
    private final List<AggregateCall> aggregateCalls;

    public AddAggCols(List<AggregateCall> aggregateCalls) {
        this.aggregateCalls = aggregateCalls;
    }

    @Override
    public Record apply(final Record record) {
        int l = record.size();
        int newRecordSize = l + aggregateCalls.size() + 1;
        Object[] resValues = new Object[newRecordSize];
        int i;
        for (i = 0; i < l; i++) {
            resValues[i] = record.getField(i);
        }
        for (AggregateCall aggregateCall : aggregateCalls) {
            String name = aggregateCall.getAggregation().getName();
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