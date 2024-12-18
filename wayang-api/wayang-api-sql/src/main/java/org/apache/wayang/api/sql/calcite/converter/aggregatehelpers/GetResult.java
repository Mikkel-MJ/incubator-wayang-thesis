package org.apache.wayang.api.sql.calcite.converter.aggregatehelpers;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.calcite.rel.core.AggregateCall;

import org.apache.wayang.api.sql.calcite.converter.calciteserialisation.CalciteAggSerializable;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class GetResult extends CalciteAggSerializable
        implements FunctionDescriptor.SerializableFunction<Record, Record> {
    private final HashSet<Integer> groupingfields;

    public GetResult(final List<AggregateCall> aggregateCalls, final HashSet<Integer> groupingfields) {
        super(aggregateCalls.toArray(AggregateCall[]::new));
        this.groupingfields = groupingfields;
    }

    @Override
    public Record apply(final Record record) {
        final List<AggregateCall> aggregateCalls = Arrays.asList(super.serializables);

        final int l = record.size();
        final int outputRecordSize = aggregateCalls.size() + groupingfields.size();

        final Object[] resValues = new Object[outputRecordSize];

        final Integer[] groupingFieldsArray = groupingfields.toArray(Integer[]::new);
        
        for (int i = 0; i < groupingfields.size(); i++) {
            resValues[i] = record.getField(groupingFieldsArray[i]);
        }

        final int offset = groupingfields.size() > 0 ? 1 : 0;

        int startingIndex = l - aggregateCalls.size() - offset;
        int resValuePos = groupingfields.size();

        for (final AggregateCall aggregateCall : aggregateCalls) {
            final String name = aggregateCall.getAggregation().getName();
            if (name.equals("AVG")) {
                resValues[resValuePos] = record.getDouble(startingIndex) / record.getDouble(l - 1);
            } else {
                resValues[resValuePos] = record.getField(startingIndex);
            }
            resValuePos++;
            startingIndex++;
        }

        return new Record(resValues);
    }
}