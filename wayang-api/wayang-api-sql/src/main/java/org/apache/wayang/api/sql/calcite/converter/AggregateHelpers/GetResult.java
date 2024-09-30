package org.apache.wayang.api.sql.calcite.converter.AggregateHelpers;

import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class GetResult implements FunctionDescriptor.SerializableFunction<Record, Record> {
    private final List<AggregateCall> aggregateCallList;
    private Set<Integer> groupingfields;

    public GetResult(List<AggregateCall> aggregateCalls, Set<Integer> groupingfields) {
        this.aggregateCallList = aggregateCalls;
        this.groupingfields = groupingfields;
    }

    @Override
    public Record apply(final Record record) {
        int l = record.size();
        int outputRecordSize = aggregateCallList.size() + groupingfields.size();
        Object[] resValues = new Object[outputRecordSize];

        int i = 0;
        int j = 0;
        for (i = 0; j < groupingfields.size(); i++) {
            if (groupingfields.contains(i)) {
                resValues[j] = record.getField(i);
                j++;
            }
        }

        i = l - aggregateCallList.size() - 1;
        for (AggregateCall aggregateCall : aggregateCallList) {
            String name = aggregateCall.getAggregation().getName();
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