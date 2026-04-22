package org.apache.wayang.api.sql.calcite.converter.aggregatehelpers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class KeyExtractor implements FunctionDescriptor.SerializableFunction<Record, Object> {

    private final List<Integer> groupFields; // sorted list of input indices

    public KeyExtractor(final Collection<Integer> groupFields) {
        // Ensure stable iteration order
        this.groupFields = groupFields.stream().sorted().collect(Collectors.toList());
    }

    @Override
    public Object apply(final Record r) {

        final int accumulatorFieldCount = groupFields.size() + /* number of aggs */ getAggCount();

        // Detect accumulator:
        final boolean isAccumulator = (r.getFields().length == accumulatorFieldCount);

        final List<Object> keys = new ArrayList<>();

        if (!isAccumulator) {
            // RAW ROW → extract group keys from input schema positions
            for (final int inputIndex : groupFields) {
                keys.add(r.getField(inputIndex));
            }
        } else {
            // ACCUMULATOR → group keys are in fixed positions 0..groupCount-1
            for (int pos = 0; pos < groupFields.size(); pos++) {
                keys.add(r.getField(pos));
            }
        }

        return keys;
    }

    private int getAggCount() {
        // You must pass agg count into this extractor.
        throw new UnsupportedOperationException("Add aggCount to KeyExtractor constructor");
    }
}
