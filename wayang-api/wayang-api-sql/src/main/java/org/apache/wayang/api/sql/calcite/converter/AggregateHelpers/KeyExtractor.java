package org.apache.wayang.api.sql.calcite.converter.AggregateHelpers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class KeyExtractor implements FunctionDescriptor.SerializableFunction<Record, Object> {
    private final HashSet<Integer> indexSet;

    public KeyExtractor(final Set<Integer> indexSet) {
        this.indexSet = new HashSet<>(indexSet); //force serialisable
    }

    public Object apply(final Record record) {
        final List<Object> keys = new ArrayList<>();
        for (final Integer index : indexSet) {
            keys.add(record.getField(index));
        }
        return keys;
    }
}