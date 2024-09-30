package org.apache.wayang.api.sql.calcite.converter.AggregateHelpers;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class KeyExtractor implements FunctionDescriptor.SerializableFunction<Record, Object> {
    private Set<Integer> indexSet;

    public KeyExtractor(Set<Integer> indexSet) {
        this.indexSet = indexSet;
    }

    public Object apply(final Record record) {
        List<Object> keys = new ArrayList<>();
        for (Integer index : indexSet) {
            keys.add(record.getField(index));
        }
        return keys;
    }
}