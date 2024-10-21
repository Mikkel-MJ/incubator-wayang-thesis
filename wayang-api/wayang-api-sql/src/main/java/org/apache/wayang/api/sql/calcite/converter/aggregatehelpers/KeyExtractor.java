package org.apache.wayang.api.sql.calcite.converter.aggregatehelpers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class KeyExtractor implements FunctionDescriptor.SerializableFunction<Record, Object> {
    private final HashSet<Integer> indexSet;

    public KeyExtractor(final HashSet<Integer> indexSet) {
        this.indexSet = indexSet; //force serialisable
    }

    public Object apply(final Record record) {
        final List<Object> keys = new ArrayList<>();
        for (final Integer index : indexSet) {
            keys.add(record.getField(index));
        }
        return keys;
    }
}