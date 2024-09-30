package org.apache.wayang.api.sql.calcite.converter.JoinHelpers;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

/**
 * Extracts the key
 */
public class KeyExtractor implements FunctionDescriptor.SerializableFunction<Record, Object> {
    private final int index;

    public KeyExtractor(final int index) {
        this.index = index;
    }

    public Object apply(final Record record) {
        return record.getField(index);
    }
}