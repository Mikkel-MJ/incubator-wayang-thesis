package org.apache.wayang.api.sql.calcite.converter.JoinHelpers;

import java.util.function.Function;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class KeyExtractor<T> implements FunctionDescriptor.SerializableFunction<Record, T> {
    private final int index;

    /**
     * Extracts a key for a joinOperator.
     * is a subtype of {@link Function}, {@link Serializable} (as required by engines which use serialisation i.e. flink/spark)
     * Takes an input {@link Record} & {@link Integer} key and maps it to a generic field object T.
     * Performs an unchecked cast when applied.
     * @param index key
     */
    public KeyExtractor(final int index) {
        this.index = index;
    }

    public T apply(final Record record) {
        return (T) record.getField(index);
    }
}