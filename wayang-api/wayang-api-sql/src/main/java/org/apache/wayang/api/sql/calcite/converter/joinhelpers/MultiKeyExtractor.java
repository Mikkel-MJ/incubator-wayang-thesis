package org.apache.wayang.api.sql.calcite.converter.joinhelpers;

import java.util.Arrays;
import java.util.function.Function;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class MultiKeyExtractor implements FunctionDescriptor.SerializableFunction<Record, Record> {
        private final Integer[] indexes;

    /**
     * Extracts a key for a joinOperator.
     * is a subtype of {@link Function}, {@link Serializable} (as required by engines which use serialisation i.e. flink/spark)
     * Takes an input {@link Record} & {@link Integer} key and maps it to a generic field object T.
     * Performs an unchecked cast when applied.
     * @param index key
     */
    public MultiKeyExtractor(final Integer... indexes) {
        this.indexes = indexes;
        System.out.println("indexes");
    }

    public Record apply(final Record record) {
        System.out.println("using indexes: " + Arrays.toString(indexes) + ", getting rec: ");
        System.out.println("rec: " + record);
        System.out.println("returning : " + Arrays.toString(Arrays.stream(indexes).map(record::getField).toArray()));
        return new Record(Arrays.stream(indexes).map(record::getField).toArray());
    }
}
