package org.apache.wayang.api.sql.calcite.converter.joinhelpers;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.function.FunctionDescriptor;

/**
 * Flattens Tuple2<Record, Record> to Record
 */
public class MapFunctionImpl implements FunctionDescriptor.SerializableFunction<Tuple2, Record> {
    public MapFunctionImpl() {

    }

    @Override
    public Record apply(final Tuple2 tuple2) {
        final int length1 = ((Tuple2<Record, Record>) tuple2).getField0().size();
        final int length2 = ((Tuple2<Record, Record>) tuple2).getField1().size();

        final int totalLength = length1 + length2;

        final Object[] fields = new Object[totalLength];

        for (int i = 0; i < length1; i++) {
            fields[i] = ((Tuple2<Record, Record>) tuple2).getField0().getField(i);
        }
        for (int j = length1; j < totalLength; j++) {
            fields[j] = ((Tuple2<Record, Record>) tuple2).getField1().getField(j - length1);
        }
        return new Record(fields);
    }
} 