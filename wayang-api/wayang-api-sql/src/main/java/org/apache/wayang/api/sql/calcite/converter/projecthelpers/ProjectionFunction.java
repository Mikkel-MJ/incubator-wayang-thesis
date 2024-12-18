package org.apache.wayang.api.sql.calcite.converter.projecthelpers;

import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.basic.data.Record;

public class ProjectionFunction <T extends SerializableFunction<Record, Object>> extends ProjectionDescriptor<Record, Object> implements SerializableFunction<Record, Object> {
    final T impl;

    public ProjectionFunction(T impl, String... fieldNames) {
        super(Record.class, Object.class, fieldNames);
        this.impl = impl;
    }


    @Override
    public Object apply(Record arg0) {
        return impl.apply(arg0);
    }

}
