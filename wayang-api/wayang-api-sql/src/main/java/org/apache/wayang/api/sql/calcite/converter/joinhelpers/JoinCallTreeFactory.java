package org.apache.wayang.api.sql.calcite.converter.joinhelpers;

import java.util.List;

import org.apache.calcite.sql.SqlKind;
import org.apache.wayang.api.sql.calcite.converter.calltrees.CallTreeFactory;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

public class JoinCallTreeFactory implements CallTreeFactory {

    @Override
    public SerializableFunction<List<Object>, Object> deriveOperation(SqlKind kind) {
        System.out.println(kind);

        switch(kind) {
            case EQUALS:
                return op -> op.get(0) + " = " + op.get(1);
            case AND:
                return op -> "(" + op.get(0) + ") AND (" + op.get(1) + ")";
            default:
                throw new UnsupportedOperationException("Not implemented: " + kind);
        }
    }
    
}
