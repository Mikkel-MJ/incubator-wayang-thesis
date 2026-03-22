package org.apache.wayang.api.sql.calcite.converter.joinhelpers;

import java.util.List;

import org.apache.calcite.sql.SqlKind;
import org.apache.wayang.api.sql.calcite.converter.calltrees.CallTreeFactory;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

public class JoinCallTreeFactory implements CallTreeFactory {

    @Override
    public SerializableFunction<List<Object>, Object> deriveOperation(SqlKind kind) {
        // TODO: needs to be converted to java functions not strings.
        switch(kind) {
            case EQUALS:
                return op -> new UnsupportedOperationException();
            case AND:
                return op -> new UnsupportedOperationException();
            case TIMES:
                return op ->  new UnsupportedOperationException();
            case GREATER_THAN:
                return op ->  new UnsupportedOperationException();
            default:
                throw new UnsupportedOperationException("Not implemented: " + kind);
        }
    }
    
}
