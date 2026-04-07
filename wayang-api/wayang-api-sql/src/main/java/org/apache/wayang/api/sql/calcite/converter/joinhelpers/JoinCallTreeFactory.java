package org.apache.wayang.api.sql.calcite.converter.joinhelpers;

import java.util.List;
import java.util.Objects;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.wayang.api.sql.calcite.converter.calltrees.CallTreeFactory;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

public class JoinCallTreeFactory implements CallTreeFactory {
    @Override
    public SerializableFunction<List<Object>, Object> deriveOperation(final SqlKind kind, final SqlTypeName returnType) {
        switch(kind) {
            case EQUALS:
                return op -> Objects.equals(op.get(0), op.get(1));
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
