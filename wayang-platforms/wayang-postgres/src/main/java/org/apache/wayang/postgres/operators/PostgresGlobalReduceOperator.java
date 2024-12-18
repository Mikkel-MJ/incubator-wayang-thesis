package org.apache.wayang.postgres.operators;

import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.jdbc.operators.JdbcGlobalReduceOperator;

public class PostgresGlobalReduceOperator<Type> extends JdbcGlobalReduceOperator<Type> implements PostgresExecutionOperator {
    public PostgresGlobalReduceOperator(final DataSetType<Type> type,
    final ReduceDescriptor<Type> reduceDescriptor) {
        super(type, reduceDescriptor);
    }

    public PostgresGlobalReduceOperator(GlobalReduceOperator<Type> operator) {
        super(operator);
    }
}
