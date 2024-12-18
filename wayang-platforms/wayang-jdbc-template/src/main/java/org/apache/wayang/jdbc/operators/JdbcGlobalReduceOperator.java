package org.apache.wayang.jdbc.operators;

import java.sql.Connection;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;

public abstract class JdbcGlobalReduceOperator<Type> extends GlobalReduceOperator<Type>
        implements JdbcExecutionOperator {

    public JdbcGlobalReduceOperator(GlobalReduceOperator<Type> globalReduceOperator) {
        super(globalReduceOperator);
    }

    public JdbcGlobalReduceOperator(final DataSetType<Type> type,
            final ReduceDescriptor<Type> reduceDescriptor) {
        super(reduceDescriptor, type);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.jdbc.globalreduce.load";
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return compiler.compile(reduceDescriptor);
    }
}
