package org.apache.wayang.jdbc.operators;

import java.sql.Connection;
import org.apache.wayang.basic.operators.CartesianOperator;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.basic.data.Record;

public abstract class JdbcCartesianOperator extends CartesianOperator<Record, Record> implements JdbcExecutionOperator {

    public JdbcCartesianOperator(final CartesianOperator<Record, Record> that) {
        super(that);
    }

    @Override
    public String createSqlClause(final Connection connection, final FunctionCompiler compiler) {
        throw new UnsupportedOperationException(
                "create sql clause not directly supported since operators may not contain information about children, you will have to implement your own recursion to find tables relevant to sql lingo: CROSS JOIN on \'table\'");
    }
}
