package org.apache.wayang.jdbc.operators;

import java.sql.Connection;

import org.apache.wayang.basic.operators.CartesianOperator;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.wayang.basic.data.Record;

public class JdbcCartesianOperator extends CartesianOperator<Record, Record> implements JdbcExecutionOperator {

    public JdbcCartesianOperator(final CartesianOperator<Record, Record> that) {
        super(that);
    }

    @Override
    public String createSqlClause(final Connection connection, final FunctionCompiler compiler) {
        return "CROSS JOIN";
    }

    @Override
    public JdbcPlatformTemplate getPlatform() {
        throw new UnsupportedOperationException("Unimplemented method 'getPlatform'");
    }
}
