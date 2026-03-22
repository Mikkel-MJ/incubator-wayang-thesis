package org.apache.wayang.postgres.operators;

import java.sql.Connection;

import org.apache.wayang.basic.operators.SortOperator;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.jdbc.operators.JdbcSortOperator;
import org.apache.wayang.basic.data.Record;

public class PostgresSortOperator extends JdbcSortOperator implements PostgresExecutionOperator {

    public PostgresSortOperator(final SortOperator<Record, Record> that) {
        super(that);
    }

    @Override
    public String createSqlClause(final Connection connection, final FunctionCompiler compiler) {
        return compiler.compile(this.getKeyDescriptor());
    }

}
