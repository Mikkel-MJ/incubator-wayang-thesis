package org.apache.wayang.postgres.operators;

import org.apache.wayang.basic.operators.CartesianOperator;
import org.apache.wayang.jdbc.operators.JdbcCartesianOperator;
import org.apache.wayang.basic.data.Record;

public class PostgresCartesianOperator extends JdbcCartesianOperator implements PostgresExecutionOperator {

    public PostgresCartesianOperator(CartesianOperator<Record, Record> that) {
        super(that);
    }
}
