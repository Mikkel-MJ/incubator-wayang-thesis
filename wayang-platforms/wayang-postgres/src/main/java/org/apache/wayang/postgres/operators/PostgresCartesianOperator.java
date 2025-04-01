package org.apache.wayang.postgres.operators;

import org.apache.wayang.basic.operators.CartesianOperator;
import org.apache.wayang.jdbc.operators.JdbcCartesianOperator;
import org.apache.wayang.basic.data.Record;

public class PostgresCartesianOperator extends JdbcCartesianOperator {

    public PostgresCartesianOperator(CartesianOperator<Record, Record> that) {
        super(that);
    }
}
