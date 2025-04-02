package org.apache.wayang.postgres.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.SortOperator;
import org.apache.wayang.jdbc.operators.JdbcSortOperator;

public class PostgresSortOperator extends JdbcSortOperator implements PostgresExecutionOperator {

    public PostgresSortOperator(SortOperator<Record, Record> that) {
        super(that);
    }
    
}
