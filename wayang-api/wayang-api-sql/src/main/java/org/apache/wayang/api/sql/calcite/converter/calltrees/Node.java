package org.apache.wayang.api.sql.calcite.converter.calltrees;

import java.io.Serializable;
import java.util.List;
import org.apache.wayang.basic.data.Record;


public interface Node extends Serializable {
    Object evaluate(final Record rec);

    String createSqlString(final List<String> fieldNames);
}
