
package org.apache.wayang.api.sql.calcite.converter.calltrees;

import java.util.List;

import org.apache.calcite.rex.RexInputRef;
import org.apache.wayang.basic.data.Record;

public final class InputRef implements Node {
    private final int key;

    public InputRef(final RexInputRef inputRef) {
        this.key = inputRef.getIndex();
    }

    @Override
    public Object evaluate(final Record rec) {
        return rec.getField(key);
    }

    @Override
    public String createSqlString(final List<String> fieldNames) {
        return fieldNames.get(key);
    }
}
