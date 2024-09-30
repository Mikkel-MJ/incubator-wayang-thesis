package org.apache.wayang.api.sql.calcite.converter.FilterHelpers;

import org.apache.calcite.rex.RexNode;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class FilterPredicateImpl implements FunctionDescriptor.SerializablePredicate<Record> {

    private transient final RexNode condition;

    public FilterPredicateImpl(final RexNode condition) {
        this.condition = condition;
    }

    @Override
    public boolean test(final Record record) {
        return condition.accept(new EvaluateFilterCondition(true, record));
    }
}