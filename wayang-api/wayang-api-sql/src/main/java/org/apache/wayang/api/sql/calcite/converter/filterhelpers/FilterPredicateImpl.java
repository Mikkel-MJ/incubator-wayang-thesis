package org.apache.wayang.api.sql.calcite.converter.filterhelpers;

import org.apache.calcite.rex.RexNode;

import org.apache.wayang.api.sql.calcite.converter.calciteserialisation.CalciteRexSerializable;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;
import java.util.Arrays;

public class FilterPredicateImpl extends CalciteRexSerializable
        implements FunctionDescriptor.SerializablePredicate<Record> {

    public FilterPredicateImpl(final RexNode condition) {
        super(condition);
    }

    @Override
    public boolean test(final Record record) {
        final RexNode condition = super.serializables[0];

        if (condition == null) {
            return false;
        }

        return condition.accept(new EvaluateFilterCondition(true, record));
    }
}
