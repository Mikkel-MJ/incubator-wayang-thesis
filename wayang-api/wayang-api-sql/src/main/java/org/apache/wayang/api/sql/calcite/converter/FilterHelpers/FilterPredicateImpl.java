package org.apache.wayang.api.sql.calcite.converter.FilterHelpers;

import org.apache.calcite.rex.RexNode;
import org.apache.wayang.api.sql.calcite.converter.CalciteSerialization.CalciteRexSerializable;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class FilterPredicateImpl extends CalciteRexSerializable implements FunctionDescriptor.SerializablePredicate<Record> {        
        public FilterPredicateImpl(final RexNode condition) {
            super(condition);
        }
    
        @Override
        public boolean test(final Record record) {
        RexNode condition = super.serializables[0]; //

        return condition.accept(new EvaluateFilterCondition(true, record));
    }
}