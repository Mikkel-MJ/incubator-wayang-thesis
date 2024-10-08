package org.apache.wayang.api.sql.calcite.converter.FilterHelpers;

import org.apache.calcite.rex.RexNode;
import org.apache.wayang.api.sql.calcite.converter.CalciteSerialization.CalciteSerializable;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class FilterPredicateImpl extends CalciteSerializable implements FunctionDescriptor.SerializablePredicate<Record> {

    private transient RexNode condition;
        
        public FilterPredicateImpl(final RexNode condition) {
            super(condition);
            this.condition = condition;
            System.out.println("condition " + condition+ " for obj: " + System.identityHashCode(this));
        }
    
        @Override
        public boolean test(final Record record) {
        this.condition = ((RexNode[]) super.serializables)[0]; 
        System.out.println("test record: " + condition + " for obj: " + System.identityHashCode(this));
        System.out.println("test condition: " + condition + " for obj: " + System.identityHashCode(this));
        return condition.accept(new EvaluateFilterCondition(true, record));
    }
}