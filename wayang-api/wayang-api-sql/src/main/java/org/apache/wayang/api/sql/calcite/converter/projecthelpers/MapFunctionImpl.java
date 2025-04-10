package org.apache.wayang.api.sql.calcite.converter.projecthelpers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import org.apache.wayang.api.sql.calcite.converter.WayangProjectVisitor;
import org.apache.wayang.api.sql.calcite.converter.calciteserialisation.CalciteRexSerializable;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class MapFunctionImpl extends CalciteRexSerializable implements FunctionDescriptor.SerializableFunction<Record, Record> {
    public MapFunctionImpl(final List<RexNode> projects) {
        super(projects.toArray(RexNode[]::new));
    }

    @Override
    public Record apply(final Record record) {
        final List<RexNode> projects       = Arrays.asList(super.serializables);
        final List<Object> projectedRecord = new ArrayList<>();

        for (int i = 0; i < projects.size(); i++){
            final RexNode exp = projects.get(i);
            if (exp instanceof RexInputRef) {
                projectedRecord.add(record.getField(((RexInputRef) exp).getIndex()));
            } else if (exp instanceof RexLiteral) {
                final RexLiteral literal = (RexLiteral) exp;
                projectedRecord.add(literal.getValue());
            } else if (exp instanceof RexCall) {
                projectedRecord.add(WayangProjectVisitor.evaluateRexCall(record, (RexCall) exp));
            }
        }
        return new Record(projectedRecord.toArray(new Object[0]));
    }
}