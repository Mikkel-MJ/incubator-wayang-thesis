package org.apache.wayang.api.sql.calcite.converter.filterhelpers;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;

public class ColumnIndexExtractor extends RexVisitorImpl<List<Integer>> {
    /**
     * Recursively extracts the column indicies used in a filter, i.e. 
     * {@code WHERE($20 = $10)} it will fetch 20 and 10
     * @param isDeep
     */
    public ColumnIndexExtractor(final Boolean isDeep) {
        super(isDeep);
    }

    @Override
    public List<Integer> visitCall(final RexCall call) {
        final List<Integer> indexes = new ArrayList<>();
        for (final RexNode operand : call.operands) {
            if (operand instanceof RexInputRef) {
                indexes.add(((RexInputRef) operand).getIndex());
            } else if (operand instanceof RexCall) {
                final List<Integer> operandIndexes = operand.accept(new ColumnIndexExtractor(true));

                if (operandIndexes.size() > 0)
                    indexes.addAll(operandIndexes);
            }    
        }

        return indexes;
    }
}