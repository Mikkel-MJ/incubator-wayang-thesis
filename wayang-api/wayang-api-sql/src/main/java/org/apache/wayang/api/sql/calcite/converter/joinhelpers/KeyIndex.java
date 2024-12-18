package org.apache.wayang.api.sql.calcite.converter.joinhelpers;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;

import org.apache.wayang.api.sql.calcite.converter.WayangJoinVisitor.Child;

/**
 * Extracts key index from the call
 */
public class KeyIndex extends RexVisitorImpl<Integer> {
    final Child child;

    public KeyIndex(final boolean deep, final Child child) {
        super(deep);
        this.child = child;
    }

    @Override
    public Integer visitCall(final RexCall call) {
        final RexNode operand = call.getOperands().get(child.ordinal());

        if (!(operand instanceof RexInputRef)) {
            throw new UnsupportedOperationException("Unsupported operation");
        }

        final RexInputRef rexInputRef = (RexInputRef) operand;
        return rexInputRef.getIndex();
    }
}