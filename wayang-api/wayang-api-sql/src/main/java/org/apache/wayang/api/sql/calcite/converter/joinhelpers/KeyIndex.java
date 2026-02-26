package org.apache.wayang.api.sql.calcite.converter.joinhelpers;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;


/**
 * Extracts key index from the call
 */
public class KeyIndex extends RexVisitorImpl<Integer> {

    public KeyIndex(final boolean deep) {
        super(deep);
    }

    @Override
    public Integer visitCall(final RexCall call) {
        return 1;
    }
}