package org.apache.wayang.api.sql.calcite.converter.utils;

import java.io.Serializable;
import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

/**
 * AST of the {@link RexCall} arithmetic, composed into serializable nodes;
 * {@link Call}, {@link InputRef}, {@link Literal}
 */
public interface CallTreeFactory extends Serializable {
    public default Node fromRexNode(final RexNode node) {
        if (node instanceof RexCall) {
            final RexCall call = (RexCall) node;
            return new Call(call, this);
        } else if (node instanceof RexInputRef) {
            final RexInputRef inputRef = (RexInputRef) node;
            return new InputRef(inputRef);
        } else if (node instanceof RexLiteral) {
            final RexLiteral literal = (RexLiteral) node;
            return new Literal(literal);
        } else {
            throw new UnsupportedOperationException("Unsupported RexNode in filter condition: " + node);
        }
    }

    /**
     * Derives the java operator for a given {@link SqlKind}, and turns it into a
     * serializable function
     *
     * @param kind {@link SqlKind} from {@link RexCall} SqlOperator
     * @return a serializable function of +, -, * or /
     * @throws UnsupportedOperationException on unrecognized {@link SqlKind}
     */
    public SerializableFunction<List<Object>, Object> deriveOperation(SqlKind kind);
}