package org.apache.wayang.api.sql.calcite.converter.calltrees;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

public final class Call implements Node {
    private final List<Node> operands;
    final List<SqlKind> operandTypes;
    final SerializableFunction<List<Object>, Object> operation;
    final SqlKind kind;

    public Call(final RexCall call, final CallTreeFactory tree) {
        this.operands = call.getOperands().stream().map(tree::fromRexNode).collect(Collectors.toList());
        this.operandTypes = call.getOperands().stream().map(RexNode::getKind).collect(Collectors.toList());
        this.kind = call.getKind();
        operation = tree.deriveOperation(call.getKind(), call.getType().getSqlTypeName());
    }

    @Override
    public Object evaluate(final Record rec) {
        return this.operation.apply(
                operands.stream()
                        .map(op -> op.evaluate(rec))
                        .collect(Collectors.toList()));
    }

    @Override
    public String createSqlString(final List<String> fieldNames) {
        if (this.operands.size() == 1) {
            if (this.kind == SqlKind.IS_NULL) {
                return this.operands.get(0).createSqlString(fieldNames) + " IS NULL";
            } else if (this.kind == SqlKind.CAST) {
                return this.operands.get(0).createSqlString(fieldNames);
            } else {
                return this.kind.sql + "(" + operands.get(0).createSqlString(fieldNames) + ")";
            }
        } else if (this.operands.size() == 2) {
            switch (this.kind) {
                case TIMES:
                    return this.operands.get(0).createSqlString(fieldNames) + " * "
                            + this.operands.get(1).createSqlString(fieldNames);
                case SEARCH:
                    final String columnSql = this.operands.get(0).createSqlString(fieldNames);
                    final String sqlString = this.operands.get(1).createSqlString(fieldNames);

                    return columnSql + " BETWEEN " + sqlString;
                default:
                    return operands.get(0).createSqlString(fieldNames) + " " + this.kind.sql + " "
                            + operands.get(1).createSqlString(fieldNames);
            }

        } else if (operands.size() > 2) {
            return operands.stream().map(op -> op.createSqlString(fieldNames))
                    .collect(Collectors.joining(" " + this.kind.sql + " "));
        }

        return kind.sql;
    }
}
