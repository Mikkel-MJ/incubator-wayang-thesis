package org.apache.wayang.api.sql.calcite.converter.filterhelpers;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Sarg;

public class FunctionExtractor extends RexVisitorImpl<String> {
    final List<Integer> columnIndexes;
    final String[] specifiedColumnNames;

    /**
     * This visitor, visits the various subnodes of a filter function,
     * and collects the filter functions, literals and columns in a
     * sql-like string representation.
     * @param isDeep
     */
    public FunctionExtractor(final Boolean isDeep, final List<Integer> columnIndexes, final String[] specifiedColumnNames) {
        super(isDeep);
        this.columnIndexes = columnIndexes;
        this.specifiedColumnNames = specifiedColumnNames;
    }

    @Override
    public String visitInputRef(final RexInputRef inputRef) {
        // map rexInputRef to its column name
        final int listIndex = columnIndexes.indexOf(inputRef.getIndex());

        final String fieldName = specifiedColumnNames[listIndex];

        return fieldName;
    }

    @Override
    public String visitLiteral(final RexLiteral literal) {
        return "'" + literal.getValue2() + "'";
    }

    @Override
    public String visitCall(final RexCall call) {
        if (call.getKind().equals(SqlKind.SEARCH)) {

        }

        final List<String> subResults = new ArrayList<>();

        for (final RexNode operand : call.operands) {
            subResults.add(operand.accept(new FunctionExtractor(true, columnIndexes, specifiedColumnNames)));
        }

        if (subResults.size() == 1) {
            // if the rexCall is (IS *)
            if(call.getOperator().getName().contains("IS")) return subResults.get(0) + " " + call.getOperator().getName();
            // if the rexCall (NOT) has just one child like in the case of LIKE with a negation
            // i.e. NOT LIKE
            // then we need to prepend the operator name
            return " " + call.getOperator().getName() + " (" + subResults.get(0) + ")" ;
        }

        if (call.getOperator().kind == SqlKind.OR) {
            return "(" + subResults.stream().collect(Collectors.joining(" " + call.getOperator().getName() + " ")) + ")";
        }

        // join the operands with the operator inbetween
        return subResults.stream().collect(Collectors.joining(" " + call.getOperator().getName() + " "));
    }
}
