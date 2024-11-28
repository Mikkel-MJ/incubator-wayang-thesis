package org.apache.wayang.api.sql.calcite.converter.filterhelpers;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

import scala.collection.mutable.StringBuilder;

public class FunctionExtractor extends RexVisitorImpl<String> {
    final List<Integer> columnIndexes;
    final String[] specifiedColumnNames;

    /**
     * 
     * @param isDeep
     */
    public FunctionExtractor(final Boolean isDeep, final List<Integer> columnIndexes,
            final String[] specifiedColumnNames) {
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
        System.out.println("call: " + call);
        System.out.println("call get sqlkind: " + call.getOperator().getKind().sql);
        System.out.println("call get op: " + call.getOperator().getName());
        System.out.println("call operands: " + call.operands);

        final List<String> subResults = new ArrayList<>();

        for (final RexNode operand : call.operands) {
            subResults.add(operand.accept(new FunctionExtractor(true, columnIndexes, specifiedColumnNames)));
        }

        System.out.println("subresults: " + subResults);

        // if the rexCall has just one child like in the case of LIKE with a negation
        // i.e. NOT LIKE
        // then we need to prepend the operator name
        if (subResults.size() == 1) {
            return " " + call.getOperator().getName() + " (" + subResults.get(0) + ")" ;
        }

        if (call.getOperator().kind == SqlKind.OR) {
            return "(" + subResults.stream().collect(Collectors.joining(" " + call.getOperator().getName() + " ")) + ")";
        }

        // join the operands with the operator inbetween 
        return subResults.stream().collect(Collectors.joining(" " + call.getOperator().getName() + " "));
    }
}