package org.apache.wayang.api.sql.calcite.converter;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.SortOperator;
import org.apache.wayang.api.sql.calcite.converter.sorthelpers.KeyExtractor;
import org.apache.wayang.api.sql.calcite.rel.WayangSort;
import org.apache.wayang.api.sql.calcite.utils.AliasFinder;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;

public class WayangSortVisitor extends WayangRelNodeVisitor<WayangSort> {

    WayangSortVisitor(final WayangRelConverter wayangRelConverter, final AliasFinder aliasFinder) {
        super(wayangRelConverter, aliasFinder);
    }

    @Override
    Operator visit(final WayangSort wayangRelNode) {
        assert (wayangRelNode.getInputs().size() == 1)
                : "Sorts must only have one input, but found: " + wayangRelNode.getInputs().size();

        final Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(), aliasFinder);

        final RexNode fetch = wayangRelNode.fetch;
        final RexLiteral offset = (RexLiteral) wayangRelNode.offset;
        final RelCollation collation = wayangRelNode.getCollation();

        final List<Direction> collationDirections = collation.getFieldCollations().stream()
                .map(fieldCol -> fieldCol.getDirection())
                .collect(Collectors.toList());

        final List<Integer> collationIndexes = collation.getFieldCollations().stream()
                .map(fieldCol -> fieldCol.getFieldIndex())
                .collect(Collectors.toList());

        final TransformationDescriptor<Record, Record> td = new TransformationDescriptor<Record, Record>(
                new KeyExtractor(
                        collationDirections,
                        collationIndexes,
                        fetch,
                        RexLiteral.intValue(offset)),
                Record.class, Record.class);

        final SortOperator<Record, Record> sort = new SortOperator<Record, Record>(td);

        childOp.connectTo(0, sort, 0);

        return sort;
    }

}
