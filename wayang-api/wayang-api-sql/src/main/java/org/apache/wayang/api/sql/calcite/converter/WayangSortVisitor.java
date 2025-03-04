package org.apache.wayang.api.sql.calcite.converter;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.api.sql.calcite.rel.WayangSort;
import org.apache.wayang.api.sql.calcite.utils.AliasFinder;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;

public class WayangSortVisitor extends WayangRelNodeVisitor<WayangSort> {

    WayangSortVisitor(WayangRelConverter wayangRelConverter, AliasFinder aliasFinder) {
        super(wayangRelConverter, aliasFinder);
    }

    @Override
    Operator visit(WayangSort wayangRelNode) {
        assert(wayangRelNode.getInputs().size() == 1) : "Sorts must only have one input, but found: " + wayangRelNode.getInputs().size();
        final Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(), aliasFinder);
        
        final List<RexNode> sorts = wayangRelNode.getSortExps();
        final RexNode fetch = wayangRelNode.fetch;
        final RexNode offset = wayangRelNode.offset;
        
        System.out.println("sorts: " + sorts);
        System.out.println("fetch: " + fetch);
        System.out.println("offset: " + offset);

        final TransformationDescriptor<?,?> td = new TransformationDescriptor<>(null, Record.class, Object.class);
        
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'visit'");
    }
    
}
