package org.apache.wayang.api.sql.calcite.converter.joinhelpers;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.api.sql.calcite.rel.WayangTableScan;

public class JoiningTableExtractor extends RelVisitor {
    String name;

    public String getName(){
        return name;
    }

    /**
     * A visitor that extracts the joining table name in a join clause:
     * {@code JOIN joiningTable ON joiningTable.a = previousTable.b}.
     * May return an empty string or null
     */
    public JoiningTableExtractor(boolean deep){

    }

    @Override
    public void visit(final RelNode node, final int ordinal, final RelNode parent) {
        System.out.println("visiting: " + node);
        if(node instanceof WayangJoin) return;
        if(node instanceof WayangTableScan) {
            name = node.getTable().getQualifiedName().get(1);
            System.out.println("setting name: " + name);
            return;
        }

        if(node.getInputs().size() > 0) {
            node.getInputs().stream()
                .forEach(childNode -> this.visit(childNode, childNode.getId(), node));
        }
    }
}
