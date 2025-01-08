package org.apache.wayang.api.sql.calcite.converter;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class TableScanVisitor extends RelVisitor {
    public final List<RelNode> tableScans;
    public RelNode catalog;

    /**
     * This visitor visits {@RelNode}s and collects {@link LogicalTableScan}.
     * Use by providing a root {@link RelNode} to
     * {@link #visit(RelNode, int, RelNode)}.
     * 
     * @param catalog
     * @param tableNameToCounterMap
     * @param fieldIndexToTableMap
     */
    public TableScanVisitor(final List<RelNode> tableScans, RelNode catalog) {
        this.tableScans = tableScans;
        this.catalog = catalog;
    }

    @Override
    public void visit(final RelNode node, final int ordinal, final RelNode parent) {
        if (catalog == null || catalog.getRowType().getFieldList().size() < node.getRowType().getFieldList().size()) {
            catalog = node;
        }

        if (node instanceof LogicalTableScan) {
            tableScans.add(node);
        }

        if (node.getInputs().size() > 0) {
            final TableScanVisitor visitor = new TableScanVisitor(tableScans, catalog);

            node.getInputs()
                    .stream()
                    .forEach(childNode -> visitor.visit(childNode, childNode.getId(), node));

            catalog = visitor.catalog;
        }
    }
}
