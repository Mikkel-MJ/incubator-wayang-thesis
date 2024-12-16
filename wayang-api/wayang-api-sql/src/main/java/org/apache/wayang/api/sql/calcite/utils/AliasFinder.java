package org.apache.wayang.api.sql.calcite.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;

import org.apache.wayang.api.sql.calcite.converter.TableScanVisitor;
import org.apache.wayang.api.sql.calcite.rel.WayangAggregate;
import org.apache.wayang.api.sql.calcite.rel.WayangProject;

public class AliasFinder {
    public final List<RelDataTypeField> catalog;
    public final List<String> columnIndexToTableName;
    public final HashMap<String, Integer> tableOccurenceCounter;
    public final Map<RelDataTypeField, String> columnToTableNameMap;

    public AliasFinder(final TableScanVisitor visitor) {
        final List<RelDataTypeField> catalog = visitor.catalog.getRowType().getFieldList();
        final List<String> columnIndexToTableName = new ArrayList<>(catalog.size());
        final HashMap<String, Integer> tableOccurenceCounter = new HashMap<>();
        final Map<RelDataTypeField, String> columnToTableNameMap = CalciteSources
                .createColumnToTableOriginMap(visitor.catalog);

        for (int i = 0; i < catalog.size(); i++) {
            final String tableName = columnToTableNameMap.get(catalog.get(i));

            if (tableOccurenceCounter.containsKey(tableName)) {
                final int currentCount = tableOccurenceCounter.get(tableName);

                // If the counter size exceeds the number of fields within the table,
                // we need to alias
                final int tableFieldCount = CalciteSources.tableOriginOf(visitor.catalog, i).getRowType()
                        .getFieldCount();

                if (currentCount >= tableFieldCount) {
                    final int postfix = currentCount / tableFieldCount;
                    final String alias = tableName + postfix;
                    columnIndexToTableName.add(i, alias);
                } else {
                    columnIndexToTableName.add(i, tableName);
                }
                tableOccurenceCounter.put(tableName, tableOccurenceCounter.get(tableName) + 1);
            } else { // first occurence of a table name
                columnIndexToTableName.add(i, tableName);
                tableOccurenceCounter.put(tableName, 1);
            }
        }

        this.catalog = catalog;
        this.columnIndexToTableName = columnIndexToTableName;
        this.columnToTableNameMap = columnToTableNameMap;
        this.tableOccurenceCounter = tableOccurenceCounter;
    }

    /**
     * Searches through {@link WayangAggregate} and {@link WayangProject} chains.
     * These nodes uses aliases which have to be unpacked by finding the last
     * non aliased input. This input still contains Calcite's unique integer
     * identifier at the end of the column name which will have to be removed.
     * 
     * @param startNode     starting {@link WayangAggregate} or
     *                      {@link WayangProject}
     * @param columnIndexes starting column indexes that you want to de-alias
     * @return input of the last {@RelNode} in the chain
     */
    public static List<RelDataTypeField> getGlobalCatalogColumnName(final RelNode startNode,
            final List<Integer> columnIndexes) {

        RelNode currentRelNode = startNode;
        List<Integer> currentIndexes = columnIndexes;

        while (currentRelNode instanceof WayangAggregate || currentRelNode instanceof WayangProject) {
            final RelNode next = currentRelNode.getInput(0);

            if (next instanceof WayangAggregate) {
                final List<Integer> nextIndexes = ((WayangAggregate) next).getAggCallList().stream()
                        .map(agg -> agg.getArgList().get(0))
                        .collect(Collectors.toList());

                currentIndexes = currentIndexes.stream()
                        .map(index -> nextIndexes.get(index))
                        .collect(Collectors.toList());
            } else if (next instanceof WayangProject) {
                final List<Integer> nextIndexes = ((WayangProject) next).getProjects().stream()
                        .map(proj -> proj.hashCode())
                        .collect(Collectors.toList());

                currentIndexes = currentIndexes.stream()
                        .map(index -> nextIndexes.get(index))
                        .collect(Collectors.toList());
            }
            currentRelNode = next;
        }

        final RelNode selectChainInputNode = currentRelNode;

        return currentIndexes.stream()
                .map(index -> selectChainInputNode.getRowType().getFieldList().get(index))
                .collect(Collectors.toList());
    }
}
