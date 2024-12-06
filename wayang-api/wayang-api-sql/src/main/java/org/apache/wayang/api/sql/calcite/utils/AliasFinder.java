package org.apache.wayang.api.sql.calcite.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.wayang.api.sql.calcite.converter.TableScanVisitor;

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
}
