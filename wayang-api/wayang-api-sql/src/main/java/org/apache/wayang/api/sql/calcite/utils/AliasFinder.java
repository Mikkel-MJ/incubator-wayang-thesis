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
        this.catalog = visitor.catalog.getRowType().getFieldList();
        this.columnIndexToTableName = new ArrayList<>(catalog.size());
        this.columnToTableNameMap = CalciteSources.createColumnToTableOriginMap(visitor.catalog);
        this.tableOccurenceCounter = new HashMap<>();

        for (int i = 0; i < this.catalog.size(); i++) {
            final String tableName = columnToTableNameMap.get(this.catalog.get(i));

            if (this.tableOccurenceCounter.containsKey(tableName)) {
                final int currentCount = this.tableOccurenceCounter.get(tableName);

                // If the counter size exceeds the number of fields within the table,
                // we need to alias
                final int tableFieldCount = CalciteSources.tableOriginOf(visitor.catalog, i).getRowType()
                        .getFieldCount();

                if (currentCount >= tableFieldCount) {
                    final int postfix = currentCount / tableFieldCount;
                    final String alias = tableName + postfix;
                    this.columnIndexToTableName.add(i, alias);
                } else {
                    this.columnIndexToTableName.add(i, tableName);
                }
                this.tableOccurenceCounter.put(tableName, this.tableOccurenceCounter.get(tableName) + 1);
            } else { // first occurence of a table name
                this.columnIndexToTableName.add(i, tableName);
                this.tableOccurenceCounter.put(tableName, 1);
            }
        }
    }
}
