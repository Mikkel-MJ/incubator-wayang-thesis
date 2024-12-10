package org.apache.wayang.api.sql.calcite.utils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;

import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.api.sql.calcite.rel.WayangRel;

/**
 * Utility class that simplifies some of the more strenuous getters for columns and table origins
 */
public final class CalciteSources {
    protected CalciteSources() {

    }

    /**
     * Calcite uses integers as identifiers to preserve uniqueness in their column
     * names, however, when this is converted to sql it will produce an error, this
     * method allows you to fetch the pure SQL names of a table's columns.
     * 
     * @param wayangRelNode the {@link RelNode} whose SQL column names you want to
     *                      fetch
     * @return list of names as {@code String}s specified in a {@code table.column} manner
     */
    public static List<String> getSqlColumnNames(final RelNode wayangRelNode) {
        return wayangRelNode.getCluster().getMetadataQuery().getTableReferences(wayangRelNode).stream()
                .map(RelTableRef::getTable)
                .map(table -> table.getRowType()
                        .getFieldList()
                        .stream()
                        .map(column -> table.getQualifiedName().get(1) + "." + column.getName())
                        .collect(Collectors.toList()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Calcite uses integers as identifiers to preserve uniqueness in their column
     * names, however, when this is converted to sql it will produce an error, this
     * method allows you to fetch the pure SQL names of a table's columns.
     * 
     * @param wayangRelNode the {@link RelNode} whose SQL column names you want to
     *                      fetch
     * @param aliasFinder   the aliasFinder of each {@link WayangRelNodeVisitor}
     *                      node
     * @return list of names as {@code String}s specified in a table.column manner
     */
    public static List<String> getSqlColumnNames(final RelNode wayangRelNode, AliasFinder aliasFinder) {
        return wayangRelNode.getCluster().getMetadataQuery().getTableReferences(wayangRelNode).stream()
                .map(RelTableRef::getTable)
                .map(table -> table.getRowType()
                        .getFieldList()
                        .stream()
                        .map(column -> {System.out.print("col index: " + column.getIndex()); return aliasFinder.columnIndexToTableName.get(column.getIndex()) + "." + column.getName();})
                        .collect(Collectors.toList()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Calcite uses integers as identifiers to preserve uniqueness in their column
     * names, however, when this is converted to sql it will produce an error. This
     * method allows you to lookup the original table sources of the columns, so you
     * can fetch their SQL name.
     * 
     * @param wayangRelNode the {@link RelNode} whose SQL column names you want to
     *                      fetch
     * @return a map that maps a {@link RelOptTable} to its columns - a list
     *         containing {@link RelDataTypeField}s.
     */
    public static Map<RelOptTable, List<RelDataTypeField>> tableToColumnMap(final RelNode wayangRelNode) {
        return wayangRelNode.getCluster()
                .getMetadataQuery()
                .getTableReferences(wayangRelNode)
                .stream()
                .map(RelTableRef::getTable)
                .collect(Collectors.toMap(
                        table -> table,
                        table -> table.getRowType().getFieldList()));
    }

    /**
     * A mapping that maps a column to its table origin.
     * 
     * @param relNode the wayangRelNode we are operating on
     * @return a map that maps a field to its table source
     */
    public static Map<RelDataTypeField, String> createColumnToTableOriginMap(final RelNode relNode) {
        return relNode.getRowType().getFieldList().stream()
                .collect(Collectors.toMap(
                        field -> field,
                        field -> tableNameOriginOf(relNode, field.getIndex())));
    }

    /**
     * Returns the table name origin of a {@link WayangRel} and a field's id.
     * See {@link #createColumnToTableOriginMap} for a practical use case.
     * 
     * @param relNode any {@link WayangRel} inheritors like {@link WayangJoin}
     * @param index   id of field from relnode
     * @return table name
     */
    public static String tableNameOriginOf(final RelNode relNode, final Integer index) {
        return tableOriginOf(relNode, index).getQualifiedName().get(1);
    }

    /**
     * Returns the {@link RelOptTable} origin of a {@link WayangRel} and a field's
     * id.
     * See {@link #createColumnToTableOriginMap} for a practical use case.
     * 
     * @param relNode any {@link WayangRel} inheritors like {@link WayangJoin}
     * @param index   id of field from a {@link RelNode}
     * @return table name
     */
    public static RelOptTable tableOriginOf(final RelNode relNode, final Integer index) {
        // get project metadata
        final RelMetadataQuery metadata = relNode.getCluster()
                .getMetadataQuerySupplier()
                .get();

        // get the origin table of the column
        final RelOptTable table = metadata.getColumnOrigin(relNode, index)
                .getOriginTable();

        return table;
    }

    /**
     * Matches a Calcite column name with unique indentifiers i.e. {@code column0}
     * with its SQL trueform equivalent {@code column}
     * 
     * @param badName Calcite column name
     * @param catalog see {@link #getSqlColumnNames(RelNode)}
     * @return SQL column name
     */
    public static String findSqlName(String badName, List<String> catalog) {
        for (final String name : catalog) {
            if (badName.contains(name)) { // we find it in the catalog
                return name; // and replace it
            }
        }

        return badName;
    }
}
