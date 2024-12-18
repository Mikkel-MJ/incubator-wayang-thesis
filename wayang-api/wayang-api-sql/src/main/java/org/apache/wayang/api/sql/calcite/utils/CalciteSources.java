package org.apache.wayang.api.sql.calcite.utils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;

import org.apache.wayang.api.sql.calcite.converter.WayangAggregateVisitor;
import org.apache.wayang.api.sql.calcite.converter.WayangProjectVisitor;
import org.apache.wayang.api.sql.calcite.rel.WayangAggregate;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.api.sql.calcite.rel.WayangProject;
import org.apache.wayang.api.sql.calcite.rel.WayangRel;

/**
 * Utility class that simplifies some of the more strenuous getters for columns
 * and table origins
 */
public final class CalciteSources {
    /**
     * Searches through {@link WayangAggregate} and {@link WayangProject} chains.
     * These nodes uses aliases which have to be unpacked by finding the last
     * non aliased input. This input still contains Calcite's unique integer
     * identifier at the end of the column name which will have to be removed.
     * 
     * @param startNode     starting {@link WayangAggregate} or
     *                      {@link WayangProject}
     * @param columnIndexes starting column indexes that you want to de-alias
     * @return input columns of the last {@RelNode} in the chain
     */
    public static List<RelDataTypeField> getColumnsFromGlobalCatalog(final RelNode startNode,
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

    /**
     * Generates SQL select statements for fields in {@link WayangProjectVisitor}s
     * and {@link WayangAggregateVisitor}s.
     * 
     * @param wayangRelNode current relnode, either projection or aggregate
     * @param columnIndexes
     * @param aliasFinder
     * @return an array containing sql strings in a {@code table.column AS alias}
     *         manner.
     */
    public static String[] getSelectStmntFieldNames(final WayangRel wayangRelNode, final List<Integer> columnIndexes,
            final AliasFinder aliasFinder) {
        // calcite's projections and aggregates use aliased fields this fetches the
        // dealiased fields, from global catalog
        final List<RelDataTypeField> dealiasedFields = CalciteSources.getColumnsFromGlobalCatalog(wayangRelNode,
                columnIndexes);

        // Local catalog for removing calcite's column identifier
        final List<String> dealiasedCatalog = CalciteSources.getSqlColumnNames(wayangRelNode);

        // we specify the dealised fields with their table name in a table.column manner
        final String[] tableSpecifiedFields = dealiasedFields.stream()
                .map(field -> CalciteSources.findSqlName(
                        aliasFinder.columnToTableNameMap.get(field) + "." + field.getName(), dealiasedCatalog) + " AS "
                        + aliasFinder.columnIndexToTableName.get(field.getIndex()))
                .toArray(String[]::new);

        return tableSpecifiedFields;
    }

    /**
     * Calcite uses integers as identifiers to preserve uniqueness in their column
     * names, however, when this is converted to sql it will produce an error, this
     * method allows you to fetch the pure SQL names of a table's columns.
     * 
     * @param wayangRelNode the {@link RelNode} whose SQL column names you want to
     *                      fetch
     * @return list of names as {@code String}s specified in a {@code table.column}
     *         manner
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
    public static List<String> getSqlColumnNames(final RelNode wayangRelNode, final AliasFinder aliasFinder) {
        return wayangRelNode.getCluster().getMetadataQuery().getTableReferences(wayangRelNode).stream()
                .map(RelTableRef::getTable)
                .map(table -> table.getRowType()
                        .getFieldList()
                        .stream()
                        .map(column -> {
                            System.out.print("col index: " + column.getIndex());
                            return aliasFinder.columnIndexToTableName.get(column.getIndex()) + "." + column.getName();
                        })
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
    public static String findSqlName(final String badName, final List<String> catalog) {
        for (final String name : catalog) {
            // TODO: need a more sophisticated way of doing this, this fails on similar
            // names like column movie_keyword.keyword does not exist. Hint: Perhaps you
            // meant to reference the column "movie_keyword.keyword_id".

            if (badName.contains(name)) { // we find it in the catalog
                return name; // and replace it
            }
        }

        return badName;
    }

    protected CalciteSources() {

    }
}
