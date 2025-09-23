package org.apache.wayang.api.sql.calcite.utils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
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
                        .map(nextIndexes::get)
                        .collect(Collectors.toList());
            } else if (next instanceof WayangProject) {
                final List<Integer> nextIndexes = ((WayangProject) next).getProjects().stream()
                        .map(Object::hashCode)
                        .collect(Collectors.toList());

                currentIndexes = currentIndexes.stream()
                        .map(nextIndexes::get)
                        .collect(Collectors.toList());
            }
            currentRelNode = next;
        }

        final RelNode selectChainInputNode = currentRelNode;

        return currentIndexes.stream()
                .map(index -> selectChainInputNode.getRowType().getFieldList().get(index))
                .collect(Collectors.toList());
    }

    public static String[] getAliasedFields(final WayangRel wayangRelNode) {
        final String[] unaliasedFieldsNames = CalciteSources.getUnaliasedFields(wayangRelNode);
        final String[] unaliasedTableNames = CalciteSources.getOriginalTableFromColumn(wayangRelNode);
        final String[] aliasedFieldNames = new String[wayangRelNode.getRowType().getFieldCount()];

        for (int i = 0; i < aliasedFieldNames.length; i++) {
            final String originalName = unaliasedFieldsNames[i];
            final String alias = wayangRelNode.getRowType().getFieldNames().get(i);
            final String originalTableName = unaliasedTableNames[i];

            aliasedFieldNames[i] = originalTableName + "." + originalName + " AS " + alias;
        }

        return aliasedFieldNames;
    }

    public static String[] getUnaliasedFields(final WayangRel wayangRelNode) {
        final RelOptCluster cluster = wayangRelNode.getCluster();
        final RelMetadataQuery metadata = cluster.getMetadataQuery();

        System.out.println("[CalciteSources.getUnaliasedFields().node]: " + wayangRelNode);

        System.out.println(
                "[CalciteSources.getUnaliasedFields().node.FieldList]: " + wayangRelNode.getRowType().getFieldList());

        final List<RelColumnOrigin> origins = wayangRelNode.getRowType().getFieldList().stream()
                .map(field -> metadata.getColumnOrigin(wayangRelNode, field.getIndex()))
                .collect(Collectors.toList());

        System.out.println("[CalciteSources.getUnaliasedFields().origins]: " + origins);

        final String[] originalNames = origins.stream()
                .map(origin -> origin.getOriginTable().getRowType().getFieldList().get(origin.getOriginColumnOrdinal()))
                .map(RelDataTypeField::getName)
                .toArray(String[]::new);

        return originalNames;
    }

    public static String[] getOriginalTableFromColumn(final WayangRel wayangRelNode) {
        final RelOptCluster cluster = wayangRelNode.getCluster();
        final RelMetadataQuery metadata = cluster.getMetadataQuery();

        final Stream<RelColumnOrigin> origins = wayangRelNode.getRowType().getFieldList().stream()
                .map(field -> metadata.getColumnOrigin(wayangRelNode, field.getIndex()));

        final String[] originalNames = origins
                .map(origin -> origin.getOriginTable().getQualifiedName().get(1))
                .toArray(String[]::new);

        return originalNames;
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
        return metadata.getColumnOrigin(relNode, index)
                .getOriginTable();
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
