package org.apache.wayang.api.sql.calcite.utils;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.wayang.api.sql.calcite.rel.WayangRel;

public final class CalciteSources {
    protected CalciteSources () {

    }

    /**
     * Gets the table origin of a field
     * @param relNode the wayangRelNode we are operating on
     * @return a map that maps a field to its table source
     */
    public static Map<RelDataTypeField, String> getTableOriginForColumn(final WayangRel relNode) {
        return relNode.getRowType().getFieldList().stream()
                .collect(Collectors.toMap(
                        field -> field,
                        field -> originOf(relNode, field.getIndex())));
    }

    /**
     * Returns the table name origin of a {@link WayangRel} and a field's id.
     * See {@link #getTableOriginForColumn} for a practical use case.
     * @param relNode any {@link WayangRel} inheritors like {@link WayangJoin}
     * @param index id of field from relnode
     * @return table name
     */
    public static String originOf(final WayangRel relNode, final Integer index) {
        final RelMetadataQuery metadata = relNode.getCluster() // get project metadata
                .getMetadataQuerySupplier()
                .get();

        final String tableName = metadata.getColumnOrigin(relNode, index) // get the origin table of the column
                .getOriginTable()
                .getQualifiedName()
                .get(1);

        return tableName;
    }
}
