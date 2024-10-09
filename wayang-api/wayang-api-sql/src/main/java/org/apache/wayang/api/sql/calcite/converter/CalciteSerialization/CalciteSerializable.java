package org.apache.wayang.api.sql.calcite.converter.CalciteSerialization;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.JsonBuilder;
import org.apache.wayang.api.sql.calcite.optimizer.Optimizer;
import org.apache.wayang.api.sql.context.SqlContext;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Interface for serialisation for calcite {@link RexNode}s and
 * {@link AggregateCall} containing classes.
 * 
 * Serialisation as of now requires that the {@link Optimizer} has been created
 * for the sake of
 * initialising the Calcite Schema/Cluster. In practice this happens when
 * {@link SqlContext}
 * is created, but some use cases may require the manual initialisation of the
 * {@link Optimizer}.
 * For manual initialisation see test {@link SqlToWayangRelTest}.
 */
public interface CalciteSerializable extends Serializable {
    /**
     * This value is taken directly from calcite.
     */
    public static final TypeReference<LinkedHashMap<String, Object>> TYPE_REF = new TypeReference<LinkedHashMap<String, Object>>() {
    };

    /**
     * Default implementation for serialising {@link AggregateCall} and
     * {@link RexNode}.
     * Has no safety checking for other types.
     * 
     * @param nodes An array of RexNodes or AggregateCalls
     * @return a readily serializable string array
     */
    default String[] getNodesAsStrings(final Object[] nodes) {
        final JsonBuilder jb = new JsonBuilder();
        final RelJson writer = new RelJson(jb);

        final String[] nodesAsStrings = Arrays.stream(nodes)
                .map(writer::toJson)    // convert nodes to json objects
                .map(jb::toJsonString) // convert json objects to string
                .toArray(String[]::new);

        return nodesAsStrings;
    }
}
