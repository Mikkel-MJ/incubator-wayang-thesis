package org.apache.wayang.api.sql.calcite.converter.calciteserialisation;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.externalize.RelJson.InputTranslator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.wayang.api.sql.calcite.optimizer.Optimizer;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CalciteRexSerializable implements CalciteSerializable, InputTranslator {

    protected transient RexNode[] serializables;
    protected final SqlTypeName typeName;

    /**
     * Handles {@link RexNode}-serialisation for classes.
     * See {@link CalciteAggSerializable} for serializable aggregate calls.
     * Serialisation requires that the {@link Optimizer} has been created, see {@link CalciteSerializable}.
     * @param rexNodes
     */
    protected CalciteRexSerializable(final RexNode... rexNodes) {
        this.serializables = rexNodes;
        this.typeName = rexNodes[0].getType().getSqlTypeName();
    }

    /**
     * Deserialisation method for {@link RexNode}s.
     * This method is taken directly from RelJson as it has hidden visibility read
     * {@link RelJson.InputTranslator}.
     * This implementation should be removed in future updates.
     * 
     * @param relJson
     * @param input
     * @param map
     * @param relInput
     * @return
     */
    @Override
    public RexNode translateInput(final RelJson relJson, final int input,
            final Map<String, @Nullable Object> map, final RelInput relInput) {
        final RelOptCluster cluster = relInput.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();

        // Check if it is a local ref.
        if (map.containsKey("type")) {
            final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
            final RelDataType type = relJson.toType(typeFactory, map.get("type"));
            return rexBuilder.makeLocalRef(type, input);
        }

        System.out.println("sql type name " + this.typeName);
        System.out.println("input int " + input);
        System.out.println("type factory " + Optimizer.getTypeFactory());
        
        return rexBuilder.makeInputRef(Optimizer.getTypeFactory()
                .createSqlType(this.typeName), input);
    }

    private void writeObject(final ObjectOutputStream out) throws IOException {
        final String[] nodes = this.getNodesAsStrings(serializables); //serialize rexnodes

        out.defaultWriteObject(); //write sqltypename
        out.writeObject(nodes); //write the rexnodes
    }

    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject(); //read sqlTypeName

        final String[] nodesAsStrings = (String[]) in.readObject(); // read in the rexnode json objects

        final Function<String, Map<String, Object>> stringToJsonObjMapper = (string) -> {
            try {
                return (new ObjectMapper()).configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                        .readValue(string, TYPE_REF);
            } catch (final Exception e) {
                e.printStackTrace();
            }
            return null;
        };

        final Function<Map<String, Object>, RexNode> jsonToRexNode = (map) -> RelJson
                .readExpression(Optimizer.getCluster(), this, map);

        final RexNode[] rexNodes = Arrays.stream(nodesAsStrings)
                .map(stringToJsonObjMapper::apply)  //map rexnode strings to json objs
                .map(jsonToRexNode::apply)  //map the json objs to rexnodes
                .toArray(RexNode[]::new); //wrap in array so they can be set

        this.serializables = rexNodes;
    }
}
