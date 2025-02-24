package org.apache.wayang.api.sql.calcite.converter.calciteserialisation;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.util.ImmutableBitSet;

import org.apache.wayang.api.sql.calcite.optimizer.Optimizer;

import org.checkerframework.checker.nullness.qual.Nullable;

import com.amazonaws.services.kms.model.UnsupportedOperationException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CalciteAggSerializable implements CalciteSerializable {

    protected transient AggregateCall[] serializables;
    protected final SqlTypeName typeName;

    /**
     * Handles serialisation for classes using {@link AggregateCall}.
     * See {@link CalciteRexSerializable} for serializable RexNodes.
     * Serialisation requires that the {@link Optimizer} has been created, see {@link CalciteSerializable}.
     * @param aggregateCalls an array or variable amount of AggregateCalls
     */
    protected CalciteAggSerializable(final AggregateCall... aggregateCalls) {
        this.serializables = aggregateCalls;
        this.typeName = aggregateCalls[0].getType().getSqlTypeName();
    }

    private void writeObject(final ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();//write default object like sqltypename

        final String[] nodes = this.getNodesAsStrings(serializables); //serialize aggregate calls
        out.writeObject(nodes);
    }

    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        final String[] nodesAsStrings = (String[]) in.readObject(); // read in the json objects

        final Function<String, Map<String, Object>> stringToJsonObjMapper = (string) -> {
            try {
                return (new ObjectMapper()).configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                        .readValue(string, TYPE_REF);
            } catch (final Exception e) {
                e.printStackTrace();
            }
            return null;
        };

        final Function<Map<String, Object>, AggregateCall> jsonToAggregateCall = (map) -> readExpression(
                Optimizer.getCluster(), map);

        final AggregateCall[] aggregateCalls = Arrays.stream(nodesAsStrings)
                .map(stringToJsonObjMapper::apply)
                .map(jsonToAggregateCall::apply)
                .toArray(AggregateCall[]::new);

        this.serializables = aggregateCalls;
    }

    /**
     * Deserialisation method for jsonized {@link AggregateCall}s
     * @param cluster {@link RelOptCluster} from the current {@link Optimizer}
     * @param map json object represented in a Java {@link Map}
     * @return {@link AggregateCall}
     */
    protected AggregateCall readExpression(final RelOptCluster cluster, final Map<String, Object> map) {
        final Map<String, Object> mapAgg    = (Map<String, Object>) map.get("agg"); //get aggregate json object
        final Map<String, Object> mapType   = (Map<String, Object>) map.get("type");
        final SqlAggFunction sqlAggFunction = (SqlAggFunction) this.toOp(mapAgg);

        final boolean distinct = (boolean) map.get("distinct");
        final boolean approximate = false; // no idea how to fetch this
        final boolean ignoreNulls = (boolean) mapType.get("nullable");
        final List<Integer> argList = (List<Integer>) map.get("operands");
        final int filterArg = 0; // no ide ahow to fetch this
        final ImmutableBitSet distinctKeys = null; // no idea how t ofetch this
        final RelCollation collation = RelCollations.EMPTY;
        final RelDataType type = Optimizer.createTypeFactory().createSqlType(typeName);
        final String name = (String) mapAgg.get("name");

        final AggregateCall call = AggregateCall.create(sqlAggFunction, distinct, approximate, ignoreNulls, argList,
                filterArg, distinctKeys, collation, type, name);

        return call;
    }

    /**
     * Duplicate from calcite's {@link RelJson} because of accessibility modifiers.
     * Converts an aggregate call json object to its SqlOperator counterpart.
     * SqlOperators need to be validated before they can be turned into their RexNode/AggregateCall equivalent.
     * @param map
     * @return
     */
    SqlOperator toOp(final Map<String, ? extends @Nullable Object> map) {
        // in case different operator has the same kind, check with both name and kind.
        final String name = (String) map.get("name");
        final String kind = (String) map.get("kind");
        final String syntax = (String) map.get("syntax");
        final SqlKind sqlKind = SqlKind.valueOf(kind);
        final SqlSyntax sqlSyntax = SqlSyntax.valueOf(syntax);
        final List<SqlOperator> operators = new ArrayList<>();

        SqlStdOperatorTable.instance().lookupOperatorOverloads(
                new SqlIdentifier(name, new SqlParserPos(0, 0)),
                null,
                sqlSyntax,
                operators,
                SqlNameMatchers.liberal());

        for (final SqlOperator operator : operators) {
            if (operator.kind == sqlKind) {
                return operator;
            }
        }

        final String class_ = (String) map.get("class");
        if (class_ != null) {
            return AvaticaUtils.instantiatePlugin(SqlOperator.class, class_);
        }

        throw new UnsupportedOperationException("could not convert json object to operator");
    }
    @Override
    public String toString(){
        String serialisablesString = Arrays.stream(serializables)
            .map(AggregateCall::toString)
            .reduce("", String::concat);

        return this.getClass().getSimpleName() + "[Serialisables: " + serialisablesString + ", Type: " + typeName + "]";
    }
}
