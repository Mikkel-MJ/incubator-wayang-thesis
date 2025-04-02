/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.converter.calciteserialisation.CalciteSerializable;
import org.apache.wayang.api.sql.calcite.converter.filterhelpers.FilterPredicateImpl;
import org.apache.wayang.api.sql.calcite.optimizer.Optimizer;
import org.apache.wayang.api.sql.calcite.rules.WayangRules;
import org.apache.wayang.api.sql.calcite.schema.SchemaUtils;
import org.apache.wayang.api.sql.calcite.schema.WayangSchema;
import org.apache.wayang.api.sql.calcite.schema.WayangSchemaBuilder;
import org.apache.wayang.api.sql.calcite.schema.WayangTable;
import org.apache.wayang.api.sql.calcite.schema.WayangTableBuilder;
import org.apache.wayang.api.sql.calcite.utils.ModelParser;
import org.apache.wayang.api.sql.context.SqlContext;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;

import org.json.simple.parser.ParseException;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class SqlToWayangRelTest {
    @Test
    public void javaCrossJoin() throws Exception {
        final SqlContext sqlContext = createSqlContext("/model-example-min.json", "/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = sqlContext.buildCollectorAndWayangPlan(
                "select * from fs.exampleSmallA cross join fs.exampleSmallB");

        final Collection<Record> collector = t.field0;
        final WayangPlan wayangPlan = t.field1;

        PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes().forEach(node -> {
            node.addTargetPlatform(Java.platform());
        });

        sqlContext.execute(wayangPlan);
        final Collection<org.apache.wayang.basic.data.Record> result = collector;
        System.out.println(result);

        final List<Record> shouldBe = List.of(
                new Record("item1", "item2", "item1", "item2", "item3"),
                new Record("item1", "item2", "item1", "item2", "item3"),
                new Record("item1", "item2", "item1", "item2", "item3"),
                new Record("item1", "item2", "item1", "item2", "item3"),
                new Record("item1", "item2", "x"    , "x"    , "x"),
                new Record("item1", "item2", "x"    , "x"    , "x")
        );

        final Map<Record, Integer> resultTally = result.stream().collect(Collectors.toMap(rec -> rec, rec -> 1, Integer::sum));
        final Map<Record, Integer> shouldBeTally = shouldBe.stream().collect(Collectors.toMap(rec -> rec, rec -> 1, Integer::sum));

        System.out.println("result: " + result);
        System.out.println(resultTally);
        System.out.println(shouldBeTally);
        assert (resultTally.equals(shouldBeTally));
    }

    // @Test
    public void javaReduceBy() throws Exception {
        final SqlContext sqlContext = createSqlContext("/model-example-min.json", "/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = sqlContext.buildCollectorAndWayangPlan(
                "select exampleSmallA.COLA, count(*) from fs.exampleSmallA join fs.exampleSmallB on exampleSmallA.COLA = exampleSmallB.COLA and exampleSmallA.COLB = exampleSmallB.COLB group by exampleSmallA.COLA");

        final Collection<Record> collector = t.field0;
        final WayangPlan wayangPlan = t.field1;

        PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes().forEach(node -> {
            node.addTargetPlatform(Java.platform());
        });

        sqlContext.execute(wayangPlan);
        final Collection<org.apache.wayang.basic.data.Record> result = collector;
        System.out.println(result);

        assert (result.stream().findFirst().get().equals(new Record("item1", 4)));
    }

    // @Test
    public void javaMultiConditionJoinSmall() throws Exception {
        final SqlContext sqlContext = createSqlContext("/model-example-min.json", "/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = sqlContext.buildCollectorAndWayangPlan(
                "SELECT * FROM fs.exampleSmallA JOIN fs.exampleSmallB ON exampleSmallA.COLA = exampleSmallB.COLA AND exampleSmallA.COLB = exampleSmallB.COLB");
        final Collection<Record> collector = t.field0;
        final WayangPlan wayangPlan = t.field1;

        PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes().forEach(node -> {
            node.addTargetPlatform(Java.platform());
        });

        sqlContext.execute(wayangPlan);
        final Collection<org.apache.wayang.basic.data.Record> result = collector;
        System.out.println(result);

        assert (result.stream().findFirst().get().equals(new Record("item1", "item2", "item1", "item2", "item3")))
                : "record mismatch got: " + result.stream().findFirst().get() + ", but expected: "
                        + new Record("item1", "item2", "item1", "item2", "item3");
        assert (result.size() == 4);

        assert (result.stream().allMatch(rec -> result.equals(rec.stream().findFirst().get())));
    }

    // @Test
    public void javaMultiConditionJoin() throws Exception {
        final SqlContext sqlContext = createSqlContext("/model-example-min.json", "/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = sqlContext.buildCollectorAndWayangPlan(
                "SELECT * FROM fs.largeLeftTableIndex JOIN fs.exampleRefToRef ON largeLeftTableIndex.NAMEA = exampleRefToRef.NAMEA AND largeLeftTableIndex.NAMEB = exampleRefToRef.NAMEB");
        final Collection<Record> collector = t.field0;
        final WayangPlan wayangPlan = t.field1;

        PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes().forEach(node -> {
            node.addTargetPlatform(Java.platform());
        });

        sqlContext.execute(wayangPlan);
        final Collection<org.apache.wayang.basic.data.Record> result = collector;
        System.out.println(result);
    }

    // @Test
    public void aggregateCountInJavaWithIntegers() throws Exception {
        final SqlContext sqlContext = createSqlContext("/model-example-min.json", "/data/exampleInt.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = sqlContext.buildCollectorAndWayangPlan(
                "SELECT exampleInt.NAMEC, COUNT(*) FROM fs.exampleInt GROUP BY NAMEC");
        final Collection<Record> collector = t.field0;
        final WayangPlan wayangPlan = t.field1;

        PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes().forEach(node -> {
            node.addTargetPlatform(Java.platform());
        });

        sqlContext.execute(wayangPlan);
        final Collection<org.apache.wayang.basic.data.Record> result = collector;

        assert (result.size() > 0);
    }

    // @Test
    public void aggregateCountInJava() throws Exception {
        final SqlContext sqlContext = createSqlContext("/model-example-min.json", "/data/largeLeftTableIndex.csv");

        final Tuple2<Collection<Record>, WayangPlan> t = sqlContext.buildCollectorAndWayangPlan(
                "SELECT largeLeftTableIndex.NAMEC, COUNT(*) FROM fs.largeLeftTableIndex GROUP BY NAMEC");
        final Collection<Record> collector = t.field0;
        final WayangPlan wayangPlan = t.field1;

        PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes().forEach(node -> {
            node.addTargetPlatform(Java.platform());
        });

        sqlContext.execute(wayangPlan);
        final Collection<org.apache.wayang.basic.data.Record> result = collector;

        final Record rec = result.stream().findFirst().get();
        assert (rec.size() == 2);
        assert (rec.getInt(1) == 3);
    }

    // @Test
    public void rexSerializationTest() throws Exception {
        // create filterPredicateImpl for serialisation
        final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        final RexBuilder rb = new RexBuilder(typeFactory);
        final RexNode leftOperand = rb.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        final RexNode rightOperand = rb.makeLiteral("test");
        final RexNode cond = rb.makeCall(SqlStdOperatorTable.EQUALS, leftOperand, rightOperand);
        final CalciteSerializable fpImpl = (CalciteSerializable) new FilterPredicateImpl(cond);

        final Properties configProperties = Optimizer.ConfigProperties.getDefaults();
        final RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();
        final String calciteModelPath = SqlAPI.class.getResource("/model-example-min.json").getPath();
        final Configuration configuration = new ModelParser(new Configuration(), calciteModelPath).setProperties();
        final CalciteSchema calciteSchema = SchemaUtils.getSchema(configuration);
        final Optimizer optimizer = Optimizer.create(calciteSchema, configProperties, relDataTypeFactory);

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject((CalciteSerializable) fpImpl);
        objectOutputStream.close();

        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
                byteArrayOutputStream.toByteArray());
        final ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        final CalciteSerializable deserializedObject = (CalciteSerializable) objectInputStream.readObject();
        objectInputStream.close();

        assert (((FilterPredicateImpl) deserializedObject).test(new Record("test")));
    }

    // @Test
    public void filterIsNull() throws Exception {
        final SqlContext sqlContext = createSqlContext("/model-example-min.json", "/data/largeLeftTableIndex.csv");

        final Collection<org.apache.wayang.basic.data.Record> result = sqlContext.executeSql(
                "SELECT * FROM fs.largeLeftTableIndex WHERE (largeLeftTableIndex.NAMEA IS NULL)" //
        );

        assert (result.size() == 0);
    }

    // @Test
    public void filterIsNotValue() throws Exception {
        final SqlContext sqlContext = createSqlContext("/model-example-min.json", "/data/largeLeftTableIndex.csv");

        final Collection<org.apache.wayang.basic.data.Record> result = sqlContext.executeSql(
                "SELECT * FROM fs.largeLeftTableIndex WHERE (largeLeftTableIndex.NAMEA <> 'test1')" //
        );

        assert (!result.stream().anyMatch(record -> record.getField(0).equals("test1")));
    }

    private SqlContext createSqlContext(final String calciteResourceName, final String tableResourceName)
            throws IOException, ParseException, SQLException {
        final String calciteModelPath = SqlAPI.class.getResource(calciteResourceName).getPath();
        assert (calciteModelPath != "" && calciteModelPath != null)
                : "Could not get calcite model from path: " + calciteResourceName;

        final Configuration configuration = new ModelParser(new Configuration(), calciteModelPath).setProperties();
        assert (configuration != null)
                : "Could not build configuration from calcite model path: " + calciteModelPath;

        final String dataPath = SqlAPI.class.getResource(tableResourceName).getPath();
        assert (dataPath != "" && dataPath != null) : "Could not get resource from path: " + tableResourceName;

        configuration.setProperty("wayang.fs.table.url", dataPath);

        configuration.setProperty(
                "wayang.ml.executions.file",
                "mle" + ".txt");

        configuration.setProperty(
                "wayang.ml.optimizations.file",
                "mlo" + ".txt");

        configuration.setProperty("wayang.ml.experience.enabled", "false");

        final SqlContext sqlContext = new SqlContext(configuration);
        return sqlContext;
    }

    // @Test
    public void filterIsNotNull() throws Exception {
        final SqlContext sqlContext = createSqlContext("/model-example-min.json", "/data/largeLeftTableIndex.csv");

        final Collection<org.apache.wayang.basic.data.Record> result = sqlContext.executeSql(
                "SELECT * FROM fs.largeLeftTableIndex WHERE (largeLeftTableIndex.NAMEA IS NOT NULL)" //
        );

        assert (!result.stream().anyMatch(record -> record.getField(0).equals(null)));
    }

    // @Test
    public void filterWithNotLike() throws Exception {
        final SqlContext sqlContext = createSqlContext("/model-example-min.json", "/data/largeLeftTableIndex.csv");

        final Collection<org.apache.wayang.basic.data.Record> result = sqlContext.executeSql(
                "SELECT * FROM fs.largeLeftTableIndex WHERE (largeLeftTableIndex.NAMEA NOT LIKE '_est1')" //
        );

        assert (!result.stream().anyMatch(record -> record.getString(0).equals("test1")));
    }

    // @Test
    public void filterWithLike() throws Exception {
        final SqlContext sqlContext = createSqlContext("/model-example-min.json", "/data/largeLeftTableIndex.csv");

        final Collection<org.apache.wayang.basic.data.Record> result = sqlContext.executeSql(
                "SELECT * FROM fs.largeLeftTableIndex WHERE (largeLeftTableIndex.NAMEA LIKE '_est1' OR largeLeftTableIndex.NAMEA LIKE 't%')" //
        );
    }

    // @Test
    public void joinWithLargeLeftTableIndexCorrect() throws Exception {
        final SqlContext sqlContext = createSqlContext("/model-example-min.json", "/data/largeLeftTableIndex.csv");

        final Collection<org.apache.wayang.basic.data.Record> result = sqlContext.executeSql(
                "SELECT * FROM fs.largeLeftTableIndex AS na INNER JOIN fs.largeLeftTableIndex AS nb ON na.NAMEB = nb.NAMEA " //
        );
    }

    // @Test
    public void joinWithLargeLeftTableIndexMirrorAlias() throws Exception {
        final SqlContext sqlContext = createSqlContext("/model-example-min.json", "/data/largeLeftTableIndex.csv");

        final Collection<org.apache.wayang.basic.data.Record> result = sqlContext.executeSql(
                "SELECT * FROM fs.largeLeftTableIndex AS na INNER JOIN fs.largeLeftTableIndex AS nb ON nb.NAMEB = na.NAMEA " //
        );
    }

    // @Test
    public void exampleFilterTableRefToTableRef() throws Exception {
        final SqlContext sqlContext = createSqlContext("/model-example-min.json", "/data/exampleRefToRef.csv");

        final Collection<org.apache.wayang.basic.data.Record> result = sqlContext.executeSql(
                "SELECT * FROM fs.exampleRefToRef WHERE exampleRefToRef.NAMEA = exampleRefToRef.NAMEB" //
        );
    }

    // @Test
    public void exampleMinWithStrings() throws Exception {
        final SqlContext sqlContext = createSqlContext("/model-example-min.json", "/data/exampleMin.csv");

        final Collection<org.apache.wayang.basic.data.Record> result = sqlContext.executeSql(
                "SELECT MIN(exampleMin.NAME) FROM fs.exampleMin" //
        );

        assert (result.stream().findAny().get().getString(0).equals("AA"));
    }

    // @Test
    public void test_simple_sql() throws Exception {
        final WayangTable customer = WayangTableBuilder.build("customer")
                .addField("id", SqlTypeName.INTEGER)
                .addField("name", SqlTypeName.VARCHAR)
                .addField("age", SqlTypeName.INTEGER)
                .withRowCount(100)
                .build();

        final WayangTable orders = WayangTableBuilder.build("orders")
                .addField("id", SqlTypeName.INTEGER)
                .addField("cid", SqlTypeName.INTEGER)
                .addField("price", SqlTypeName.DECIMAL)
                .addField("quantity", SqlTypeName.INTEGER)
                .withRowCount(100)
                .build();

        final WayangSchema wayangSchema = WayangSchemaBuilder.build("exSchema")
                .addTable(customer)
                .addTable(orders)
                .build();

        final Optimizer optimizer = Optimizer.create(wayangSchema);

        final String sql = "select c.name, c.age, o.price from customer c join orders o on c.id = o.cid where c.age > 40 "
                +
                "and o" +
                ".price < 100";

        final SqlNode sqlNode = optimizer.parseSql(sql);
        final SqlNode validatedSqlNode = optimizer.validate(sqlNode);
        final RelNode relNode = optimizer.convert(validatedSqlNode);

        final RuleSet rules = RuleSets.ofList(
                WayangRules.WAYANG_TABLESCAN_RULE,
                WayangRules.WAYANG_PROJECT_RULE,
                WayangRules.WAYANG_FILTER_RULE,
                WayangRules.WAYANG_TABLESCAN_ENUMERABLE_RULE,
                WayangRules.WAYANG_JOIN_RULE,
                WayangRules.WAYANG_AGGREGATE_RULE);

        final RelNode wayangRel = optimizer.optimize(
                relNode,
                relNode.getTraitSet().plus(WayangConvention.INSTANCE),
                rules);
    }

    private void print(final String header, final WayangPlan plan) {
        final StringWriter sw = new StringWriter();
        sw.append(header).append(":").append("\n");

        final Collection<Operator> operators = PlanTraversal.upstream().traverse(plan.getSinks())
                .getTraversedNodes();
        operators.forEach(o -> sw.append(o.toString()));

        System.out.println(sw.toString());
    }

    private void print(final String header, final RelNode relTree) {
        final StringWriter sw = new StringWriter();

        sw.append(header).append(":").append("\n");

        final RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, true);

        relTree.explain(relWriter);

        System.out.println(sw.toString());
    }

}
