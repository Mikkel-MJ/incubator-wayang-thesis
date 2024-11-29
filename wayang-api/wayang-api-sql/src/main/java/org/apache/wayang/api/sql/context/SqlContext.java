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

package org.apache.wayang.api.sql.context;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.optimizer.Optimizer;
import org.apache.wayang.api.sql.calcite.rules.WayangRules;
import org.apache.wayang.api.sql.calcite.schema.SchemaUtils;
import org.apache.wayang.api.sql.calcite.utils.PrintUtils;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.spark.Spark;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;

public class SqlContext extends WayangContext {

    private static final AtomicInteger jobId = new AtomicInteger(0);

    private final CalciteSchema calciteSchema;

    public SqlContext() throws SQLException {
        this(new Configuration());
    }

    public SqlContext(Configuration configuration) throws SQLException {
        super(configuration.fork(String.format("SqlContext(%s)", configuration.getName())));

        this.withPlugin(Java.basicPlugin());
        this.withPlugin(Spark.basicPlugin());
        this.withPlugin(Postgres.plugin());

        calciteSchema = SchemaUtils.getSchema(configuration);
    }

    public SqlContext(Configuration configuration, List<Plugin> plugins) throws SQLException {
        super(configuration.fork(String.format("SqlContext(%s)", configuration.getName())));

        for (Plugin plugin : plugins) {
            this.withPlugin(plugin);
        }

        calciteSchema = SchemaUtils.getSchema(configuration);
    }

    public SqlContext(Configuration configuration, Plugin... plugins) throws SQLException {
        super(configuration.fork(String.format("SqlContext(%s)", configuration.getName())));

        for (Plugin plugin : plugins) {
            this.withPlugin(plugin);
        }

        calciteSchema = SchemaUtils.getSchema(configuration);
    }

    /**
     * Executes sql with varargs udfJars. udfJars can help in serialisation contexts where the
     * jars need to be used for serialisation remotely, in use cases like Spark and
     * Flink.
     * udfJars can be given by: {@code ReflectionUtils.getDeclaringJar(Foo.class)}
     * @param sql     string sql without ";"
     * @param udfJars varargs of your udf jars, typically just the calling class
     * @return collection of sql records
     * @throws SqlParseException
     */
    public Collection<Record> executeSql(String sql, String... udfJars) throws SqlParseException {
        Properties configProperties = Optimizer.ConfigProperties.getDefaults();
        RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();

        Optimizer optimizer = Optimizer.create(calciteSchema, configProperties,
                relDataTypeFactory);
    
        SqlNode sqlNode = optimizer.parseSql(sql);
        SqlNode validatedSqlNode = optimizer.validate(sqlNode);

        System.out.println("sqlNode: " + sqlNode);
        System.out.println("validated sqlNodes: " + validatedSqlNode);

        RelNode relNode = optimizer.convert(validatedSqlNode);

        PrintUtils.print("After parsing sql query", relNode);

        RuleSet rules = RuleSets.ofList(
                WayangRules.WAYANG_TABLESCAN_RULE,
                WayangRules.WAYANG_TABLESCAN_ENUMERABLE_RULE,
                WayangRules.WAYANG_PROJECT_RULE,
                WayangRules.WAYANG_FILTER_RULE,
                WayangRules.WAYANG_JOIN_RULE,
                WayangRules.WAYANG_AGGREGATE_RULE);

        RelNode wayangRel = optimizer.optimize(
                relNode,
                relNode.getTraitSet().plus(WayangConvention.INSTANCE),
                rules);

        PrintUtils.print("After translating logical intermediate plan", wayangRel);

        //Optimizer.getCluster().getPlanner().setRoot(wayangRel);
        //RelNode wayangRelOptimized = Optimizer.getCluster().getPlanner().findBestExp();

        Collection<Record> collector = new ArrayList<>();
        WayangPlan wayangPlan = optimizer.convert(wayangRel, collector);
        PrintUtils.print("After optimiser conversion: ", wayangPlan);

        if (udfJars.length == 0) {
            System.out.println("Executing w/o udfJars");
            //PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes().forEach(node -> {if (!node.isSink()) node.addTargetPlatform(Postgres.platform());});
            this.execute(getJobName(), wayangPlan);
        } else {
            System.out.println("Executing with udfJars: ");
            Arrays.stream(udfJars).forEach(System.out::println);
            this.execute(getJobName(), wayangPlan, udfJars);
        }

        return collector;
    }

    private static String getJobName() {
        return "SQL[" + jobId.incrementAndGet() + "]";
    }
}
