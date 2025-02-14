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
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.converter.TableScanVisitor;
import org.apache.wayang.api.sql.calcite.optimizer.Optimizer;
import org.apache.wayang.api.sql.calcite.rules.WayangMultiConditionJoinSplitRule;
import org.apache.wayang.api.sql.calcite.rules.WayangRules;
import org.apache.wayang.api.sql.calcite.schema.SchemaUtils;
import org.apache.wayang.api.sql.calcite.utils.AliasFinder;
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

    public final CalciteSchema calciteSchema;

    public SqlContext() throws SQLException {
        this(new Configuration());
    }

    public SqlContext(final Configuration configuration) throws SQLException {
        super(configuration.fork(String.format("SqlContext(%s)", configuration.getName())));

        this.withPlugin(Java.basicPlugin());
        this.withPlugin(Spark.basicPlugin());
        this.withPlugin(Postgres.plugin());

        calciteSchema = SchemaUtils.getSchema(configuration);
    }

    public SqlContext(final Configuration configuration, final List<Plugin> plugins) throws SQLException {
        super(configuration.fork(String.format("SqlContext(%s)", configuration.getName())));

        for (final Plugin plugin : plugins) {
            this.withPlugin(plugin);
        }

        calciteSchema = SchemaUtils.getSchema(configuration);
    }

    public SqlContext(final Configuration configuration, final Plugin... plugins) throws SQLException {
        super(configuration.fork(String.format("SqlContext(%s)", configuration.getName())));

        for (final Plugin plugin : plugins) {
            this.withPlugin(plugin);
        }

        calciteSchema = SchemaUtils.getSchema(configuration);
    }

    /**
     * Method for building {@link WayangPlan}s useful for testing, benchmarking and
     * other
     * usages where you want to handle the intermediate {@link WayangPlan}
     * 
     * @param sql     sql query string with the {@code ;} cut off
     * @param udfJars
     * @return a {@link WayangPlan} of a given sql string
     * @throws SqlParseException
     */
    public WayangPlan buildWayangPlan(final String sql, final String... udfJars) throws SqlParseException {
        final Properties configProperties = Optimizer.ConfigProperties.getDefaults();
        final RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();

        final Optimizer optimizer = Optimizer.create(calciteSchema, configProperties,
                relDataTypeFactory);

        final SqlNode sqlNode = optimizer.parseSql(sql);
        final SqlNode validatedSqlNode = optimizer.validate(sqlNode);
        final RelNode relNode = optimizer.convert(validatedSqlNode);

        final TableScanVisitor visitor = new TableScanVisitor(new ArrayList<>(), null);
        visitor.visit(relNode, 0, null);

        final AliasFinder aliasFinder = new AliasFinder(visitor);

        final RuleSet rules = RuleSets.ofList(
                CoreRules.FILTER_INTO_JOIN,
                WayangRules.WAYANG_TABLESCAN_RULE,
                WayangRules.WAYANG_TABLESCAN_ENUMERABLE_RULE,
                WayangRules.WAYANG_PROJECT_RULE,
                WayangRules.WAYANG_FILTER_RULE,
                WayangRules.WAYANG_JOIN_RULE,
                WayangRules.WAYANG_AGGREGATE_RULE,
                WayangRules.WAYANG_MULTI_CONDITION_JOIN_RULE);

        final RelNode wayangRel = optimizer.optimize(
                relNode,
                relNode.getTraitSet().plus(WayangConvention.INSTANCE),
                rules);

        final Collection<Record> collector = new ArrayList<>();
        final WayangPlan wayangPlan = optimizer.convert(wayangRel, collector, aliasFinder);

        return wayangPlan;
    }

    /**
     * Executes sql with varargs udfJars. udfJars can help in serialisation contexts
     * where the
     * jars need to be used for serialisation remotely, in use cases like Spark and
     * Flink.
     * udfJars can be given by: {@code ReflectionUtils.getDeclaringJar(Foo.class)}
     *
     * @param sql     string sql without ";"
     * @param udfJars varargs of your udf jars, typically just the calling class
     * @return collection of sql records
     * @throws SqlParseException
     */
    public Collection<Record> executeSql(final String sql, final String... udfJars) throws SqlParseException {
        final Properties configProperties = Optimizer.ConfigProperties.getDefaults();
        final RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();

        final Optimizer optimizer = Optimizer.create(calciteSchema, configProperties,
                relDataTypeFactory);

        final SqlNode sqlNode = optimizer.parseSql(sql);
        final SqlNode validatedSqlNode = optimizer.validate(sqlNode);
        final RelNode relNode = optimizer.convert(validatedSqlNode);
        // we call optimizer.prepare to remove structures within the relnode tree before handled by
        // the sql api.
        final RelNode fixedTree = optimizer.prepare(relNode);
        PrintUtils.print("Tree after converting to handleable tree: ", fixedTree);
        final TableScanVisitor visitor = new TableScanVisitor(new ArrayList<>(), null);
        visitor.visit(fixedTree, 0, null);

        final AliasFinder aliasFinder = new AliasFinder(visitor);

        final RuleSet rules = RuleSets.ofList(
                WayangRules.WAYANG_TABLESCAN_RULE,
                WayangRules.WAYANG_TABLESCAN_ENUMERABLE_RULE,
                WayangRules.WAYANG_PROJECT_RULE,
                WayangRules.WAYANG_FILTER_RULE,
                WayangRules.WAYANG_JOIN_RULE,
                WayangRules.WAYANG_AGGREGATE_RULE,
                WayangRules.WAYANG_MULTI_CONDITION_JOIN_RULE);

        final RelNode wayangRel = optimizer.optimize(
                fixedTree,
                fixedTree.getTraitSet().plus(WayangConvention.INSTANCE),
                rules);

        PrintUtils.print("Logical WayangPlan", wayangRel);

        final Collection<Record> collector = new ArrayList<>();
        final WayangPlan wayangPlan = optimizer.convert(wayangRel, collector, aliasFinder);

        if (udfJars.length == 0) {
            PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes().forEach(node -> {
                if (!node.isSink())
                    node.addTargetPlatform(Postgres.platform());
            });
            this.execute(getJobName(), wayangPlan);
        } else {
            Arrays.stream(udfJars).forEach(System.out::println);
            this.execute(getJobName(), wayangPlan, udfJars);
        }

        return collector;
    }

    private static String getJobName() {
        return "SQL[" + jobId.incrementAndGet() + "]";
    }
}
