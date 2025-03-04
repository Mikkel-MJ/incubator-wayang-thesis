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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexChecker;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.Litmus;
import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.converter.TableScanVisitor;
import org.apache.wayang.api.sql.calcite.optimizer.Optimizer;
import org.apache.wayang.api.sql.calcite.rules.WayangRules;
import org.apache.wayang.api.sql.calcite.schema.SchemaUtils;
import org.apache.wayang.api.sql.calcite.utils.AliasFinder;
import org.apache.wayang.api.sql.calcite.utils.PrintUtils;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
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
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.List;
import java.util.Map;

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
                WayangRules.WAYANG_BINARY_JOIN_RULE);

        final RelNode wayangRel = optimizer.optimize(
                relNode,
                relNode.getTraitSet().plus(WayangConvention.INSTANCE),
                rules);

        PrintUtils.print("Optimized tree: ", wayangRel);
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
        PrintUtils.print("Calcite logical RelNode tree: ", relNode);

        // we call optimizer.prepare to remove structures within the relnode tree before
        // handled by
        // the sql api.
        final Tuple2<RelNode, HepPlanner> filterIntoJoin = optimizer.prepare(relNode, CoreRules.FILTER_INTO_JOIN);

        PrintUtils.print("Tree after converting to handleable tree: ", filterIntoJoin.field0);

        System.out.println("after filter into join:");
        this.traverseAndCollect(filterIntoJoin.field0).stream().filter(node -> node instanceof Join).forEach(join -> {
            RexChecker checker = new RexChecker(join.getRowType(), null, Litmus.IGNORE);
            System.out.println("isValid: " + checker.isValid(((Join) join).getCondition()) + ", join: " + join);
        });

        final Tuple2<RelNode, HepPlanner> split = optimizer.prepare(filterIntoJoin.field0,
                WayangRules.WAYANG_MULTI_CONDITION_JOIN_SPLIT_RULE);

        PrintUtils.print("Tree after converting to handleable tree: ", split.field0);

        System.out.println("after split rowChecker validity check:");
        this.traverseAndCollect(split.field0).stream().filter(node -> node instanceof Join).forEach(join -> {
            RexChecker checker = new RexChecker(join.getRowType(), null, Litmus.IGNORE);
            System.out.println("isValid: " + checker.isValid(((Join) join).getCondition()) + ", join: " + join);
        });


        final Tuple2<RelNode, HepPlanner> fixedTree = optimizer.prepare(split.field0,
                WayangRules.WAYANG_BINARY_JOIN_RULE);

        PrintUtils.print("Tree after converting to handleable tree: ", fixedTree.field0);

        System.out.println("after binary join rule");
        this.traverseAndCollect(fixedTree.field0).stream().filter(node -> node instanceof Join).forEach(join -> {
            RexChecker checker = new RexChecker(join.getRowType(), null, Litmus.IGNORE);
            System.out.println("isValid: " + checker.isValid(((Join) join).getCondition()) + ", join: " + join);
        });

        final RuleSet rules = RuleSets.ofList(
                WayangRules.WAYANG_TABLESCAN_RULE,
                WayangRules.WAYANG_TABLESCAN_ENUMERABLE_RULE,
                WayangRules.WAYANG_PROJECT_RULE,
                WayangRules.WAYANG_FILTER_RULE,
                WayangRules.WAYANG_JOIN_RULE,
                WayangRules.WAYANG_AGGREGATE_RULE,
                WayangRules.WAYANG_SORT_RULE);

        System.out.println("fixed tree check: ");

        final RelNode wayangRel = optimizer.optimize(
                fixedTree.field0,
                fixedTree.field0.getTraitSet().plus(WayangConvention.INSTANCE),
                rules);

        PrintUtils.print("Logical WayangPlan", wayangRel);

        // utils for helping calcite with converting calcites intermediate table
        // representation
        // aliases to the trueform sql alias
        final TableScanVisitor visitor = new TableScanVisitor(new ArrayList<>(), null);
        visitor.visit(fixedTree.field0, 0, null);
        final AliasFinder aliasFinder = new AliasFinder(visitor);

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

    /**
     * Traverses and collects all RelNodes from root
     * 
     * @param root starting point
     * @return list of all traversed relNodes
     */
    private List<RelNode> traverseAndCollect(final RelNode root) {
        final LinkedList<RelNode> list = new LinkedList<>();
        final List<RelNode> allRels = new ArrayList<>();
        list.add(root);

        while (list.size() > 0) {
            final RelNode cur = list.pop();
            list.addAll(cur.getInputs());
            allRels.add(cur);
        }

        return allRels;
    }

    private void registerCheck(final RelNode relNode) {
        System.out.println("Starting register check: ####");
        final LinkedList<RelNode> list = new LinkedList<>();
        final List<RelNode> allRels = new ArrayList<>();
        list.add(relNode);
        while (list.size() > 0) {
            final RelNode cur = list.pop();
            final List<RelNode> inputs = cur.getInputs();
            list.addAll(inputs);
            allRels.add(cur);
        }

        System.out.println("planner: " + allRels.get(0).getCluster().getPlanner());
        System.out.println("agg rowtype: " + allRels.get(0).getRowType() + ", node: " + allRels.get(0));
        final Map<RelNode, RelDataType> registeredNodes = allRels.stream().collect(Collectors.toMap(n -> n, n -> {
            System.out.println("looking at rel: " + n);
            System.out.println(n.onRegister(n.getCluster().getPlanner()));
            n.register(n.getCluster().getPlanner());
            return n.getCluster().getPlanner().ensureRegistered(n, null).getRowType();
        }));

        final List<String> integerPrint = allRels.stream()
                .map(rel -> rel + ", fc: " + rel.getRowType().getFieldCount()
                        + rel.getInputs().stream().map(RelNode::getRowType).map(RelDataType::getFieldCount)
                                .collect(Collectors.toList()).toString()
                        + "\n")
                .collect(Collectors.toList());
        System.out.println(integerPrint);
        // System.out.println("all rels :D?: " + allRels);
    }

    private void registerCheck(final RelNode relNode, final HepPlanner planner) {
        System.out.println("Starting register check: ####");
        final LinkedList<RelNode> list = new LinkedList<>();
        final List<RelNode> allRels = new ArrayList<>();
        list.add(relNode);
        while (list.size() > 0) {
            final RelNode cur = list.pop();
            final List<RelNode> inputs = cur.getInputs();
            list.addAll(inputs);
            allRels.add(cur);
        }

        System.out.println("planner: " + allRels.get(0).getCluster().getPlanner());
        System.out.println("agg rowtype: " + allRels.get(0).getRowType() + ", node: " + allRels.get(0));
        final Map<RelNode, RelDataType> registeredNodes = allRels.stream().distinct()
                .collect(Collectors.toMap(n -> n, n -> {
                    return planner.ensureRegistered(n, null).getRowType();
                }));

        System.out.println("registered: " + registeredNodes);
        // RelOptUtil.equal(getJobName(), null, getJobName(), null, null);
        final List<String> matches = allRels.stream()
                .map(rel -> rel + ", ne match: " + RelOptUtil.equal("original rel: ",
                        rel.getRowType(), "ensured rel: ", registeredNodes.get(rel), Litmus.IGNORE) + "\n")
                .collect(Collectors.toList());

        // Lists.reverse(allRels);
        final List<String> integerPrint = allRels.stream()
                .map(rel -> rel + ", fc: " + rel.getRowType().getFieldCount()
                        + rel.getInputs().stream().map(RelNode::getRowType).map(RelDataType::getFieldCount)
                                .collect(Collectors.toList()).toString()
                        + "\n")
                .collect(Collectors.toList());
        System.out.println(integerPrint);
        // System.out.println("all rels :D?: " + allRels);
    }

    private static String getJobName() {
        return "SQL[" + jobId.incrementAndGet() + "]";
    }
}
