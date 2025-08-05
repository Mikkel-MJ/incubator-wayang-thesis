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

package org.apache.wayang.api.sql.calcite.optimizer;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;

import org.apache.wayang.api.sql.calcite.rules.WayangRules;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.api.sql.calcite.converter.WayangRelConverter;
import org.apache.wayang.api.sql.calcite.schema.WayangSchema;
import org.apache.wayang.api.sql.calcite.utils.AliasFinder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.Properties;

public class Optimizer {

    private final CalciteConnectionConfig config;
    private final SqlValidator sqlValidator;
    private final SqlToRelConverter sqlToRelConverter;
    private final VolcanoPlanner volcanoPlanner;
    private static RelOptCluster cluster;
    private static RelOptSchema relOptSchema;
    private static Schema schema;
    private static RelDataTypeFactory relDataTypeFactory;

    /**
     * Rule {@link JoinCommuteRule} takes too long when joins number grows. We disable this rule if query has joins
     * count bigger than this value.
     */
    public static final int MAX_JOINS_TO_COMMUTE = 999;

    public VolcanoPlanner getPlanner(){
        return this.volcanoPlanner;
    }
    public static Schema getCalciteSchema(){
        return schema;
    }

    public static RelOptSchema getRelOptSchema() {
        return relOptSchema;
    }

    public static RelOptCluster getCluster() {
        return cluster;
    }

    public static RelDataTypeFactory getTypeFactory(){
        return relDataTypeFactory;
    }

    protected Optimizer(
            CalciteConnectionConfig config,
            SqlValidator sqlValidator,
            SqlToRelConverter sqlToRelConverter,
            VolcanoPlanner volcanoPlanner) {
        this.config = config;
        this.sqlValidator = sqlValidator;
        this.sqlToRelConverter = sqlToRelConverter;
        this.volcanoPlanner = volcanoPlanner;
    }

    public static Optimizer create(
            CalciteSchema calciteSchema,
            Properties configProperties,
            RelDataTypeFactory typeFactory) {
        relDataTypeFactory = typeFactory;
        schema = calciteSchema.schema;
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                calciteSchema.root(),
                ImmutableList.of(calciteSchema.name),
                typeFactory,
                config);

        relOptSchema = catalogReader; //set the reloptschema for serialisation access

        SqlOperatorTable operatorTable = SqlOperatorTables.chain(ImmutableList.of(SqlStdOperatorTable.instance()));

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);

        SqlValidator validator = SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory,
                validatorConfig);

        VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));

        // Set up the trait def (mandatory in VolcanoPlanner)
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.setNoneConventionHasInfiniteCost(true);
        //planner.setTopDownOpt(true);

        // Add some core rules
        //planner.addRule(CoreRules.FILTER_INTO_JOIN);

        // Create an empty trait set
        RelTraitSet traitSet = planner.emptyTraitSet().replace(Convention.NONE);

        cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);

        SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig);

        return new Optimizer(config, validator, converter, planner);
    }

    // To remove
    /**
     *
     * @param wayangSchema
     * @return
     * @deprecated Use
     *             {@link #create(CalciteSchema, Properties, RelDataTypeFactory)}
     *             instead
     */
    @Deprecated
    public static Optimizer create(WayangSchema wayangSchema) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        relDataTypeFactory = typeFactory;
        // Configuration
        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(wayangSchema.getSchemaName(), wayangSchema);

        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema,
                Collections.singletonList(wayangSchema.getSchemaName()),
                typeFactory,
                config
        );

        SqlOperatorTable operatorTable = SqlOperatorTables.chain(ImmutableList.of(SqlStdOperatorTable.instance()));

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);

        SqlValidator validator = SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory,
                validatorConfig);

        VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRule(FilterIntoJoinRuleConfig.DEFAULT.toRule());

        cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);


        SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig);

        return new Optimizer(config, validator, converter, planner);
    }

    public SqlNode parseSql(String sql) throws SqlParseException {
        SqlParser.Config parserConfig = SqlParser.config()
                .withCaseSensitive(config.caseSensitive())
                .withQuotedCasing(config.quotedCasing())
                .withUnquotedCasing(config.unquotedCasing())
                .withConformance(config.conformance());

        SqlParser parser = SqlParser.create(sql, parserConfig);

        return parser.parseStmt();
    }

    public SqlNode validate(SqlNode sqlNode) {
        return sqlValidator.validate(sqlNode);
    }

    public RelNode convert(SqlNode sqlNode) {
        RelRoot root = sqlToRelConverter.convertQuery(sqlNode, false, true);
        return root.rel;
    }

    // TODO: create a basic ruleset
    public RelNode optimize(RelNode node, RelTraitSet requiredTraitSet, RuleSet rules) {
        int joinsCnt = RelOptUtil.countJoins(node);

        if (joinsCnt > MAX_JOINS_TO_COMMUTE) {

            rules = RuleSets.ofList(
                StreamSupport.stream(rules.spliterator(), false)
                .filter(rule -> rule != CoreRules.JOIN_ASSOCIATE)
                .collect(Collectors.toList())
            );
        }

        //Program program = Programs.of(RuleSets.ofList(rules));
        Program logicalRules = Programs.heuristicJoinOrder(
            rules,
            false,
            6
        );

        return logicalRules.run(
                volcanoPlanner,
                node,
                requiredTraitSet,
                Collections.emptyList(),
                Collections.emptyList());
    }

    public WayangPlan convert(RelNode relNode) {
        return convert(relNode, new ArrayList<>(), null);
    }

    public WayangPlan convert(RelNode relNode, Collection<Record> collector, AliasFinder aliasFinder) {
        //LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(collector, Record.class);
        LocalCallbackSink<Record> sink = LocalCallbackSink.createStdoutSink(Record.class);
        Operator op = new WayangRelConverter().convert(relNode, aliasFinder);

        op.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }

    public static RelOptCluster createCluster() {
        // Recreate Cluster because serialization is hard uwu
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        //planner.setTopDownOpt(true);

        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        return cluster;
    }

    public static RelDataTypeFactory createTypeFactory() {
        return new JavaTypeFactoryImpl();
    }

    public static class ConfigProperties {

        public static Properties getDefaults() {
            Properties configProperties = new Properties();
            configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
            configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
            configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
            return configProperties;
        }
    }

    /**
     * This method prepares the RelNode tree for Wayang as there are certain
     * operations
     * that Wayang cannot yet handle, this includes
     * {@link WayangMultiConditionJoinSplitRule}
     *
     * @param node
     * @param rules
     * @return
     */
    public RelNode prepare(final RelNode node, Collection<RelOptRule> rules) {
        // use hep as it doesnt take cost into consideration while volcano planner does.
        final HepProgramBuilder programBuilder = HepProgram.builder();
        //programBuilder.addRuleInstance(rule);
        programBuilder.addRuleCollection(rules);

        final HepProgram program = programBuilder.build();
        final HepPlanner planner = new HepPlanner(program);
        planner.setRoot(node);

        return planner.findBestExp();
    }
}
