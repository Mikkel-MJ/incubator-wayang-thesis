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

package org.apache.wayang.api.sql.calcite.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexChecker;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Litmus;
import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.rel.WayangFilter;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.api.sql.calcite.rel.WayangProject;
import org.apache.wayang.api.sql.calcite.rel.WayangTableScan;
import org.apache.wayang.api.sql.calcite.rules.WayangMultiConditionJoinSplitRule;
import org.apache.wayang.api.sql.calcite.rel.WayangAggregate;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

//TODO: split into multiple classes
public class WayangRules {

    private WayangRules() {
    }

    public static final RelOptRule WAYANG_JOIN_RULE = new WayangJoinRule(WayangJoinRule.DEFAULT_CONFIG);
    public static final RelOptRule WAYANG_PROJECT_RULE = new WayangProjectRule(WayangProjectRule.DEFAULT_CONFIG);
    public static final RelOptRule WAYANG_FILTER_RULE = new WayangFilterRule(WayangFilterRule.DEFAULT_CONFIG);
    public static final RelOptRule WAYANG_TABLESCAN_RULE = new WayangTableScanRule(WayangTableScanRule.DEFAULT_CONFIG);
    public static final RelOptRule WAYANG_TABLESCAN_ENUMERABLE_RULE = new WayangTableScanRule(
            WayangTableScanRule.ENUMERABLE_CONFIG);
    public static final RelOptRule WAYANG_AGGREGATE_RULE = new WayangAggregateRule(WayangAggregateRule.DEFAULT_CONFIG);

    /**
     * Rule that tries to take a multi conditional join and splits it into multiple
     * binary joins.
     */
    public static final RelOptRule WAYANG_MULTI_CONDITION_JOIN_SPLIT_RULE = new WayangMultiConditionJoinSplitRule(
            WayangMultiConditionJoinSplitRule.Config.DEFAULT);

    private static class WayangProjectRule extends ConverterRule {

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalProject.class,
                        Convention.NONE, WayangConvention.INSTANCE,
                        "WayangProjectRule")
                .withRuleFactory(WayangProjectRule::new);

        protected WayangProjectRule(final Config config) {
            super(config);
        }

        public RelNode convert(final RelNode rel) {
            final LogicalProject project = (LogicalProject) rel;

            return new WayangProject(
                    project.getCluster(),
                    project.getTraitSet().replace(WayangConvention.INSTANCE),
                    convert(project.getInput(), project.getInput().getTraitSet().replace(WayangConvention.INSTANCE)),
                    project.getProjects(),
                    project.getRowType());
        }
    }

    private static class WayangFilterRule extends ConverterRule {

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalFilter.class,
                        Convention.NONE, WayangConvention.INSTANCE,
                        "WayangFilterRule")
                .withRuleFactory(WayangFilterRule::new);

        protected WayangFilterRule(final Config config) {
            super(config);
        }

        @Override
        public RelNode convert(final RelNode rel) {
            final LogicalFilter filter = (LogicalFilter) rel;

            return new WayangFilter(
                    rel.getCluster(),
                    rel.getTraitSet().replace(WayangConvention.INSTANCE),
                    convert(filter.getInput(), filter.getInput().getTraitSet().replace(WayangConvention.INSTANCE)),
                    filter.getCondition());
        }

    }

    private static class WayangTableScanRule extends ConverterRule {

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalTableScan.class,
                        Convention.NONE, WayangConvention.INSTANCE,
                        "WayangTableScanRule")
                .withRuleFactory(WayangTableScanRule::new);

        public static final Config ENUMERABLE_CONFIG = Config.INSTANCE
                .withConversion(TableScan.class,
                        EnumerableConvention.INSTANCE, WayangConvention.INSTANCE,
                        "WayangTableScanRule1")
                .withRuleFactory(WayangTableScanRule::new);

        protected WayangTableScanRule(final Config config) {
            super(config);
        }

        @Override
        public @Nullable RelNode convert(final RelNode relNode) {
            final TableScan scan = (TableScan) relNode;
            final RelOptTable relOptTable = scan.getTable();

            /**
             * This is quick hack to prevent volcano from merging projects on to TableScans
             * TODO: a cleaner way to handle this
             */
            if (relOptTable.getRowType() == scan.getRowType()) {
                return WayangTableScan.create(scan.getCluster(), relOptTable);
            }
            return null;
        }
    }

    private static class WayangJoinRule extends ConverterRule {

         public static final Config DEFAULT_CONFIG = (Config) Config.INSTANCE
                                .withConversion(LogicalJoin.class, Convention.NONE,
                                                WayangConvention.INSTANCE, "WayangJoinRule")
                                .withRuleFactory(WayangJoinRule::new)
                                .withOperandSupplier(b0 -> b0.operand(LogicalJoin.class)
                                        .predicate(join -> !(join.getCondition() instanceof RexLiteral))
                                        .predicate(join -> !join.getCondition().isA(SqlKind.LITERAL))
                                        .predicate(join -> !join.getCondition().isA(SqlKind.AND))
                                        .anyInputs()
                                );

        protected WayangJoinRule(final Config config) {
            super(config);
        }

        @Override
        public @Nullable RelNode convert(final RelNode relNode) {
            final LogicalJoin join = (LogicalJoin) relNode;

            if (join.getCondition().isA(SqlKind.AND)) {
                return join;
            }
                /*
                throw new UnsupportedOperationException(
                        "Multiconditional joins are currently not supported for Wayang conversions.");*/
            final List<RelNode> newInputs = new ArrayList<>();

            for (RelNode input : join.getInputs()) {
                if (!(input.getConvention() instanceof WayangConvention)) {
                    input = convert(input, input.getTraitSet().replace(WayangConvention.INSTANCE));
                }
                newInputs.add(input);
            }

            final WayangJoin newJoin = new WayangJoin(
                    join.getCluster(),
                    join.getTraitSet().replace(WayangConvention.INSTANCE),
                    newInputs.get(0),
                    newInputs.get(1),
                    join.getCondition(),
                    join.getVariablesSet(),
                    join.getJoinType());

            // this should be safe as no multiconditional joins should be available at this
            // time
            /*
            final List<RexInputRef> oldRefs = ((RexCall) join.getCondition()).operands.stream()
                    .map(RexInputRef.class::cast)
                    .collect(Collectors.toList());
            */

            return newJoin;
        }
    }

    /*
     * Ensures that the types within the join condition and in the table representation that
     * calcite has for the for the current join have parity
     */
    public static boolean ensureConditionAndInputRefTypeParity(Join join){
        final RexCall condition = (RexCall) join.getCondition();

        // assuming binary join
        assert(condition.isA(SqlKind.EQUALS)) : "Condition was not a binary: " + join.getCondition();

        final List<Integer> keys = condition.operands.stream()
            .map(RexInputRef.class::cast)
            .map(RexInputRef::getIndex)
            .collect(Collectors.toList());

        final List<String> fieldTypeNames = keys.stream()
            .map(key -> join.getRowType().getFieldList().get(key))
            .map(RelDataTypeField::getType)
            .map(RelDataType::getFullTypeString)
            .collect(Collectors.toList());

        final List<String> refTypeNames = condition.getOperands().stream()
            .map(RexInputRef.class::cast)
            .map(RexInputRef::getType)
            .map(RelDataType::getFullTypeString)
            .collect(Collectors.toList());

        final boolean isParity = fieldTypeNames.equals(refTypeNames);

        if(!isParity) System.err.println("Keys: " + keys + ", fieldTypeNames: " + fieldTypeNames + ", refTypeNames: " + refTypeNames);

        return isParity;
    }

    private static class WayangAggregateRule extends ConverterRule {

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalAggregate.class,
                        Convention.NONE, WayangConvention.INSTANCE,
                        "WayangAggregateRule")
                .withRuleFactory(WayangAggregateRule::new);

        protected WayangAggregateRule(final Config config) {
            super(config);
        }

        @Override
        public @Nullable RelNode convert(final RelNode relNode) {
            final LogicalAggregate aggregate = (LogicalAggregate) relNode;
            final RelNode input = convert(aggregate.getInput(),
                    aggregate.getInput().getTraitSet().replace(WayangConvention.INSTANCE));

            return new WayangAggregate(
                    aggregate.getCluster(),
                    aggregate.getTraitSet().replace(WayangConvention.INSTANCE),
                    aggregate.getHints(),
                    input,
                    aggregate.getGroupSet(),
                    aggregate.getGroupSets(),
                    aggregate.getAggCallList());
        }
    }
}
