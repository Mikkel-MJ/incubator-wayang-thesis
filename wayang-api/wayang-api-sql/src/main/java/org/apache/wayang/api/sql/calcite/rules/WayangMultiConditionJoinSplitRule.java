package org.apache.wayang.api.sql.calcite.rules;

import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.wayang.api.sql.calcite.utils.CalciteSources;

import org.immutables.value.Value;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This rule splits joins with multiple conditions into several binary joins
 * i.e.
 * {@code WayangJoin(condition=[AND(=($1,$2), =($1,$3), =($4,$1))]},
 * into three joins encompassing {@code =($1,$2)},
 * {@code =($1,$3)} and
 * {@code =($4,$1)}
 * this is inefficient as it forces extra table scans on {@code $1} but is in
 * place
 * as long as we dont support multiconditional joins in Wayang
 */
@Value.Enclosing
public class WayangMultiConditionJoinSplitRule extends RelRule<WayangMultiConditionJoinSplitRule.Config>
        implements TransformationRule {

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableWayangMultiConditionJoinSplitRule.Config.builder()
                .operandSupplier(b0 -> b0
                        .operand(Join.class)
                        .predicate(join -> join.getCondition().isA(SqlKind.AND))
                        .anyInputs())
                .build();

        @Override
        default WayangMultiConditionJoinSplitRule toRule() {
            return new WayangMultiConditionJoinSplitRule(this);
        }
    }

    protected WayangMultiConditionJoinSplitRule(final Config config) {
        super(config);
    }

    @Override
    public void onMatch(final RelOptRuleCall call) {
        final Join join = call.rel(0);
        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final RexCall condition = (RexCall) join.getCondition();

        // fetch operands and eagerly cast as this is a join operator based on an AND
        final List<RexCall> operands = condition.getOperands().stream()
                .map(RexCall.class::cast)
                .collect(Collectors.toList());

        RelNode left = join.getLeft();
        RelNode right = join.getRight();

        if (left instanceof RelSubset) {
            left = ((RelSubset) left).getBestOrOriginal();
        }

        if (right instanceof RelSubset) {
            right = ((RelSubset) right).getBestOrOriginal();
        }


        // Find the Join in the tree that is a literal [condition=true]
        // that can be used to join the left and right table here
        // Skip first operand as it will be used for this joins condition
        List<RelNode> children = this.traverseAndCollect(join);

        final LinkedList<RelNode> crossJoins = children.stream().filter(input -> {
            if (input instanceof LogicalJoin)  {
                final LogicalJoin inputJoin = (LogicalJoin) input;

                return inputJoin.getCondition().isA(SqlKind.LITERAL);
            }

            if (input instanceof RelSubset) {
                RelSubset subset = (RelSubset) input;

                RelNode original = subset.getBestOrOriginal();

                if (original instanceof LogicalJoin)  {
                    final LogicalJoin inputJoin = (LogicalJoin) original;

                    return inputJoin.getCondition().isA(SqlKind.LITERAL);
                }
            }

            return false;
        }).map(input -> {
                if (input instanceof RelSubset) {
                    return ((RelSubset) input).getBestOrOriginal();
                }

                return input;
        }).collect(Collectors.toCollection(LinkedList::new));

        if (crossJoins.size() == 0) {
            return;
        }

        for (RexCall operand : operands.subList(1, operands.size())) {

            if (crossJoins.size() == 0) {
                return;
            }

            // Just pick the first cross join available
            RelNode candidate = crossJoins.poll();


            if (candidate == null) {
                continue;
            }
        }

        /*
        call.transformTo(lastBinaryJoin);
        */
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
            RelNode cur = list.pop();

            if (cur instanceof RelSubset) {
                RelSubset subset = (RelSubset) cur;
                cur = subset.getBestOrOriginal();
            }

            list.addAll(cur.getInputs());
            allRels.add(cur);
        }

        return allRels;
    }
}
