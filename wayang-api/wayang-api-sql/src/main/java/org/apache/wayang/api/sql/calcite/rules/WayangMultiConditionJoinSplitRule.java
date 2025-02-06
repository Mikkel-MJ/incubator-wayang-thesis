package org.apache.wayang.api.sql.calcite.rules;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import org.immutables.value.Value;

/**
 * This rule splits joins with multiple conditions into several joins
 * i.e. {@code WayangJoin(condition=[AND(=($23, $3), =($21, $5), =($22, $0))]},
 * into three joins encompassing {@code =($23, $3)}, {@code =($21, $5)} and
 * {@code =($22, $0)}
 */
@Value.Enclosing
public class WayangMultiConditionJoinSplitRule extends RelRule<WayangMultiConditionJoinSplitRule.Config> {

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableWayangMultiConditionJoinSplitRule.Config.builder()
                .operandSupplier(b0 -> b0
                        .operand(LogicalJoin.class)
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
    public boolean matches(final RelOptRuleCall call) {
        System.out.println("matches: " + call);
        System.out.println("conv: " + super.getOutConvention());
        System.out.println("trait: " + super.getOutTrait());
        return super.matches(call);
    }

    @Override
    public void onMatch(final RelOptRuleCall call) {
        System.out.println("On match: " + call);
        final LogicalJoin join = call.rel(0);

        final RelBuilder relBuilder = call.builder();
        final RexCall condition = (RexCall) join.getCondition();
        final List<RexNode> operands = condition.getOperands();

        // push the two inputs of the base join, so we can use them for the first join
        // in the chain
        relBuilder.push(join.getLeft());
        relBuilder.push(join.getRight());

        // for each operand =($23, $3) in AND (=($23, $3), =($21, $5), =($22,
        // $0)),joinType=inner)
        // we create the join and push the right base input to each condition
        final List<LogicalJoin> joinChain = operands.stream().map(op -> {
            final LogicalJoin link = (LogicalJoin) relBuilder
                    .join(join.getJoinType(), op, join.getVariablesSet())
                    .build();
            relBuilder.push(join.getLeft());
            relBuilder.push(link);
            return link;
        }).collect(Collectors.toList());

        System.out.println("join chain: " + joinChain);

        relBuilder.build(); // pop the last extraneous join.getRight();
        final LogicalJoin toTransform = (LogicalJoin) joinChain.get(0);
        System.out.println("call pre transform: " + call.getRelList() + ", attempting to transform with node: " + toTransform);
        call.transformTo(toTransform);
        System.out.println("call post transform: " + call.getRelList());
    }
}
