package org.apache.wayang.api.sql.calcite.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.wayang.api.sql.calcite.utils.CalciteSources;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import org.immutables.value.Value;

/**
 * This rule splits joins with multiple conditions into several joins
 * i.e.
 * {@code WayangJoin(condition=[AND(=(, RelTraitSet), =(, List<RelHint>), =(, ))]},
 * into three joins encompassing {@code =(, RelTraitSet)},
 * {@code =(, List<RelHint>)} and
 * {@code =(, )}
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
    public void onMatch(final RelOptRuleCall call) {
        final LogicalJoin join = call.rel(0);
        System.out.println("matched join: " + join);
        System.out.println("join row count: " + join.getRowType().getFieldCount());
        System.out.println("join input left field count: " + join.getLeft().getRowType().getFieldCount());
        System.out.println("join left: " + join.getLeft());
        System.out.println("join input right field count: " + join.getRight().getRowType().getFieldCount());
        System.out.println("join right: " + join.getRight());

        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final RexCall condition = (RexCall) join.getCondition();    
        
        // fetch operands and eagerly cast as this is a join operator based on an AND
        final List<RexCall> operands = condition.getOperands().stream()
            .map(RexCall.class::cast)
            .collect(Collectors.toList());

        // push the two inputs of the base join, so we can use them for the first join
        // in the chain
        final CustomLogicalJoin firstJoinInChain = new CustomLogicalJoin(
                join.getCluster(),
                join.getTraitSet(),
                join.getHints(),
                join.getLeft(),
                join.getRight(), 
                operands.get(0),
                join.getVariablesSet(),
                join.getJoinType());

        final ArrayList<CustomLogicalJoin> joinsInChain = new ArrayList<>();
        final CustomLogicalJoin joino = operands.stream().sequential().skip(1).reduce(
                firstJoinInChain,
                (prev, op) -> {
                    // fetch columns referenced in the join condition:
                    final List<RexInputRef> inputRefs = op.getOperands().stream()
                        .map(RexInputRef.class::cast)
                        .collect(Collectors.toList());

                    // fetch the keys of the columns so we can offset them and construct a new operand l8r
                    final List<Integer> keys = inputRefs.stream()
                        .map(RexInputRef::getIndex)
                        .collect(Collectors.toList());
          
                    // we need to offset the operand by the extra tables we are now adding to the catalogue
                    final int operatorNumber = operands.indexOf(op); // since for each operator we add an extra table
                    final int offset = join.getRight().getRowType().getFieldCount() * operatorNumber;

                    final RexCall offsetOperand = (RexCall) rexBuilder.makeCall(
                        op.getOperator(),
                        rexBuilder.makeInputRef(inputRefs.get(0).getType(), keys.get(0) + offset),
                        rexBuilder.makeInputRef(inputRefs.get(1).getType(), keys.get(1))
                    );

                    System.out.println("offset operand testing: " + offsetOperand);
                    final CustomLogicalJoin cur = this.createNextJoin(prev, offsetOperand, join.getRight());
                    
                    joinsInChain.add(cur);
                    return cur;
                },
                (n, n1) -> {
                    throw new NotImplementedException(
                            "combining operator for parallelism is not implemented for this object.");
                });

        // Set the left & right rowtype of the replacing join to be to
        // the calls rowtype so we can trick calcite into thinking the
        // rowtypes are equal
        joino.left = join.getLeft().getRowType();
        joino.right = join.getRight().getRowType();

        System.out.println("transform to! " + " call row type: " + call.rel(0).getRowType() + " join rowtype: " + joino.getRowType());
        call.transformTo(joino);

        // After we have called transform we mutate the row types back to the proper
        // type
        System.out.println("joins in chain : " + joinsInChain);
        System.out.println("firs tjoin: " + firstJoinInChain);
        System.out.println("operands: " + operands);
        firstJoinInChain.left = null;
        firstJoinInChain.right = null;

        final CustomLogicalJoin joino2 = joinsInChain.stream().sequential().reduce(
                firstJoinInChain,
                (prev, cur) -> {
                    cur.left = null;
                    cur.right = null;
                    return cur;
                });

        System.out.println("base join rows size: " + join.getRowType().getFieldCount());
        System.out.println("joinsinchain size: " + joinsInChain.size());
        List<String> testo = joinsInChain.stream().map(joine -> joine + "rows:\n" + joine.getRowType().toString()).collect(Collectors.toList());
        System.out.println(testo);
        System.out.println(joino2.explain());
    }

    /**
     * Creates a join from a previous join + an operand and makes it so the right
     * input of the join is the previous join
     * 
     * @param previousJoinInChain
     * @param operand
     * @param joiningTable
     * @return
     */
    CustomLogicalJoin createNextJoin(CustomLogicalJoin previousJoinInChain, RexNode operand, RelNode joiningTable) {
        return new CustomLogicalJoin(previousJoinInChain.getCluster(),
                previousJoinInChain.getTraitSet(),
                previousJoinInChain.getHints(),
                previousJoinInChain,
                joiningTable,
                operand,
                previousJoinInChain.getVariablesSet(),
                previousJoinInChain.getJoinType());
    }

    /*
     * @Override
     * public void onMatch(final RelOptRuleCall call) {
     * final LogicalJoin join = call.rel(0);
     * 
     * final RelBuilder relBuilder = call.builder();
     * final RexCall condition = (RexCall) join.getCondition();
     * final List<RexNode> operands = condition.getOperands();
     * 
     * System.out.println("join get left explained: " + join.getLeft().explain());
     * System.out.println("join get right explained: " + join.getRight().explain());
     * 
     * // push the two inputs of the base join, so we can use them for the first
     * join
     * // in the chain
     * relBuilder.push(join.getLeft());
     * relBuilder.push(join.getRight());
     * 
     * 
     * // for each operand =(, RelTraitSet) in AND (=(, RelTraitSet), =(,
     * List<RelHint>), =(,
     * // )),joinType=inner)
     * // we create the join and push the right base input to each condition
     * final List<LogicalJoin> joinChain = operands.stream().map(op -> {
     * final LogicalJoin link = (LogicalJoin) relBuilder
     * .join(join.getJoinType(), op, join.getVariablesSet())
     * .build();
     * relBuilder.push(link);
     * relBuilder.push(join.getRight());
     * 
     * return link;
     * }).collect(Collectors.toList());
     * 
     * //relBuilder.convert(call.rel(0).getRowType(), false);
     * relBuilder.build(); // pop the last extraneous join.getRight();
     * final LogicalJoin toTransform = (LogicalJoin) joinChain.get(joinChain.size()
     * - 1);
     * System.out.println("replaced: " + call.rel(0) + " with " + toTransform +
     * "\n");
     * System.out.println("explain call: " + call.rel(0).explain());
     * System.out.println("explain toTransform: " + toTransform.explain());
     * System.out.println("children: " + toTransform.getLeft() + ", " +
     * toTransform.getRight());
     * System.out.println("origin calL: " +
     * call.rel(0).getRowType().getFieldList().stream().map(field ->
     * CalciteSources.tableNameOriginOf(call.rel(0),
     * field.getIndex())).collect(Collectors.toList()));
     * System.out.println("origin calL: " +
     * toTransform.getRowType().getFieldList().stream().map(field ->
     * CalciteSources.tableNameOriginOf(toTransform,
     * field.getIndex())).collect(Collectors.toList()));
     * call.transformTo(toTransform);
     * }
     */

    /**
     * This object exists to trick calcites row check on .transformTo
     * since calcite requires the row type of the old relnode and new relnode
     * during transformTo to be equal; we expose the deriveRowType so we can mutate it
     * after the transformTo call
     */
    public class CustomLogicalJoin extends Join {
        public RelDataType left;
        public RelDataType right;

        protected CustomLogicalJoin(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelNode left,
                RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
            super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
            // TODO Auto-generated constructor stub
            System.out.println("construction custom join");
        }

        @Override
        public RelDataType deriveRowType() {
            System.out.println("deriving row type, with left: " + left + ", right: " + right);
            if (left != null && right != null) {
                RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
                RelDataType customType = typeFactory.createJoinType(left, right);
                return customType;
            }
            return super.deriveRowType();
        }

        @Override
        public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType,
                boolean semiJoinDone) {

            ArrayList list = new ArrayList<RelHint>();
            return new CustomLogicalJoin(left.getCluster(), traitSet, list, left, right, conditionExpr,
                    left.getVariablesSet(), joinType);
        }
    }
}
