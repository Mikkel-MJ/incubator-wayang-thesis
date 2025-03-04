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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import org.immutables.value.Value;

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
        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final RexCall condition = (RexCall) join.getCondition();

        // fetch operands and eagerly cast as this is a join operator based on an AND
        final List<RexCall> operands = condition.getOperands().stream()
                .map(RexCall.class::cast)
                .collect(Collectors.toList());

        //verify that the join condition is valid in regards to types:
        //assert(operands.stream().allMatch(op -> op.getOperands().get(0).getType().getFullTypeString().equals(op.getOperands().get(1).getType().getFullTypeString())));

        // push the two inputs of the base join, so we can use them for the first join
        // in the chain
        final BinaryJoin firstBinaryJoin = new BinaryJoin(
                join.getCluster(),
                join.getTraitSet(),
                join.getHints(),
                join.getLeft(),
                join.getRight(),
                operands.get(0),
                join.getVariablesSet(),
                join.getJoinType());

        final ArrayList<BinaryJoin> joinsInChain = new ArrayList<>();

        final BinaryJoin lastBinaryJoin = operands.stream().sequential().skip(1).reduce(
                firstBinaryJoin,
                (prev, op) -> {
                    // the following inputRef handling is needed because we create
                    // a new condition with new inputRef indexes based of how many new
                    // right tables we add.
                    // we make a couple of assumptions.
                    // 1. in a multi condition join, if the result set of the multicondition
                    // join is created by {leftInputTable, rightInputTable}
                    // then we know that the lesser inputRef index is always refering to a column
                    // in the left table.

                    // fetch columns referenced in the join condition:
                    final List<RexInputRef> inputRefs = op.getOperands().stream()
                            .map(RexInputRef.class::cast)
                            .collect(Collectors.toList());

                    // fetch the keys of the columns so we can offset them and construct a new
                    // operand l8r
                    final List<Integer> keys = inputRefs.stream()
                            .map(RexInputRef::getIndex)
                            .collect(Collectors.toList());

                    // we need to offset the operand by the extra tables we are now adding to the
                    // catalogue
                    final int operatorNumber = operands.indexOf(op); // since for each operator we add an extra table
                    final int offset = join.getRight().getRowType().getFieldCount() * operatorNumber;

                    final RexCall offsetOperand = keys.get(0) > keys.get(1)
                            ? (RexCall) rexBuilder.makeCall(
                                    op.getOperator(),
                                    rexBuilder.makeInputRef(inputRefs.get(0).getType(), keys.get(0) + offset),
                                    rexBuilder.makeInputRef(inputRefs.get(1).getType(), keys.get(1)))
                            : (RexCall) rexBuilder.makeCall(
                                    op.getOperator(),
                                    rexBuilder.makeInputRef(inputRefs.get(0).getType(), keys.get(0)),
                                    rexBuilder.makeInputRef(inputRefs.get(1).getType(), keys.get(1) + offset));

                    System.out.println("types of operand: " + offsetOperand.getOperands().stream().map(RexNode::getType).map(RelDataType::getFullTypeString).collect(Collectors.toList()));

                    /*
                    assert (offsetOperand.getOperands().get(0).getType().getFullTypeString().equals(offsetOperand.getOperands().get(1).getType().getFullTypeString())) :
                        "Only allow joins on the same row type in condition, got left: " + offsetOperand.getOperands().get(0).getType().getFullTypeString() + ", right: " + offsetOperand.getOperands().get(1).getType().getFullTypeString();
                    */
                    // check that the typing of the output of the rule is the same as the original
                    // operand
                    assert (offsetOperand.getOperands().stream()
                            .map(RexInputRef.class::cast)
                            .map(ref -> ref.getType().getFullTypeString())
                            .collect(Collectors.toList())
                            .equals(op.getOperands().stream()
                                    .map(RexInputRef.class::cast)
                                    .map(ref -> ref.getType().getFullTypeString())
                                    .collect(Collectors.toList())))
                            : "RexInputRefs in new join changed types from old operand";

                    final BinaryJoin cur = this.createNextJoin(prev, offsetOperand, join.getRight());
                    // System.out.println("cur rows: " + cur.getRowType().getFieldCount());
                    joinsInChain.add(cur);
                    return cur;
                },
                (n, n1) -> {
                    throw new UnsupportedOperationException(
                            "combining operator for parallelism is not implemented for this object.");
                });

        // Set the left & right rowtype of the replacing join to be to
        // the calls rowtype so we can trick calcite into thinking the
        // rowtypes are equal
        lastBinaryJoin.left = join.getLeft().getRowType();
        lastBinaryJoin.right = join.getRight().getRowType();

        assert (call.rel(0).getRowType().getFieldCount() == joinsInChain.get(joinsInChain.size() - 1).getRowType()
                .getFieldCount())
                : "Expected same fieldcount from b4 and after, call row fieldcount:\n "
                        + call.rel(0).getRowType().getFieldCount()
                        + "\njoin rowtype:\n "
                        + joinsInChain.get(joinsInChain.size() - 1).getRowType().getFieldCount();

        assert (checkRexInputRefTypes(operands, joinsInChain, firstBinaryJoin))
                : "Expected the RexInputRef's type string to be the same before and after rule.";

        call.transformTo(lastBinaryJoin);

        assert (joinsInChain.size() + 1 == operands.size())
                : "Expected the amount of created binary joins to be equal to the amount of joins in the multiconditional join, got: "
                        + joinsInChain.size() + 1 + ", expected: " + operands.size();

        // After we have called transform we mutate the row types back to the proper
        // type
        // mutate left and right back so calcite will derive the rowtype based on the
        // left and right input ref in the node
        lastBinaryJoin.left = null;
        lastBinaryJoin.right = null;
        System.out.println("derive rowtype: " + lastBinaryJoin.deriveRowType().getFieldCount());
        assert (WayangRules.ensureConditionAndInputRefTypeParity(lastBinaryJoin));
        assert (joinsInChain.stream().allMatch(WayangRules::ensureConditionAndInputRefTypeParity));

        assert (checkRexInputRefTypes(operands, joinsInChain, firstBinaryJoin))
        : "Expected the RexInputRef's type string to be the same before and after rule.";
    }

    /**
     * Verifies that the RexInputRef output types of the old multicondition join
     * is the same as the output types of the new joins
     * @param operands
     * @param joins
     * @param firstBinaryJoin
     * @return
     */
    boolean checkRexInputRefTypes(final List<RexCall> operands, final List<BinaryJoin> joins,
            final BinaryJoin firstBinaryJoin) {
        // make sure we have all binary joins so we can fetch all new rexinputrefs
        final List<BinaryJoin> joinsInChain = new ArrayList<>();
        joinsInChain.add(firstBinaryJoin);
        joinsInChain.addAll(joins);

        final List<String> oldRefs = operands.stream()
                .map(RexCall::getOperands)
                .flatMap(List::stream)
                .map(RexInputRef.class::cast)
                .map(RexInputRef::getType)
                .map(RelDataType::getFullTypeString)
                .collect(Collectors.toList());

        final List<String> newRefs = joinsInChain.stream()
                .map(BinaryJoin::getCondition)
                .map(RexCall.class::cast)
                .map(RexCall::getOperands)
                .flatMap(List::stream)
                .map(RexInputRef.class::cast)
                .map(RexInputRef::getType)
                .map(RelDataType::getFullTypeString)
                .collect(Collectors.toList());

        return newRefs.equals(oldRefs);
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
    BinaryJoin createNextJoin(final BinaryJoin previousJoinInChain, final RexNode operand,
            final RelNode joiningTable) {
        return new BinaryJoin(previousJoinInChain.getCluster(),
                previousJoinInChain.getTraitSet(),
                previousJoinInChain.getHints(),
                previousJoinInChain,
                joiningTable,
                operand,
                previousJoinInChain.getVariablesSet(),
                previousJoinInChain.getJoinType());
    }

    /**
     * This object exists to trick calcites row check on .transformTo
     * since calcite requires the row type of the old relnode and new relnode
     * during transformTo to be equal; we expose the deriveRowType so we can mutate
     * it
     * after the transformTo call
     */
    public class BinaryJoin extends Join {
        public RelDataType left;
        public RelDataType right;

        protected BinaryJoin(final RelOptCluster cluster, final RelTraitSet traitSet, final List<RelHint> hints,
                final RelNode left,
                final RelNode right, final RexNode condition, final Set<CorrelationId> variablesSet,
                final JoinRelType joinType) {
            super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
            // TODO Auto-generated constructor stub
        }

        @Override
        public RelDataType deriveRowType() {
            if (left != null && right != null) {
                final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
                final RelDataType customType = typeFactory.createJoinType(left, right);
                //this.rowType = customType;
                return customType;
            }
            //this.rowType = super.deriveRowType();
            return super.deriveRowType();
        }

        public void setRowType(RelDataType relDataType){
            this.rowType = relDataType;
        }

        @Override
        public Join copy(final RelTraitSet traitSet, final RexNode conditionExpr, final RelNode left,
                final RelNode right, final JoinRelType joinType,
                final boolean semiJoinDone) {

            return new BinaryJoin(this.getCluster(), traitSet, this.getHints(), left, right, conditionExpr,
                    this.getVariablesSet(), joinType);
        }
    }
}
