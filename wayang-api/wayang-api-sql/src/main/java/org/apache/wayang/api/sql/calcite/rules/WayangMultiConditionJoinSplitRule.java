package org.apache.wayang.api.sql.calcite.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
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
        System.out.println("matched split rule with: " + call);
        final Join join = call.rel(0);
        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final RexCall condition = (RexCall) join.getCondition();

        // fetch operands and eagerly cast as this is a join operator based on an AND
        final List<RexCall> operands = condition.getOperands().stream()
                .map(RexCall.class::cast)
                .collect(Collectors.toList());

        // verify that the join condition is valid in regards to types:
        // assert(operands.stream().allMatch(op ->
        // op.getOperands().get(0).getType().getFullTypeString().equals(op.getOperands().get(1).getType().getFullTypeString())));

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

        System.out.println("join condition: " + join.getCondition());
        System.out.println("join left: " + join.getLeft());
        System.out.println("join right: " + join.getRight());
        System.out.println("join row type: " + join.getRowType());

        System.out.println("first bin join condition: " + firstBinaryJoin.getCondition());
        System.out.println("first bin join left: " + firstBinaryJoin.getLeft());
        System.out.println("first bin join right: " + firstBinaryJoin.getRight());
        System.out.println("first bin join row type: " + firstBinaryJoin.getRowType());

        assert (join.getRight() instanceof HepRelVertex
                ? !(((HepRelVertex) join.getRight()).getCurrentRel() instanceof Join)
                : !(join.getRight() instanceof Join))
                : "Expected the right input of the join not be another join, but got "
                        + (join.getRight() instanceof HepRelVertex
                                ? ((HepRelVertex) join.getRight()).getCurrentRel()
                                : join.getRight());

        System.out.println("first binary join field count: " + join.getRowType().getFieldCount());
        System.out.println(
                "final offset should probably be: "
                        + (operands.size() * join.getRight().getRowType().getFieldCount()));

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
                    final int operatorNumber = operands.indexOf(op); // since for each operator we
                                                                     // add an extra table
                    final int offset = join.getRight().getRowType().getFieldCount()
                            * operatorNumber;

                    System.out.println("calculated offset: " + offset + ", for operator number: "
                            + operatorNumber);

                    System.out.println("expected rowcount: " + (join.getRowType().getFieldCount() + offset));

                    final RexCall offsetOperand = keys.get(0) > keys.get(1)
                            ? (RexCall) rexBuilder.makeCall(
                                    op.getOperator(),
                                    rexBuilder.makeInputRef(
                                            inputRefs.get(0).getType(),
                                            keys.get(0) + offset),
                                    rexBuilder.makeInputRef(
                                            inputRefs.get(1).getType(),
                                            keys.get(1)))
                            : (RexCall) rexBuilder.makeCall(
                                    op.getOperator(),
                                    rexBuilder.makeInputRef(
                                            inputRefs.get(0).getType(),
                                            keys.get(0)),
                                    rexBuilder.makeInputRef(
                                            inputRefs.get(1).getType(),
                                            keys.get(1) + offset));

                    System.out.println("left field count: " + (join.getRowType().getFieldCount()
                            + join.getRight().getRowType().getFieldCount() * (operatorNumber - 1)));

                    System.out.println("final field count: " + ((join.getRowType().getFieldCount()
                            + join.getRight().getRowType().getFieldCount() * (operatorNumber - 1))
                            + join.getRight().getRowType().getFieldCount()));

                    System.out.println("offset operand: " + offsetOperand);

                    System.out.println("use left stmnt (" + keys.get(0) + " > " + keys.get(1)
                            + ")?: "
                            + (keys.get(0) > keys.get(1)) + ", derived\n\tleft stmnt: "
                            + (keys.get(0) + offset) + ", "
                            + keys.get(1) + " right stmnt: " + keys.get(0) + ", "
                            + (keys.get(1) + offset));
                    System.out.println("types of operand: "
                            + offsetOperand.getOperands().stream().map(RexNode::getType)
                                    .map(RelDataType::getFullTypeString)
                                    .collect(Collectors.toList()));

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

                    final BinaryJoin cur = this.createNextJoin(prev, offsetOperand,
                            join.getRight());
                    cur.operandNr = operatorNumber;
                    System.out.println("setting operator number: " + operatorNumber);
                    joinsInChain.add(cur);

                    assert (this.checkBounds(offsetOperand, operatorNumber, join));

                    return cur;
                },
                (n, n1) -> {
                    throw new UnsupportedOperationException(
                            "combining operator for parallelism is not implemented for this object.");
                });

        // Set the left & right rowtype of the replacing join to be to
        // the calls rowtype so we can trick calcite into thinking the
        // rowtypes are equal
        lastBinaryJoin.setRowType(join.getRowType());

        assert (call.rel(0).getRowType().getFieldCount() == joinsInChain.get(joinsInChain.size() - 1)
                .getRowType()
                .getFieldCount())
                : "Expected same fieldcount from b4 and after, call row fieldcount:\n "
                        + call.rel(0).getRowType().getFieldCount()
                        + "\njoin rowtype:\n "
                        + joinsInChain.get(joinsInChain.size() - 1).getRowType()
                                .getFieldCount();

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
        joinsInChain.stream().forEach(BinaryJoin::setRowType);
        firstBinaryJoin.setRowType();
        lastBinaryJoin.setRowType();
        // lastBinaryJoin.setRowType();
        // System.out.println("derive rowtype: " +
        // lastBinaryJoin.deriveRowType().getFieldCount());

        System.out.println("last binary explained: " + lastBinaryJoin.explain());
        assert (this.checkConditionValidity(firstBinaryJoin));
        assert (joinsInChain.stream().allMatch(this::checkConditionValidity));
        assert (this.checkRexInputRefNames(operands, join, joinsInChain, firstBinaryJoin));
        assert (WayangRules.ensureConditionAndInputRefTypeParity(lastBinaryJoin));
        assert (joinsInChain.stream().allMatch(WayangRules::ensureConditionAndInputRefTypeParity));
        assert (checkRexInputRefTypes(operands, joinsInChain, firstBinaryJoin))
                : "Expected the RexInputRef's type string to be the same before and after rule.";
    }

    /**
     * Checks that the operands indexes actually refers to the range of rows within
     * the join
     * 
     * @param condition
     * @param currentOperatorNumber
     * @return
     */
    boolean checkBounds(final RexCall condition, final int currentOperatorNumber, final Join initialJoin) {
        // keys created in the new join condition
        final List<Integer> keys = condition.operands.stream()
                .map(RexInputRef.class::cast)
                .map(RexInputRef::getIndex)
                .collect(Collectors.toList());

        // the amount of cols that will be joined additionally on every binary join
        // due to splitting the multicondition
        final int incomingCols = initialJoin.getRight().getRowType().getFieldCount() * currentOperatorNumber - 1;

        // the amount of columns in the initial left join
        final int baseCols = initialJoin.getLeft().getRowType().getFieldCount();

        // the amount of columns in the left input of the current join
        final int leftCols = incomingCols + baseCols;

        System.out.println("dumping bounds check");
        System.out.println(keys);
        System.out.println(incomingCols);
        System.out.println(baseCols);
        System.out.println(leftCols);

        final boolean check = keys.get(0) > keys.get(1)
                ? ((keys.get(0) > leftCols) && (keys.get(0) < leftCols + initialJoin.getRowType().getFieldCount()))
                : ((keys.get(1) > leftCols) && (keys.get(1) < leftCols + initialJoin.getRowType().getFieldCount()));
        return check;
    }

    /*
     * Checking whether the new conditions given the offset can fetch the correct
     * columns
     */
    boolean checkConditionValidity(final BinaryJoin join) {
        final RexCall condition = (RexCall) join.getCondition();
        final RelNode left = join.getLeft();
        final RelNode right = join.getRight();
        final int leftFieldCount = left.getRowType().getFieldCount();
        final int rightFieldConut = right.getRowType().getFieldCount();

        final List<Integer> keys = condition.getOperands().stream().map(RexInputRef.class::cast)
                .map(RexInputRef::getIndex).collect(Collectors.toList());
        final int leftKey = keys.get(0);
        final int rightKey = keys.get(1);

        System.out.println("condition: " + condition);
        System.out.println("left: " + left);
        System.out.println("right: " + right);
        System.out.println("lfc: " + leftFieldCount);
        System.out.println("rfc: " + rightFieldConut);
        System.out.println("keys: " + keys);
        System.out.println("left key: " + leftKey);
        System.out.println("right key: " + rightKey);

        if (leftKey < rightKey) { // left key can get from left table
            final boolean canGetLeft = leftFieldCount >= leftKey;
            final boolean canGetRight = (rightKey - leftFieldCount) <= rightFieldConut;
            System.out.println(
                    "left < right, can get left: " + canGetLeft + ", canGetRight: " + canGetRight);
            return canGetLeft && canGetRight;
        } else { // right key get from left table
            final boolean canGetLeft = leftFieldCount >= rightKey;
            final boolean canGetRight = (leftKey - leftFieldCount) <= rightFieldConut;
            System.out.println(
                    "left > right, can get left: " + canGetLeft + ", canGetRight: " + canGetRight);
            return canGetLeft && canGetRight;
        }
    }

    /**
     * Verifies that the RexInputRef output types of the old multicondition join
     * is the same as the output types of the new joins
     * 
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
     * Verifies that the RexInputRef output types of the old multicondition join
     * is the same as the output types of the new joins
     * 
     * @param operands
     * @param joins
     * @param firstBinaryJoin
     * @return
     */
    boolean checkRexInputRefNames(final List<RexCall> operands, final Join oldJoin, final List<BinaryJoin> joins,
            final BinaryJoin firstBinaryJoin) {
        // make sure we have all binary joins so we can fetch all new rexinputrefs
        final List<BinaryJoin> joinsInChain = new ArrayList<>();
        joinsInChain.add(firstBinaryJoin);
        joinsInChain.addAll(joins);

        final List<String> oldRefs = operands.stream()
                .map(RexCall::getOperands)
                .flatMap(List::stream)
                .map(RexInputRef.class::cast)
                .map(RexInputRef::getIndex)
                .map(index -> oldJoin.getRowType().getFieldList().get(index))
                .map(RelDataTypeField::toString)
                .map(str -> str.replaceAll("[0-9]", ""))
                .collect(Collectors.toList());

        final List<String> newRefs = joinsInChain.stream()
                .map(BinaryJoin::getCondition)
                .map(RexCall.class::cast)
                .map(RexCall::getOperands)
                .flatMap(List::stream)
                .map(RexInputRef.class::cast)
                .map(RexInputRef::getIndex)
                .map(index -> joinsInChain.get(joinsInChain.size() - 1).getRowType().getFieldList()
                        .get(index))
                .map(RelDataTypeField::toString)
                .map(str -> str.replaceAll("[0-9]", ""))
                .collect(Collectors.toList());

        System.out.println("_________");
        System.out.println(oldRefs);
        System.out.println(newRefs);

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
        public int operandNr = 0;

        protected BinaryJoin(final RelOptCluster cluster, final RelTraitSet traitSet, final List<RelHint> hints,
                final RelNode left,
                final RelNode right, final RexNode condition, final Set<CorrelationId> variablesSet,
                final JoinRelType joinType) {
            super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
            // TODO Auto-generated constructor stub
        }

        @Override
        public RelNode onRegister(final RelOptPlanner planner) {
            System.out.println("registering this: " + this);
            final List<RelNode> oldInputs = getInputs();
            final List<RelNode> inputs = new ArrayList<>(oldInputs.size());

            for (final RelNode input : oldInputs) {
                final RelNode e = planner.ensureRegistered(input, null);
                this.setRowType();

                System.out.println("this operator numbeR: " + this.operandNr);
                System.out.println("this: " + this + " this fc: " + this.getRowType().getFieldCount()
                        + ", input fc: "
                        + input.getRowType().getFieldCount() + ", ensured fc: "
                        + e.getRowType().getFieldCount());

                assert e == input || RelOptUtil.equal(
                        "Nodes:\n\tthis: " + this + "\n\tinput: " + input + "\n\tensured: " + e
                                + "\nrowtype of rel before registration",
                        input.getRowType(),
                        "rowtype of rel after registration",
                        e.getRowType(),
                        Litmus.THROW);
                inputs.add(e);
            }
            RelNode r = this;
            if (!Util.equalShallow(oldInputs, inputs)) {
                r = copy(getTraitSet(), inputs);
            }
            r.recomputeDigest();
            assert r.isValid(Litmus.THROW, null);
            return r;
        }

        /*
         * optimistically gets the row type may be null
         */
        public RelDataType optGetRowType() {
            return this.rowType;
        }

        public void setRowType(final RelDataType relDataType) {
            this.rowType = relDataType;
        }

        public void setRowType() {
            this.rowType = null;
        }

        @Override
        public Join copy(final RelTraitSet traitSet, final RexNode conditionExpr, final RelNode left,
                final RelNode right, final JoinRelType joinType,
                final boolean semiJoinDone) {

            this.setRowType();
            System.out.println("being copied uwu " + this.operandNr);
            System.out.println(this.getLeft().getRowType().getFieldCount());
            System.out.println(this.getRight().getRowType().getFieldCount());
            System.out.println(this.getRowType().getFieldCount());
            assert (this.getLeft().getRowType().getFieldCount()
                    + this.getRight().getRowType().getFieldCount() == this
                            .getRowType().getFieldCount());
            return new BinaryJoin(this.getCluster(), traitSet, this.getHints(), left, right, conditionExpr,
                    this.getVariablesSet(), joinType);
        }
    }
}
