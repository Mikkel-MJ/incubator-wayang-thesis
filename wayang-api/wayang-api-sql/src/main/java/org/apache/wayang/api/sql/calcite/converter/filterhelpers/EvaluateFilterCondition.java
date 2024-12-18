package org.apache.wayang.api.sql.calcite.converter.filterhelpers;

import java.io.Serializable;
import java.util.Optional;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlKind;

import org.apache.wayang.api.sql.calcite.converter.WayangFilterVisitor;
import org.apache.wayang.basic.data.Record;

public class EvaluateFilterCondition extends RexVisitorImpl<Boolean> implements Serializable {
    final Record record;

    protected EvaluateFilterCondition(final boolean deep, final Record record) {
        super(deep);
        this.record = record;
    }

    @Override
    public Boolean visitCall(final RexCall call) {
        final SqlKind kind = call.getKind();
                
        if(!kind.belongsTo(WayangFilterVisitor.SUPPORTED_OPS)) throw new IllegalStateException("Cannot handle this filter predicate yet: " + kind + " during RexCall: " + call);
        
        switch(kind){
            case IS_NOT_NULL:
                assert(call.getOperands().size() == 1);
                return eval(record, kind, call.getOperands().get(0), null);
            case IS_NULL:
                assert(call.getOperands().size() == 1);
                return eval(record, kind, call.getOperands().get(0), null);
            case NOT:
                assert(call.getOperands().size() == 1);
                return !(call.getOperands().get(0).accept(this)); //Since NOT captures only one operand we just get the first
            case AND:
                return call.getOperands().stream().allMatch(operator -> operator.accept(this));
            case OR:
                return call.getOperands().stream().anyMatch(operator -> operator.accept(this));
            default:
                assert(call.getOperands().size() == 2);
                return eval(record, kind, call.getOperands().get(0), call.getOperands().get(1));
        }
    }

    public boolean eval(final Record record, final SqlKind kind, final RexNode leftOperand, final RexNode rightOperand) {
        if(leftOperand instanceof RexInputRef && rightOperand instanceof RexLiteral) {
            final RexInputRef rexInputRef = (RexInputRef) leftOperand;
            final int index = rexInputRef.getIndex();
            final Optional<?> field = Optional.ofNullable(record.getField(index));
            final RexLiteral rexLiteral = (RexLiteral) rightOperand;

            switch (kind) {
                case LIKE:
                    return this.like(field, rexLiteral);
                case GREATER_THAN:
                    return isGreaterThan(field, rexLiteral);
                case LESS_THAN:
                    return isLessThan(field, rexLiteral);
                case EQUALS:
                    return isEqualTo(field, rexLiteral);
                case NOT_EQUALS:
                    return !isEqualTo(field, rexLiteral);
                case GREATER_THAN_OR_EQUAL:
                    return isGreaterThan(field, rexLiteral) || isEqualTo(field, rexLiteral);
                case LESS_THAN_OR_EQUAL:
                    return isLessThan(field, rexLiteral) || isEqualTo(field, rexLiteral);
                default:
                    throw new IllegalStateException("Predicate not supported yet, record: " + record + ", SqlKind:" + kind + ", left operand: " + leftOperand + ", right operand: " + rightOperand);
            }
        } else if (leftOperand instanceof RexInputRef && rightOperand instanceof RexInputRef) {  //filters with column a = column b
            final RexInputRef leftRexInputRef = (RexInputRef) leftOperand;
            final int leftIndex = leftRexInputRef.getIndex();
            final RexInputRef righRexInputRef = (RexInputRef) rightOperand;
            final int rightIndex = righRexInputRef.getIndex();

            final Optional<?> leftField = Optional.ofNullable(record.getField(leftIndex));
            final Optional<?> rightField = Optional.ofNullable(record.getField(rightIndex));

            switch (kind) {
                case EQUALS:
                    return isEqualTo(leftField, rightField);
                default:
                    throw new IllegalStateException("Predicate not supported yet, kind: " + kind + " left field: " + leftField + " right field: " + rightField);
            }
        } else if (leftOperand instanceof RexInputRef && rightOperand == null) {
            final RexInputRef leftRexInputRef = (RexInputRef) leftOperand;
            final int leftIndex = leftRexInputRef.getIndex();

            final Optional<?> leftField = Optional.ofNullable(record.getField(leftIndex));
            
            switch (kind) {
                case IS_NOT_NULL:
                    return !isEqualTo(leftField, Optional.empty());
                case IS_NULL:
                    return isEqualTo(leftField, Optional.empty());
                default:
                    throw new IllegalStateException("Predicate not supported yet, kind: " + kind + " left field: " + leftField);
            }
        }
        else {
            throw new IllegalStateException("Predicate not supported yet, kind: " + kind + ", record: " + record + ", left operand: " + leftOperand + ", right operand: " + rightOperand);
        }
    }

    private boolean like(final Optional<?> o, final RexLiteral toCompare) {       
        final String unwrapped = o.map(s -> (String) s).orElse("");
        final boolean isMatch = SqlFunctions.like(unwrapped, toCompare
            .toString()
            .replace("'", "") //the calcite sqlToRegex api needs input w/o 's
        );

        return isMatch;
    }

    /***
     * Checks if object o is greater than the rex literal
     * @param o object of any type including null
     * @param rexLiteral contains value of any type including null
     * @return boolean, where if both items are null, it picks the object over the rex literal
     */
    private boolean isGreaterThan(final Optional<?> o, final RexLiteral rexLiteral) {
       if(o.isPresent() && !rexLiteral.isNull()) { //if o is any and rex is any 
            final Object comparator = rexLiteral.getValueAs(o.get().getClass());
            return ((Comparable) o.get()).compareTo(comparator) > 0;
        } else if (rexLiteral.isNull()){
            return true; //if o is any and rex is null
        } else {
            return false; //is o is null and rex any other value
        }
    }

    /**
     * Checks if object o is less than the rex literal
     * @param o object of any type including null
     * @param rexLiteral contains value of any type including null
     * @return boolean, where if both items are null, it picks the object over the rex literal
     */
    private boolean isLessThan(final Optional<?> o, final RexLiteral rexLiteral) {
        if(o.isPresent() && !rexLiteral.isNull()) { //if o is any and rex is any 
            final Object comparator = rexLiteral.getValueAs(o.get().getClass());
            return ((Comparable) o.get()).compareTo(comparator) < 0;
        } else if (rexLiteral.isNull()){
            return true; //if o is any and rex is null
        } else {
            return false; //is o is null and rex any other value
        }
    }

    private boolean isEqualTo(final Optional<?> o, final RexLiteral rexLiteral) {
        try {
            if(o.isEmpty() && rexLiteral.isNull()) return true;
            if(o.isPresent()) return ((Comparable) o.get()).compareTo(rexLiteral.getValueAs(o.get().getClass())) == 0;
            return false;
        } catch (final Exception e) {
            throw new IllegalStateException("Predicate not supported yet, something went wrong when computing an isEqualTo predicate, object: " + o + " rexLiteral: " + rexLiteral + " rexLiteral kind: " + rexLiteral.getKind() + " rexLiteral type: " + rexLiteral.getType());
        }
    }

    private boolean isEqualTo(final Optional<?> o, final Optional<?> o2) {
        try {
            if(o.isEmpty() && o2.isEmpty()) return true;
            if(o.isPresent() && o2.isPresent()) return ((Comparable) o.get()).compareTo(o2.get()) == 0;
            return false;
        } catch (final Exception e) {
            throw new IllegalStateException("Predicate not supported yet, something went wrong when computing an isEqualTo predicate, object: " + o + " object2: " + o2);
        }
    }
}