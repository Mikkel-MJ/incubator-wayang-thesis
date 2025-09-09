/*PlanImplementationPlanImplementati
 * Licensed to the Avapache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.core.optimizer.enumeration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.costs.EstimatableCost;
import org.apache.wayang.core.optimizer.costs.TimeEstimate;
import org.apache.wayang.core.optimizer.costs.TimeToCostConverter;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ElementaryOperator;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.LoopSubplan;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.Slot;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.Junction;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.util.Canonicalizer;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.WayangCollections;

/**
 * Represents a partial execution plan.
 */
public class PlanImplementation {

    /**
     * Amends a {@link ConcatenationGroupDescriptor} by
     * {@link PlanImplementation}-specific information.
     */
    class ConcatenationDescriptor {

        final ConcatenationGroupDescriptor groupDescriptor;

        final PlanImplementation execOutputPlanImplementation;

        /**
         * Creates a new instance.
         */
        ConcatenationDescriptor(final OutputSlot<?> output, final List<InputSlot<?>> inputs) {
            // Find the ExecutionOperator's corresponding OutputSlot along with the nested
            // PlanImplementation.
            OutputSlot<?> execOutput = null;
            PlanImplementation execOutputPlanImplementation = null;
            if (output != null) {
                final Collection<Tuple<OutputSlot<?>, PlanImplementation>> execOpOutputsWithContext = PlanImplementation.this
                        .findExecutionOperatorOutputWithContext(output);
                final Tuple<OutputSlot<?>, PlanImplementation> execOpOutputWithCtx = WayangCollections
                        .getSingleOrNull(execOpOutputsWithContext);
                assert execOpOutputsWithContext != null : String.format("No outputs found for %s.", output);
                execOutput = execOpOutputWithCtx.field0;
                execOutputPlanImplementation = execOpOutputWithCtx.field1;
            }

            // Find the ExecutionOperators' corresponding InputSlots.
            final List<LinkedHashSet<InputSlot<?>>> execInputs = new ArrayList<>(inputs.size());
            for (final InputSlot<?> input : inputs) {
                if (input == null) {
                    execInputs.add(null);
                } else {
                    execInputs.add(WayangCollections
                            .asLinkedHashSet(PlanImplementation.this.findExecutionOperatorInputs(input)));
                }
            }

            this.groupDescriptor = new ConcatenationGroupDescriptor(execOutput, execInputs);
            this.execOutputPlanImplementation = execOutputPlanImplementation;
        }

        PlanImplementation getPlanImplementation() {
            return PlanImplementation.this;
        }

    }

    /**
     * Describes a group of {@link PlanImplementation}s in terms of their
     * implementations for some {@link OutputSlot} and
     * {@link InputSlot}s. These {@link Slot}s are not stored in this class and must
     * be clear from the context.
     */
    static class ConcatenationGroupDescriptor {

        /**
         * A corresponding {@link ExecutionOperator}s {@link OutputSlot} or
         * {@code null}.
         */
        final OutputSlot<?> execOutput;

        /**
         * {@link Set}s of corresponding {@link ExecutionOperator}s' {@link InputSlot}s.
         * Individual components can
         * be {@code null} if the {@link PlanImplementation}s do not implement the
         * corresponding {@link InputSlot}.
         */
        final List<LinkedHashSet<InputSlot<?>>> execInputs;

        ConcatenationGroupDescriptor(final OutputSlot<?> execOutput,
                final List<LinkedHashSet<InputSlot<?>>> execInputs) {
            this.execOutput = execOutput;
            this.execInputs = execInputs;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final ConcatenationGroupDescriptor that = (ConcatenationGroupDescriptor) o;
            return Objects.equals(execOutput, that.execOutput) &&
                    Objects.equals(execInputs, that.execInputs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(execOutput, execInputs);
        }
    }

    private static final Logger logger = LogManager.getLogger(PlanImplementation.class);

    /**
     * {@link ExecutionOperator}s contained in this instance.
     */
    private final Canonicalizer<ExecutionOperator> operators = new Canonicalizer<>();

    /**
     * Describes the {@link Channel}s that have been picked between
     * {@link ExecutionOperator}s and how they are
     * implemented.
     */
    private final LinkedHashMap<OutputSlot<?>, Junction> junctions = new LinkedHashMap<>();

    /**
     * Defines how {@link LoopSubplan}s should be executed.
     */
    private final LinkedHashMap<LoopSubplan, LoopImplementation> loopImplementations = new LinkedHashMap<>();

    /**
     * An enumerated plan is mainly characterized by the
     * {@link OperatorAlternative.Alternative}s that have
     * been picked so far. This member keeps track of them.
     */
    private final LinkedHashMap<OperatorAlternative, OperatorAlternative.Alternative> settledAlternatives = new LinkedHashMap<>();

    /**
     * The {@link PlanEnumeration} that hosts this instance. Can change over time.
     */
    // TODO: I think, we don't maintain this field properly. Also, its semantics
    // blur inside of LoopImplementations. Can we remove it?
    private PlanEnumeration planEnumeration;

    /**
     * Keep track of the {@link Platform}s of our {@link #operators}.
     */
    private LinkedHashSet<Platform> platformCache;

    /**
     * {@link OptimizationContext} that provides estimates for the
     * {@link #operators}.
     */
    private final OptimizationContext optimizationContext;

    private final EstimatableCost costModel;

    /**
     * The squashed cost estimate to execute this instance. This will be used to
     * select the best plan!
     */
    private final double squashedCostEstimateCache = Double.NaN, squashedCostEstimateWithoutOverheadCache = Double.NaN;

    /**
     * The parallel cost estimate . This will store both calculated squashed cost
     * and cost that will be used to select the best enumerated plan!
     */
    private Tuple<List<ProbabilisticDoubleInterval>, List<Double>> parallelCostEstimateCache = null;

    /**
     * This will be used to store the parallel cost of each operator.
     */
    private final List<Tuple<Operator, Tuple<List<ProbabilisticDoubleInterval>, List<Double>>>> calculatedParallelOperatorCostCache = new ArrayList<>();

    /**
     * Copy constructor.
     */
    public PlanImplementation(final PlanImplementation original) {
        this.planEnumeration = original.planEnumeration;
        this.junctions.putAll(original.junctions);
        this.operators.addAll(original.getOperators());
        this.settledAlternatives.putAll(original.settledAlternatives);
        this.loopImplementations.putAll(original.loopImplementations);
        this.optimizationContext = original.optimizationContext;
        this.costModel = this.optimizationContext
                .getConfiguration()
                .getCostModel()
                .getFactory()
                .makeCost();
    }

    /**
     * Create a new instance.
     */
    PlanImplementation(
            final PlanEnumeration planEnumeration,
            final LinkedHashMap<OutputSlot<?>, Junction> junctions,
            final Collection<ExecutionOperator> operators,
            final OptimizationContext optimizationContext) {
        this(planEnumeration, junctions, new Canonicalizer<>(operators), optimizationContext);
    }

    /**
     * Creates new instance.
     */
    PlanImplementation(final PlanEnumeration planEnumeration,
            final LinkedHashMap<OutputSlot<?>, Junction> junctions,
            final OptimizationContext optimizationContext) {
        this(planEnumeration, junctions, new Canonicalizer<>(), optimizationContext);
    }

    /**
     * Base constructor.
     */
    private PlanImplementation(final PlanEnumeration planEnumeration,
            final LinkedHashMap<OutputSlot<?>, Junction> junctions,
            final Canonicalizer<ExecutionOperator> operators,
            final OptimizationContext optimizationContext) {
        this.planEnumeration = planEnumeration;
        this.junctions.putAll(junctions);
        this.operators.addAll(operators);
        this.optimizationContext = optimizationContext;
        this.costModel = this.optimizationContext
                .getConfiguration()
                .getCostModel()
                .getFactory()
                .makeCost();

        assert this.planEnumeration != null;
    }

    /**
     * @return the {@link PlanEnumeration} this instance belongs to
     */
    public PlanEnumeration getPlanEnumeration() {
        return this.planEnumeration;
    }

    public void setPlanEnumeration(final PlanEnumeration planEnumeration) {
        this.planEnumeration = planEnumeration;
    }

    /**
     * Escapes the {@link OperatorAlternative} that contains this instance.
     *
     * @param alternative        contains this instance
     * @param newPlanEnumeration will host the new instance
     * @return
     */
    public PlanImplementation escape(final OperatorAlternative.Alternative alternative,
            final PlanEnumeration newPlanEnumeration) {
        final PlanImplementation escapedPlanImplementation = new PlanImplementation(
                newPlanEnumeration, this.junctions, this.operators, this.optimizationContext);
        escapedPlanImplementation.settledAlternatives.putAll(this.settledAlternatives);
        assert !escapedPlanImplementation.settledAlternatives.containsKey(alternative.getOperatorAlternative());
        escapedPlanImplementation.settledAlternatives.put(alternative.getOperatorAlternative(), alternative);
        escapedPlanImplementation.loopImplementations.putAll(this.getLoopImplementations());
        return escapedPlanImplementation;
    }

    public Canonicalizer<ExecutionOperator> getOperators() {
        return this.operators;
    }

    public LinkedHashMap<OperatorAlternative, OperatorAlternative.Alternative> getSettledAlternatives() {
        return this.settledAlternatives;
    }

    public LinkedHashMap<LoopSubplan, LoopImplementation> getLoopImplementations() {
        return this.loopImplementations;
    }

    /**
     * Adds a new {@link LoopImplementation} for a given {@link LoopSubplan}.
     *
     * @param loop               the {@link LoopSubplan}
     * @param loopImplementation the {@link LoopImplementation}
     */
    public void addLoopImplementation(final LoopSubplan loop, final LoopImplementation loopImplementation) {
        this.loopImplementations.put(loop, loopImplementation);
    }

    /**
     * @return those contained {@link ExecutionOperator}s that have a {@link Slot}
     *         that is yet to be connected
     *         to a further {@link ExecutionOperator} in the further plan
     *         enumeration process
     */
    public Collection<ExecutionOperator> getInterfaceOperators() {
        Validate.notNull(this.getPlanEnumeration());
        final LinkedHashSet<OutputSlot> outputSlots = this.getPlanEnumeration().servingOutputSlots.stream()
                .map(Tuple::getField0)
                .distinct()
                .collect(Collectors.toCollection(LinkedHashSet::new));

        final LinkedHashSet<InputSlot<?>> inputSlots = this.getPlanEnumeration().requestedInputSlots;

        return this.operators.stream()
                .filter(operator -> this.allOutermostInputSlots(operator).anyMatch(inputSlots::contains) ||
                        this.allOutermostOutputSlots(operator).anyMatch(outputSlots::contains))
                .collect(Collectors.toList());
    }

    /**
     * Find the {@link ExecutionOperator} that do not depend on any other
     * {@link ExecutionOperator} as input.
     *
     * @return the start {@link ElementaryOperator}s
     */
    public List<ExecutionOperator> getStartOperators() {
        return this.operators.stream()
                .filter(this::isStartOperator)
                .collect(Collectors.toList());
    }

    /**
     * Find for a given {@link OperatorAlternative}, which
     * {@link OperatorAlternative.Alternative} has been picked
     * by this instance
     *
     * @param operatorAlternative the {@link OperatorAlternative} in question
     * @return the {@link OperatorAlternative.Alternative} or {@code null} if none
     *         has been chosen in this instance
     */
    public OperatorAlternative.Alternative getChosenAlternative(final OperatorAlternative operatorAlternative) {
        return this.settledAlternatives.get(operatorAlternative);
    }

    /**
     * Retrieves the {@link TimeEstimate} for this instance, including platform
     * overhead.
     *
     * @return the {@link TimeEstimate}
     */
    public TimeEstimate getTimeEstimate() {
        return this.getTimeEstimate(true);
    }

    /**
     * Retrieves the {@link TimeEstimate} for this instance, including platform
     * overhead.
     *
     * @param isIncludeOverhead whether to include any incurring global overhead
     * @return the {@link TimeEstimate}
     */
    public TimeEstimate getTimeEstimate(final boolean isIncludeOverhead) {
        final TimeEstimate operatorTimeEstimate = this.operators.stream()
                .map(op -> this.optimizationContext.getOperatorContext(op).getTimeEstimate())
                .reduce(TimeEstimate.ZERO, TimeEstimate::plus);
        final TimeEstimate junctionTimeEstimate = this.optimizationContext.getDefaultOptimizationContexts().stream()
                .flatMap(optCtx -> this.junctions.values().stream().map(jct -> jct.getTimeEstimate(optCtx)))
                .reduce(TimeEstimate.ZERO, TimeEstimate::plus);
        final TimeEstimate loopTimeEstimate = this.loopImplementations.values().stream()
                .map(LoopImplementation::getTimeEstimate)
                .reduce(TimeEstimate.ZERO, TimeEstimate::plus);
        TimeEstimate timeEstimate = operatorTimeEstimate.plus(junctionTimeEstimate).plus(loopTimeEstimate);

        if (isIncludeOverhead) {
            final long platformInitializationTime = this.getUtilizedPlatforms().stream()
                    .map(platform -> this.optimizationContext.getConfiguration().getPlatformStartUpTimeProvider()
                            .provideFor(platform))
                    .reduce(0L, (a, b) -> a + b);
            timeEstimate = timeEstimate.plus(platformInitializationTime);
        }

        return timeEstimate;
    }

    /**
     * Retrieves the cost estimate for this instance including any overhead.
     *
     * @return the cost estimate
     */
    public ProbabilisticDoubleInterval getCostEstimate() {
        return this.getCostEstimate(true);
    }

    /**
     * Retrieves the cost estimate for this instance including any overhead.
     *
     * @return the cost estimate
     */
    public double getSquashedCostEstimate() {
        return this.getSquashedCostEstimate(true);
    }

    public Junction getJunction(final OutputSlot<?> output) {
        return this.junctions.get(output);
    }

    public void putJunction(final OutputSlot<?> output, final Junction junction) {
        final Junction oldValue = junction == null ? this.junctions.remove(output)
                : this.junctions.put(output, junction);
        if (oldValue != null) {
            logger.warn("Replaced {} with {}.", oldValue, junction);
        }
    }

    public OptimizationContext getOptimizationContext() {
        return this.optimizationContext;
    }

    /**
     * Merges the {@link OptimizationContext}s of the {@link Junction}s in this
     * instance into its main
     * {@link OptimizationContext}/
     */
    public void mergeJunctionOptimizationContexts() {
        // Merge the top-level Junctions.
        for (final Junction junction : this.junctions.values()) {
            junction.getOptimizationContexts().forEach(OptimizationContext::mergeToBase);
        }

        // Descend into loops.
        this.loopImplementations.values().stream()
                .flatMap(loopImplementation -> loopImplementation.getIterationImplementations().stream())
                .map(LoopImplementation.IterationImplementation::getBodyImplementation)
                .forEach(PlanImplementation::mergeJunctionOptimizationContexts);
    }

    public void logTimeEstimates() {
        if (!this.logger.isDebugEnabled())
            return;

        this.logger.debug(">>> Regular operators");
        for (final ExecutionOperator operator : this.operators) {
            this.logger.debug("Estimated execution time of {}: {}",
                    operator, this.optimizationContext.getOperatorContext(operator).getTimeEstimate());
        }
        this.logger.debug(">>> Glue operators");
        for (final Junction junction : junctions.values()) {
            for (final ExecutionTask task : junction.getConversionTasks()) {
                final ExecutionOperator operator = task.getOperator();
                this.logger.debug("Estimated execution time of {}: {}",
                        operator, this.optimizationContext.getOperatorContext(operator).getTimeEstimate());
            }
        }
        this.logger.debug(">>> Loops");
        for (final LoopImplementation loopImplementation : this.loopImplementations.values()) {
            for (final LoopImplementation.IterationImplementation iterationImplementation : loopImplementation
                    .getIterationImplementations()) {
                iterationImplementation.getBodyImplementation().logTimeEstimates();
            }
        }
    }

    public LinkedHashMap<OutputSlot<?>, Junction> getJunctions() {
        return this.junctions;
    }

    public EstimatableCost getCost() {
        return this.costModel;
    }

    /**
     * Retrieve the {@link Platform}s that are utilized by this instance.
     *
     * @return the {@link Platform}s
     */
    public LinkedHashSet<Platform> getUtilizedPlatforms() {
        if (this.platformCache == null) {
            this.platformCache = this.streamOperators()
                    .map(ExecutionOperator::getPlatform)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
        return this.platformCache;
    }

    @Override
    public String toString() {
        return String.format("PlanImplementation[%s, %s, costs=%s]",
                this.getUtilizedPlatforms(), this.getTimeEstimate(), this.getCostEstimate());
    }

    /**
     * Find the {@link InputSlot}s of already picked {@link ExecutionOperator}s that
     * represent the given {@link InputSlot}.
     * <p>
     * Note that we require that this instance either provides all or no
     * {@link ExecutionOperator}s necessary to
     * implement the {@link InputSlot}.
     * </p>
     *
     * @param someInput any {@link InputSlot} of the original {@link WayangPlan}
     * @return the representing {@link InputSlot}s or {@code null} if this instance
     *         has no {@link ExecutionOperator}
     *         backing the given {@link InputSlot}
     */
    Collection<InputSlot<?>> findExecutionOperatorInputs(final InputSlot<?> someInput) {
        final Operator owner = someInput.getOwner();
        if (owner.isAlternative()) {
            final OperatorAlternative operatorAlternative = (OperatorAlternative) owner;
            final OperatorAlternative.Alternative alternative = this.settledAlternatives.get(operatorAlternative);
            if (alternative == null)
                return null;
            @SuppressWarnings("unchecked")
            final Collection<InputSlot<?>> innerInputs = (Collection<InputSlot<?>>) (Collection) alternative
                    .followInput(someInput);
            boolean isWithNull = false;
            Collection<InputSlot<?>> result = null;
            for (final InputSlot<?> innerInput : innerInputs) {
                final Collection<InputSlot<?>> resolvedInputs = this.findExecutionOperatorInputs(innerInput);
                if (isWithNull && resolvedInputs != null) {
                    throw new IllegalStateException(
                            String.format("Disallowed that %s is required by two different alternatives.", someInput));
                }
                isWithNull |= resolvedInputs == null;
                if (result == null) {
                    result = resolvedInputs;
                } else {
                    assert resolvedInputs != null;
                    result.addAll(resolvedInputs);
                }
            }
            return result;

        } else if (owner.isLoopSubplan()) {
            final LoopSubplan loopSubplan = (LoopSubplan) owner;
            final LoopImplementation loopImplementation = this.getLoopImplementations().get(loopSubplan);
            if (loopImplementation == null)
                return null;

            // Enter the LoopSubplan.
            final Collection<InputSlot<?>> innerInputs = loopSubplan.followInputUnchecked(someInput);
            if (innerInputs.isEmpty())
                return innerInputs;

            // Discern LoopHeadOperator InputSlots and loop body InputSlots.
            final List<LoopImplementation.IterationImplementation> iterationImpls = loopImplementation
                    .getIterationImplementations();
            final Collection<InputSlot<?>> collector = new LinkedHashSet<>(innerInputs.size());
            for (final InputSlot<?> innerInput : innerInputs) {
                if (innerInput.getOwner() == loopSubplan.getLoopHead()) {
                    final LoopImplementation.IterationImplementation initialIterationImpl = iterationImpls.get(0);
                    collector.addAll(
                            initialIterationImpl.getBodyImplementation().findExecutionOperatorInputs(innerInput));
                } else {
                    for (final LoopImplementation.IterationImplementation iterationImpl : iterationImpls) {
                        collector.addAll(
                                iterationImpl.getBodyImplementation().findExecutionOperatorInputs(innerInput));
                    }
                }
            }
            return collector;

        } else {
            assert owner.isExecutionOperator();
            final Collection<InputSlot<?>> result = new LinkedList<>();
            result.add(someInput);
            return result;
        }

    }

    /**
     * Find the {@link OutputSlot}s of already picked {@link ExecutionOperator}s
     * that represent the given {@link OutputSlot}.
     *
     * @param someOutput any {@link OutputSlot} of the original {@link WayangPlan}
     * @return the representing {@link OutputSlot}s
     */
    Collection<OutputSlot<?>> findExecutionOperatorOutput(final OutputSlot<?> someOutput) {
        return this.findExecutionOperatorOutputWithContext(someOutput).stream()
                .map(Tuple::getField0)
                .collect(Collectors.toList());
    }

    /**
     * Find the {@link OutputSlot}s of already picked {@link ExecutionOperator}s
     * that represent the given {@link OutputSlot}.
     *
     * @param someOutput any {@link OutputSlot} of the original
     *                   {@link WayangPlan}PlanImplementation
     * @return the representing {@link OutputSlot}s together with their enclosing
     *         {@link PlanImplementation}
     */
    Collection<Tuple<OutputSlot<?>, PlanImplementation>> findExecutionOperatorOutputWithContext(
            OutputSlot<?> someOutput) {
        while (someOutput != null
                && someOutput.getOwner().isAlternative()) {
            final Operator owner = someOutput.getOwner();
            final OperatorAlternative operatorAlternative = (OperatorAlternative) owner;
            final OperatorAlternative.Alternative alternative = this.settledAlternatives.get(operatorAlternative);
            someOutput = alternative == null ? null : alternative.traceOutput(someOutput);
        }

        // If we did not find a terminal OutputSlot.
        if (someOutput == null) {
            return Collections.emptySet();
        }

        // Otherwise, discern LoopSubplans and ExecutionOperators.
        final Operator owner = someOutput.getOwner();
        if (owner.isLoopSubplan()) {
            final LoopSubplan loopSubplan = (LoopSubplan) owner;
            final LoopImplementation loopImplementation = this.getLoopImplementations().get(loopSubplan);
            if (loopImplementation == null)
                return Collections.emptyList();

            // Enter the LoopSubplan.
            final OutputSlot<?> innerOutput = loopSubplan.traceOutput(someOutput);
            if (innerOutput == null)
                return Collections.emptyList();
            assert innerOutput.getOwner().isLoopHead();

            // For all the iterations, return the potential OutputSlots.
            final List<LoopImplementation.IterationImplementation> iterationImpls = loopImplementation
                    .getIterationImplementations();
            final LinkedHashSet<Tuple<OutputSlot<?>, PlanImplementation>> collector = new LinkedHashSet<>(
                    iterationImpls.size());
            for (final LoopImplementation.IterationImplementation iterationImpl : iterationImpls) {
                final Collection<Tuple<OutputSlot<?>, PlanImplementation>> outputsWithContext = iterationImpl
                        .getBodyImplementation().findExecutionOperatorOutputWithContext(innerOutput);
                collector.addAll(outputsWithContext);
            }

            return collector;

        } else {
            assert owner.isExecutionOperator();
            return Collections.singleton(new Tuple<>(someOutput, this));
        }
    }

    /**
     * Creates a new instance that forms the concatenation of this instance with the
     * {@code targetPlans} via the
     * {@code junction}.
     *
     * @param targetPlans              instances to connect to
     * @param junction                 connects this instance with the
     *                                 {@code targetPlans}
     * @param outputPlanImplementation nested instance of this instance that hosts
     *                                 the {@code junction}
     * @param concatenationEnumeration that will host the concatenated instance
     * @return the concatenated instance or {@code null} if the inputs are
     *         contradicting each other
     */
    PlanImplementation concatenate(final List<PlanImplementation> targetPlans,
            final Junction junction,
            final PlanImplementation outputPlanImplementation,
            final PlanEnumeration concatenationEnumeration) {

        final PlanImplementation concatenation = new PlanImplementation(
                concatenationEnumeration,
                new LinkedHashMap<>(this.junctions.size() + 1),
                new LinkedHashSet<>(this.settledAlternatives.size(), targetPlans.size() * 4f), // ballpark figure
                this.optimizationContext);

        concatenation.operators.addAll(this.operators);
        concatenation.junctions.putAll(this.junctions);
        concatenation.settledAlternatives.putAll(this.settledAlternatives);

        // Find the appropriate PlanImplementation for the junction and copy the loop
        // implementations.
        PlanImplementation junctionPlanImplementation;
        if (outputPlanImplementation == this) {
            // Special case: The junction resides inside the top-level PlanImplementation.
            concatenation.loopImplementations.putAll(this.loopImplementations);
            junctionPlanImplementation = concatenation;
        } else {
            // Exhaustively, yet focused, search for the PlanImplementation.
            junctionPlanImplementation = concatenation.copyLoopImplementations(
                    this,
                    outputPlanImplementation,
                    junction.getSourceOutput().getOwner().getLoopStack());
        }
        junctionPlanImplementation.junctions.put(junction.getSourceOutput(), junction);

        for (final PlanImplementation targetPlan : targetPlans) {
            // NB: Join semantics at this point weaved in.
            if (concatenation.isSettledAlternativesContradicting(targetPlan)) {
                return null;
            }
            concatenation.operators.addAll(targetPlan.operators);
            concatenation.loopImplementations.putAll(targetPlan.loopImplementations);
            concatenation.junctions.putAll(targetPlan.junctions);
            concatenation.settledAlternatives.putAll(targetPlan.settledAlternatives);
        }

        return concatenation;
    }

    /**
     * Retrieves the cost estimate for this instance.
     *
     * @param isIncludeOverhead whether to include global overhead in the
     *                          {@link TimeEstimate} (to avoid repeating
     *                          overhead in nested instances)
     * @return the cost estimate
     */
    ProbabilisticDoubleInterval getCostEstimate(final boolean isIncludeOverhead) {
        if (this.optimizationContext.getConfiguration()
                .getBooleanProperty("wayang.core.optimizer.enumeration.parallel-tasks")) {
            return this.costModel.getParallelEstimate(this, isIncludeOverhead);
        } else {
            return this.costModel.getEstimate(this, isIncludeOverhead);
        }
    }

    /**
     * Retrieves the cost estimate for this instance.
     *
     * @param isIncludeOverhead whether to include global overhead in the
     *                          {@link TimeEstimate} (to avoid repeating
     *                          overhead in nested instances)
     * @return the squashed cost estimate
     */
    double getSquashedCostEstimate(final boolean isIncludeOverhead) {
        // Check if the parallel cost calculation is enabled in the configuration file
        if (this.optimizationContext.getConfiguration()
                .getBooleanProperty("wayang.core.optimizer.enumeration.parallel-tasks")) {
            return this.costModel.getSquashedParallelEstimate(this, isIncludeOverhead);
        } else {
            return this.costModel.getSquashedEstimate(this, isIncludeOverhead);
        }
    }

    /**
     * Retrieves the cost estimate for this instance taking into account parallel
     * stage execution.
     *
     * @param isIncludeOverhead whether to include global overhead in the
     *                          {@link TimeEstimate} (to avoid repeating
     *                          overhead in nested instances)
     * @return the cost estimate taking into account parallel stage execution
     */
    ProbabilisticDoubleInterval getParallelCostEstimate(final boolean isIncludeOverhead) {
        ProbabilisticDoubleInterval parallelCostEstimateWithoutOverhead, parallelCostEstimate;

        if (this.parallelCostEstimateCache == null) {
            // It means that the squashed cost is not yet called, might be only one possible
            // execution plan
            this.getSquashedParallelCostEstimate(true);
        }

        final ProbabilisticDoubleInterval loopCosts = this.loopImplementations.values().stream()
                .map(LoopImplementation::getCostEstimate)
                .reduce(ProbabilisticDoubleInterval.zero, ProbabilisticDoubleInterval::plus);
        parallelCostEstimateWithoutOverhead = this.parallelCostEstimateCache.field0.get(0)
                .plus(this.parallelCostEstimateCache.field0.get(1)).plus(loopCosts);
        final ProbabilisticDoubleInterval overheadCosts = this.getUtilizedPlatforms().stream()
                .map(platform -> {
                    final Configuration configuration = this.optimizationContext.getConfiguration();
                    final long startUpTime = configuration.getPlatformStartUpTimeProvider().provideFor(platform);
                    final TimeToCostConverter timeToCostConverter = configuration.getTimeToCostConverterProvider()
                            .provideFor(platform);
                    return timeToCostConverter.convert(new TimeEstimate(startUpTime, startUpTime, 1d));
                })
                .reduce(ProbabilisticDoubleInterval.zero, ProbabilisticDoubleInterval::plus);
        parallelCostEstimate = parallelCostEstimateWithoutOverhead.plus(overheadCosts);
        return isIncludeOverhead ? parallelCostEstimate : parallelCostEstimateWithoutOverhead;
    }

    /**
     * Retrieves the cost estimate for this instance taking into account parallel
     * stage execution.
     *
     * @param isIncludeOverhead whether to include global overhead in the
     *                          {@link TimeEstimate} (to avoid repeating
     *                          overhead in nested instances)
     * @return the squashed cost estimate taking into account parallel stage
     *         execution
     */
    double getSquashedParallelCostEstimate(final boolean isIncludeOverhead) {
        // Collect sink operators by Removing all operators that have an output
        LinkedHashSet<Operator> sinkOperators;
        sinkOperators = this.operators.stream()
                .filter(op -> op.getNumOutputs() == 0)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // Retrieve operator and junction cost with parallel stage consideration
        double parallelOperatorCosts = 0f;
        double parallelJunctionCosts = 0f;

        // Iterate through all sinks to find the expensive sink
        for (final Operator op : sinkOperators) {
            final Tuple<List<ProbabilisticDoubleInterval>, List<Double>> tempParallelCostEstimate = this
                    .getParallelOperatorJunctionAllCostEstimate(op);
            final List<Double> tempSquashedCost = tempParallelCostEstimate.field1;

            if (tempSquashedCost.get(0) + tempSquashedCost.get(1) > parallelOperatorCosts + parallelJunctionCosts) {
                parallelOperatorCosts = tempSquashedCost.get(0);
                parallelJunctionCosts = tempSquashedCost.get(1);
                this.parallelCostEstimateCache = tempParallelCostEstimate;
            }
        }
        final double loopCosts = this.loopImplementations.values().stream()
                .mapToDouble(LoopImplementation::getSquashedCostEstimate)
                .sum();
        final double parallelSquashedCostEstimateWithoutOverhead = parallelOperatorCosts + parallelJunctionCosts
                + loopCosts;
        final double overheadCosts = this.getUtilizedPlatforms().stream()
                .mapToDouble(platform -> {
                    final Configuration configuration = this.optimizationContext.getConfiguration();

                    final long startUpTime = configuration.getPlatformStartUpTimeProvider().provideFor(platform);

                    final TimeToCostConverter timeToCostConverter = configuration.getTimeToCostConverterProvider()
                            .provideFor(platform);
                    final ProbabilisticDoubleInterval costs = timeToCostConverter
                            .convert(new TimeEstimate(startUpTime, startUpTime, 1d));

                    final ToDoubleFunction<ProbabilisticDoubleInterval> squasher = configuration
                            .getCostSquasherProvider().provide();
                    return squasher.applyAsDouble(costs);
                })
                .sum();
        final double parallelSquashedCostEstimate = parallelSquashedCostEstimateWithoutOverhead + overheadCosts;
        return isIncludeOverhead ? parallelSquashedCostEstimate : parallelSquashedCostEstimateWithoutOverhead;
    }

    /**
     * Stream all the {@link ExecutionOperator}s in this instance.
     *
     * @return a {@link Stream} containing every {@link ExecutionOperator} at least
     *         once
     */
    Stream<ExecutionOperator> streamOperators() {
        Stream<ExecutionOperator> operatorStream = Stream.concat(
                this.operators.stream(),
                this.junctions.values().stream().flatMap(j -> j.getConversionTasks().stream())
                        .map(ExecutionTask::getOperator));
        if (!this.loopImplementations.isEmpty()) {
            operatorStream = Stream.concat(
                    operatorStream,
                    this.loopImplementations.values().stream().flatMap(LoopImplementation::streamOperators));
        }
        return operatorStream;
    }

    /**
     * Creates a new {@link ConcatenationDescriptor} for this instance.
     *
     * @param output the relevant {@link OutputSlot} or {@code null}
     * @param inputs the relevant {@link InputSlot}s; components can be {@code null}
     * @return the {@link ConcatenationDescriptor}
     */
    ConcatenationDescriptor createConcatenationDescriptor(final OutputSlot<?> output, final List<InputSlot<?>> inputs) {
        return new ConcatenationDescriptor(output, inputs);
    }

    /**
     * Find the a given nested {@link PlanImplementation} in a further
     * {@link PlanImplementation} and copy it to
     * this instance.
     *
     * @param originalPlanImplementation the (top-level) {@link PlanImplementation}
     *                                   to copy from
     * @param targetPlanImplementation   the (nested) {@link PlanImplementation}
     *                                   that should be copied
     * @param loopStack                  of an {@link ExecutionOperator} inside of
     *                                   the {@code targetPlanImplementation}
     * @return the copied {@link PlanImplementation} inside of this instance
     */
    private PlanImplementation copyLoopImplementations(final PlanImplementation originalPlanImplementation,
            final PlanImplementation targetPlanImplementation,
            final LinkedList<LoopSubplan> loopStack) {
        // Descend into the loopStack.
        assert !loopStack.isEmpty();
        final LoopSubplan visitedLoop = loopStack.pop();

        // Copy the LoopImplementations of the originalPlanImplementation.
        this.loopImplementations.putAll(originalPlanImplementation.getLoopImplementations());
        // This one will be altered, so make an instance copy.
        final LoopImplementation loopImplCopy = this.loopImplementations.compute(visitedLoop,
                (key, value) -> new LoopImplementation(value));

        // Find the original counterpart to loopImplCopy.
        final LoopImplementation originalLoopImpl = originalPlanImplementation.loopImplementations.get(visitedLoop);

        // Go over the iterations of the LoopImplementations in parallel to process
        // their PlanImplementations.
        PlanImplementation targetPlanImplementationCopy = null;
        final Iterator<LoopImplementation.IterationImplementation> originalIterator = originalLoopImpl
                .getIterationImplementations().iterator(),
                copyIterator = loopImplCopy.getIterationImplementations().iterator();
        while (originalIterator.hasNext()) {
            final LoopImplementation.IterationImplementation nextCopy = copyIterator.next();
            final LoopImplementation.IterationImplementation nextOriginal = originalIterator.next();
            // If we need to descend further, invoke a recursive call.
            if (!loopStack.isEmpty()) {
                targetPlanImplementationCopy = nextCopy.getBodyImplementation().copyLoopImplementations(
                        nextOriginal.getBodyImplementation(),
                        targetPlanImplementation,
                        loopStack);

                // Once, we have found the iteration that contains the targetPlanImplementation,
                // we can stop.
                if (targetPlanImplementationCopy != null)
                    break;
            } else {
                // If we cannot descend futher, we basically need to find the correct iteration
                // only.
                if (nextOriginal.getBodyImplementation() == targetPlanImplementation) {
                    targetPlanImplementationCopy = nextCopy.getBodyImplementation();
                    break;
                }
            }
        }

        // Restore the loopStack.
        loopStack.push(visitedLoop);

        // Return the match.
        return targetPlanImplementationCopy;

    }

    private boolean isSettledAlternativesContradicting(final PlanImplementation that) {
        for (final Entry<OperatorAlternative, OperatorAlternative.Alternative> entry : this.settledAlternatives
                .entrySet()) {
            final OperatorAlternative opAlt = entry.getKey();
            final OperatorAlternative.Alternative alternative = entry.getValue();
            final OperatorAlternative.Alternative thatAlternative = that.settledAlternatives.get(opAlt);
            if (thatAlternative != null && alternative != thatAlternative) {
                return true;
            }
        }
        for (final Entry<LoopSubplan, LoopImplementation> entry : this.loopImplementations.entrySet()) {
            final LoopSubplan loop = entry.getKey();
            final LoopImplementation thisLoopImplementation = entry.getValue();
            final LoopImplementation thatLoopImplementation = that.loopImplementations.get(loop);
            if (thatLoopImplementation == null)
                continue;
            if (thisLoopImplementation
                    .getSingleIterationImplementation()
                    .getBodyImplementation()
                    .isSettledAlternativesContradicting(
                            thatLoopImplementation
                                    .getIterationImplementations().get(0)
                                    .getBodyImplementation())) {
                return true;
            }
        }
        return false;
    }

    private Stream<OutputSlot> allOutermostOutputSlots(final Operator operator) {
        return Arrays.stream(operator.getAllOutputs())
                .flatMap(output -> operator.getOutermostOutputSlots(output).stream());
    }

    private Stream<InputSlot> allOutermostInputSlots(final Operator operator) {
        return Arrays.stream(operator.getAllInputs())
                .map(operator::getOutermostInputSlot);
    }

    /**
     * Detects start {@link ExecutionOperator}s.
     * <p>
     * A start {@link ExecutionOperator} has an {@link InputSlot} that is requested
     * by the {@link #planEnumeration}.
     * </p>
     */
    private boolean isStartOperator(final ExecutionOperator executionOperator) {
        ForLoop: for (InputSlot<?> inputSlot : executionOperator.getOriginal().getAllInputs()) {
            while (inputSlot != null) {
                if (this.planEnumeration.requestedInputSlots.contains(inputSlot)) {
                    continue ForLoop;
                }
                inputSlot = inputSlot.getOwner().getOuterInputSlot(inputSlot);
            }
            return false;
        }
        return true;
    }

    /**
     * Retrieves the cost estimate of input {@link Operator} and input
     * {@link Junction} and recurse if there is input Operators
     *
     * @param operator {@link Operator} that will be used to retreive the
     *                 cost/squashed costs
     * @return list of probabilisticDoubleInterval where First element is the
     *         operator cost and second element is the junction cost; and
     *         list of double retreived where First element is the operator squashed
     *         cost and second element is the junction squashed cost
     *         <p>
     *         PS: This function will start with the sink operator
     */

    private Tuple<List<ProbabilisticDoubleInterval>, List<Double>> getParallelOperatorJunctionAllCostEstimate(
            final Operator operator) {

        final LinkedHashSet<Operator> inputOperators = new LinkedHashSet<>();
        final LinkedHashSet<Junction> inputJunction = new LinkedHashSet<>();

        final List<ProbabilisticDoubleInterval> probalisticCost = new ArrayList<>();
        final List<Double> squashedCost = new ArrayList<>();

        // check if the operator cost was already calculated and cached
        for (final Tuple<Operator, Tuple<List<ProbabilisticDoubleInterval>, List<Double>>> t : calculatedParallelOperatorCostCache) {
            if (t.field0 == operator)
                return t.field1;
        }

        if (this.optimizationContext.getOperatorContext(operator) != null) {
            // Get input junctions
            this.junctions.values()
                    .forEach(j -> {
                        for (int itr = 0; itr < j.getNumTargets(); itr++) {
                            if (j.getTargetOperator(itr) == operator)
                                inputJunction.add(j);
                        }
                    });
            // Get input operators associated with input junctions
            inputJunction
                    .forEach((final Junction j) -> {
                        inputOperators.add(j.getSourceOperator());
                    });

            if (inputOperators.size() == 0) {
                // If there is no input operator, only the cost of the current operator is
                // returned
                probalisticCost.add(this.optimizationContext.getOperatorContext(operator).getCostEstimate());
                probalisticCost.add(new ProbabilisticDoubleInterval(0f, 0f, 0f));
                squashedCost.add(this.optimizationContext.getOperatorContext(operator).getSquashedCostEstimate());
                squashedCost.add(.0);
                final Tuple<List<ProbabilisticDoubleInterval>, List<Double>> returnedCost = new Tuple(probalisticCost,
                        squashedCost);
                this.calculatedParallelOperatorCostCache.add(new Tuple(operator, returnedCost));
                return returnedCost;
            } else if (inputOperators.size() == 1) {
                // If there is only one input operator the cost of the current operator plus the
                // cost of the input operator is returned

                // Get the operator probalistic cost and put it as a first element in
                // probalisticCost
                probalisticCost.add(this.optimizationContext.getOperatorContext(operator).getCostEstimate()
                        .plus(this.getParallelOperatorJunctionAllCostEstimate(inputOperators.iterator().next()).field0
                                .get(0)));
                // Get the junction probalistic cost and put it as a second element in
                // probalisticCost
                probalisticCost.add(inputJunction.iterator().next()
                        .getCostEstimate(this.optimizationContext.getDefaultOptimizationContexts().get(0))
                        .plus(this.getParallelOperatorJunctionAllCostEstimate(inputOperators.iterator().next()).field0
                                .get(1)));
                // Get the operator squashed cost and put it as a first element in squashedCost
                squashedCost.add(this.optimizationContext.getOperatorContext(operator).getSquashedCostEstimate()
                        + this.getParallelOperatorJunctionAllCostEstimate(inputOperators.iterator().next()).field1
                                .get(0));
                // Get the junction squashed cost and put it as a second element in squashedCost
                squashedCost.add(inputJunction.iterator().next()
                        .getSquashedCostEstimate(this.optimizationContext.getDefaultOptimizationContexts().get(0))
                        + this.getParallelOperatorJunctionAllCostEstimate(inputOperators.iterator().next()).field1
                                .get(1));

                final Tuple<List<ProbabilisticDoubleInterval>, List<Double>> returnedCost = new Tuple(probalisticCost,
                        squashedCost);
                this.calculatedParallelOperatorCostCache.add(new Tuple(operator, returnedCost));
                return returnedCost;
            } else {
                // If multiple input operators, the cost returned is the max of input operators
                ProbabilisticDoubleInterval maxControlProbabilistic = new ProbabilisticDoubleInterval(0f, 0f, 0f);
                ProbabilisticDoubleInterval maxJunctionProbabilistic = new ProbabilisticDoubleInterval(0f, 0f, 0f);

                double maxControlSquash = 0;
                double maxJunctionSquash = 0;

                for (final Iterator<Operator> op = inputOperators.iterator(); op.hasNext();) {
                    final Tuple<List<ProbabilisticDoubleInterval>, List<Double>> val = this
                            .getParallelOperatorJunctionAllCostEstimate(op.next());
                    final List<ProbabilisticDoubleInterval> valProbalistic = val.field0;
                    final List<Double> valSquash = val.field1;
                    // Take the max of the probalistic cost
                    if (valProbalistic.get(0).getAverageEstimate()
                            + valProbalistic.get(1).getAverageEstimate() > maxControlProbabilistic.getAverageEstimate()
                                    + maxJunctionProbabilistic.getAverageEstimate()) {
                        // Get the control probalistic cost
                        maxControlProbabilistic = valProbalistic.get(0);
                        // Get the junction probalistic cost
                        maxJunctionProbabilistic = valProbalistic.get(1);
                    }
                    // Take the cost of the squashed cost
                    if (valSquash.get(0) > maxControlSquash) {
                        maxControlSquash = valSquash.get(0);
                    }
                    if (valSquash.get(1) > maxJunctionSquash) {
                        maxJunctionSquash = valSquash.get(1);
                    }
                }
                // Get the operator probalistic cost and put it as a first element in
                // probalisticCost
                probalisticCost.add(this.optimizationContext.getOperatorContext(operator).getCostEstimate()
                        .plus(maxControlProbabilistic));
                // Get the junction probalistic cost and put it as a second element in
                // probalisticCost
                probalisticCost.add(inputJunction.iterator().next()
                        .getCostEstimate(this.optimizationContext.getDefaultOptimizationContexts().get(0))
                        .plus(maxJunctionProbabilistic));
                // Get the operator squashed cost and put it as a first element in squashedCost
                squashedCost.add(this.optimizationContext.getOperatorContext(operator).getSquashedCostEstimate()
                        + maxControlSquash);
                // Get the junction squashed cost and put it as a second element in squashedCost
                squashedCost.add(inputJunction.iterator().next()
                        .getSquashedCostEstimate(this.optimizationContext.getDefaultOptimizationContexts().get(0))
                        + maxJunctionSquash);

                final Tuple<List<ProbabilisticDoubleInterval>, List<Double>> returnedCost = new Tuple<>(probalisticCost,
                        squashedCost);
                this.calculatedParallelOperatorCostCache.add(new Tuple<>(operator, returnedCost));
                return returnedCost;
            }
        } else {
            // Handle the case of a control not defined in this.operators (exp: loop
            // operators)
            final double controlSquash = 0;
            final double junctionSquash = 0;
            final ProbabilisticDoubleInterval controlProbabilistic = new ProbabilisticDoubleInterval(0f, 0f, 0f);
            final ProbabilisticDoubleInterval junctionProbabilistic = new ProbabilisticDoubleInterval(0f, 0f, 0f);

            probalisticCost.add(controlProbabilistic);
            probalisticCost.add(junctionProbabilistic);
            squashedCost.add(controlSquash);
            squashedCost.add(junctionSquash);

            return new Tuple<>(probalisticCost, squashedCost);
        }
    }
}
