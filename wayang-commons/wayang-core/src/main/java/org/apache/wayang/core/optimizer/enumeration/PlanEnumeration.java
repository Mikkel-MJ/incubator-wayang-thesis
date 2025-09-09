/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.channels.ChannelConversionGraph;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.Junction;
import org.apache.wayang.core.util.LinkedMultiMap;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.core.util.Tuple;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Represents a collection of {@link PlanImplementation}s that all implement the
 * same section of a {@link WayangPlan} (which
 * is assumed to contain {@link OperatorAlternative}s in general).
 * <p>
 * Instances can be mutated and combined in algebraic manner. In particular,
 * instances can be unioned if they implement
 * the same part of the {@link WayangPlan}, concatenated if there are contact
 * points, and pruned.
 * </p>
 */
public class PlanEnumeration {

    private static final Logger logger = LogManager.getLogger(PlanEnumeration.class);

    /**
     * The {@link OperatorAlternative}s for that an
     * {@link OperatorAlternative.Alternative} has been picked.
     */
    final LinkedHashSet<OperatorAlternative> scope;

    /**
     * {@link InputSlot}s that are not satisfied in this instance.
     */
    final LinkedHashSet<InputSlot<?>> requestedInputSlots;

    /**
     * Combinations of {@link OutputSlot}s and {@link InputSlot}, where the former
     * is served by this instance and the
     * latter is not yet assigned in this instance. If there is no such
     * {@link InputSlot} (because we are enumerating
     * an {@link OperatorAlternative.Alternative}, then we put {@code null} instead
     * of it.
     */
    final LinkedHashSet<Tuple<OutputSlot<?>, InputSlot<?>>> servingOutputSlots;

    /**
     * {@link PlanImplementation}s contained in this instance.
     */
    final List<PlanImplementation> planImplementations;

    /**
     * {@link ExecutionTask}s that have already been executed.
     */
    final LinkedHashMap<ExecutionOperator, ExecutionTask> executedTasks;

    /**
     * Creates a new instance.
     */
    public PlanEnumeration() {
        this(new LinkedHashSet<>(), new LinkedHashSet<>(), new LinkedHashSet<>());
    }

    /**
     * Creates a new instance.
     */
    private PlanEnumeration(final LinkedHashSet<OperatorAlternative> scope,
            final LinkedHashSet<InputSlot<?>> requestedInputSlots,
            final LinkedHashSet<Tuple<OutputSlot<?>, InputSlot<?>>> servingOutputSlots) {
        this(scope, requestedInputSlots, servingOutputSlots, new LinkedList<>(), new LinkedHashMap<>());
    }

    /**
     * Creates a new instance.
     */
    private PlanEnumeration(final LinkedHashSet<OperatorAlternative> scope,
            final LinkedHashSet<InputSlot<?>> requestedInputSlots,
            final LinkedHashSet<Tuple<OutputSlot<?>, InputSlot<?>>> servingOutputSlots,
            final List<PlanImplementation> planImplementations,
            final LinkedHashMap<ExecutionOperator, ExecutionTask> executedTasks) {
        this.scope = scope;
        this.requestedInputSlots = requestedInputSlots;
        this.servingOutputSlots = servingOutputSlots;
        this.planImplementations = planImplementations;
        this.executedTasks = executedTasks;
    }

    /**
     * Create an instance for a single {@link ExecutionOperator}.
     *
     * @param operator the mentioned {@link ExecutionOperator}
     * @return the new instance
     */
    static PlanEnumeration createSingleton(final ExecutionOperator operator,
            final OptimizationContext optimizationContext) {
        final PlanEnumeration enumeration = createFor(operator, operator);
        final PlanImplementation singletonPlanImplementation = enumeration.createSingletonPartialPlan(operator,
                optimizationContext);
        enumeration.add(singletonPlanImplementation);
        return enumeration;
    }

    /**
     * Creates a new instance.
     *
     * @param inputOperator  provides the requested {@link InputSlot}s
     * @param outputOperator provides the requested {@link OutputSlot}s
     * @return the new instance
     */
    static PlanEnumeration createFor(final Operator inputOperator, final Operator outputOperator) {
        return createFor(inputOperator, input -> true, outputOperator, output -> true);
    }

    /**
     * Creates a new instance.
     *
     * @param inputOperator       provides the requested {@link InputSlot}s
     * @param inputSlotPredicate  can narrow down the {@link InputSlot}s
     * @param outputOperator      provides the requested {@link OutputSlot}s
     * @param outputSlotPredicate can narrow down the {@link OutputSlot}s
     * @return the new instance
     */
    static PlanEnumeration createFor(final Operator inputOperator,
            final Predicate<InputSlot<?>> inputSlotPredicate,
            final Operator outputOperator,
            final Predicate<OutputSlot<?>> outputSlotPredicate) {

        final PlanEnumeration instance = new PlanEnumeration();
        for (final InputSlot<?> inputSlot : inputOperator.getAllInputs()) {
            if (inputSlotPredicate.test(inputSlot)) {
                instance.requestedInputSlots.add(inputSlot);
            }
        }

        for (final OutputSlot outputSlot : outputOperator.getAllOutputs()) {
            if (outputSlotPredicate.test(outputSlot)) {
                List<InputSlot> inputSlots = outputSlot.getOccupiedSlots();
                if (inputSlots.isEmpty()) {
                    inputSlots = Collections.singletonList(null); // InputSlot is probably in a surrounding plan.
                }
                for (final InputSlot inputSlot : inputSlots) {
                    instance.servingOutputSlots.add(new Tuple<>(outputSlot, inputSlot));
                }
            }
        }

        return instance;
    }

    /**
     * Asserts that two given instances enumerate the same part of a
     * {@link WayangPlan}.
     */
    private static void assertMatchingInterface(final PlanEnumeration instance1, final PlanEnumeration instance2) {
        if (!instance1.requestedInputSlots.equals(instance2.requestedInputSlots)) {
            throw new IllegalArgumentException("Input slots are not matching.");
        }

        if (!instance1.servingOutputSlots.equals(instance2.servingOutputSlots)) {
            throw new IllegalArgumentException("Output slots are not matching.");
        }
    }

    /**
     * Concatenates the {@code baseEnumeration} via its {@code openOutputSlot} to
     * the {@code targetEnumerations}.
     * All {@link PlanEnumeration}s should be distinct.
     */
    public PlanEnumeration concatenate(final OutputSlot<?> openOutputSlot,
            final Collection<Channel> openChannels,
            final LinkedHashMap<InputSlot<?>, PlanEnumeration> targetEnumerations,
            final OptimizationContext optimizationContext,
            final TimeMeasurement enumerationMeasurement) {

        // Check the parameters' validity.
        assert this.getServingOutputSlots().stream()
                .map(Tuple::getField0)
                .anyMatch(openOutputSlot::equals)
                : String.format("Cannot concatenate %s: it is not a served output.", openOutputSlot);
        assert !targetEnumerations.isEmpty();

        final TimeMeasurement concatenationMeasurement = enumerationMeasurement == null ? null
                : enumerationMeasurement.start("Concatenation");

        if (logger.isInfoEnabled()) {
            final StringBuilder sb = new StringBuilder();
            sb.append("Concatenating ").append(this.getPlanImplementations().size());
            for (final PlanEnumeration targetEnumeration : targetEnumerations.values()) {
                sb.append("x").append(targetEnumeration.getPlanImplementations().size());
            }
            sb.append(" plan implementations.");
            logger.debug(sb.toString());
        }

        // Prepare the result instance from this instance.
        final PlanEnumeration result = new PlanEnumeration();
        result.scope.addAll(this.getScope());
        result.requestedInputSlots.addAll(this.getRequestedInputSlots());
        result.servingOutputSlots.addAll(this.getServingOutputSlots());
        result.executedTasks.putAll(this.getExecutedTasks());

        // Update the result instance from the target instances.
        for (final Entry<InputSlot<?>, PlanEnumeration> entry : targetEnumerations.entrySet()) {
            final PlanEnumeration targetEnumeration = entry.getValue();
            result.scope.addAll(targetEnumeration.getScope());
            result.requestedInputSlots.addAll(targetEnumeration.getRequestedInputSlots());
            result.servingOutputSlots.addAll(targetEnumeration.getServingOutputSlots());
            result.executedTasks.putAll(targetEnumeration.getExecutedTasks());
        }

        // NB: We need to store remove the InputSlots only here, because a single
        // targetEnumeration
        // might service multiple InputSlots. If this targetEnumeration is then also the
        // baseEnumeration, it might
        // re-request already serviced InputSlots, although already deleted.
        result.requestedInputSlots.removeAll(targetEnumerations.keySet());
        result.servingOutputSlots.removeIf(slotService -> slotService.getField0().equals(openOutputSlot));

        // Create the PlanImplementations.
        result.planImplementations.addAll(this.concatenatePartialPlans(
                openOutputSlot,
                openChannels,
                targetEnumerations,
                optimizationContext,
                result,
                concatenationMeasurement));

        logger.debug("Created {} plan implementations.", result.getPlanImplementations().size());
        if (concatenationMeasurement != null)
            concatenationMeasurement.stop();
        return result;
    }

    /**
     * Concatenates all {@link PlanImplementation}s of the {@code baseEnumeration}
     * via its {@code openOutputSlot}
     * to the {@code targetEnumerations}' {@link PlanImplementation}s.
     * All {@link PlanEnumeration}s should be distinct.
     */
    private Collection<PlanImplementation> concatenatePartialPlans(final OutputSlot<?> openOutputSlot,
            final Collection<Channel> openChannels,
            final LinkedHashMap<InputSlot<?>, PlanEnumeration> targetEnumerations,
            final OptimizationContext optimizationContext,
            final PlanEnumeration concatenationEnumeration,
            final TimeMeasurement concatenationMeasurement) {
        final Job job = optimizationContext.getJob();
        final OptimizationContext.OperatorContext operatorContext = optimizationContext
                .getOperatorContext(openOutputSlot.getOwner());
        final boolean isRequestBreakpoint = job.isRequestBreakpointFor(openOutputSlot, operatorContext);
        return this.concatenatePartialPlansBatchwise(
                openOutputSlot,
                openChannels,
                targetEnumerations,
                optimizationContext,
                isRequestBreakpoint,
                concatenationEnumeration,
                concatenationMeasurement);
    }

    /**
     * Concatenates {@link PlanEnumeration}s by batchwise processing of
     * {@link PlanImplementation}s. All {@link PlanImplementation}s
     * that share a certain implementation of the {@code openOutputSlot} or its fed
     * {@link InputSlot}s are grouped
     * into combinations so that we avoid to seek redundant {@link Junction}s.
     *
     * @param openOutputSlot           of this instance to be concatenated
     * @param targetEnumerations       whose {@link InputSlot}s should be
     *                                 concatenated with the {@code openOutputSlot}
     * @param optimizationContext      provides concatenation information
     * @param concatenationEnumeration to which the {@link PlanImplementation}s
     *                                 should be added
     * @param concatenationMeasurement
     * @param isRequestBreakpoint      whether a breakpoint-capable {@link Channel}
     *                                 should be inserted
     * @return the concatenated {@link PlanImplementation}s
     */
    private Collection<PlanImplementation> concatenatePartialPlansBatchwise(
            final OutputSlot<?> openOutputSlot,
            final Collection<Channel> openChannels,
            final LinkedHashMap<InputSlot<?>, PlanEnumeration> targetEnumerations,
            final OptimizationContext optimizationContext,
            final boolean isRequestBreakpoint,
            final PlanEnumeration concatenationEnumeration,
            final TimeMeasurement concatenationMeasurement) {

        // Preparatory initializations.
        final ChannelConversionGraph channelConversionGraph = optimizationContext.getChannelConversionGraph();

        // Allocate result collector.
        final Collection<PlanImplementation> result = new LinkedList<>();

        // Bring the InputSlots to fixed order.
        final List<InputSlot<?>> inputs = new ArrayList<>(targetEnumerations.keySet());

        // Identify identical PlanEnumerations among the targetEnumerations and
        // baseEnumeration.
        final LinkedMultiMap<PlanEnumeration, InputSlot<?>> targetEnumerationGroups = new LinkedMultiMap<>();
        for (final Entry<InputSlot<?>, PlanEnumeration> entry : targetEnumerations.entrySet()) {
            targetEnumerationGroups.putSingle(entry.getValue(), entry.getKey());
        }

        // Group the PlanImplementations within each enumeration group.
        final LinkedMultiMap<PlanEnumeration, PlanImplementation.ConcatenationGroupDescriptor> enum2concatGroup = new LinkedMultiMap<>();
        final LinkedMultiMap<PlanImplementation.ConcatenationGroupDescriptor, PlanImplementation.ConcatenationDescriptor> concatGroup2concatDescriptor = new LinkedMultiMap<>();
        for (final Entry<PlanEnumeration, LinkedHashSet<InputSlot<?>>> entry : targetEnumerationGroups.entrySet()) {
            final PlanEnumeration planEnumeration = entry.getKey();
            final OutputSlot<?> groupOutput = planEnumeration == this ? openOutputSlot : null;
            final LinkedHashSet<InputSlot<?>> groupInputSet = entry.getValue();
            final List<InputSlot<?>> groupInputs = new ArrayList<>(inputs.size());
            for (final InputSlot<?> input : inputs) {
                groupInputs.add(groupInputSet.contains(input) ? input : null);
            }
            for (final PlanImplementation planImplementation : planEnumeration.getPlanImplementations()) {
                final PlanImplementation.ConcatenationDescriptor concatDescriptor = planImplementation
                        .createConcatenationDescriptor(groupOutput, groupInputs);
                concatGroup2concatDescriptor.putSingle(concatDescriptor.groupDescriptor, concatDescriptor);
                enum2concatGroup.putSingle(planImplementation.getPlanEnumeration(), concatDescriptor.groupDescriptor);
            }
        }

        // Handle cases where this instance is not a target enumeration.
        if (!targetEnumerationGroups.containsKey(this)) {
            final List<InputSlot<?>> emptyGroupInputs = WayangCollections.createNullFilledArrayList(inputs.size());
            for (final PlanImplementation planImplementation : this.getPlanImplementations()) {
                final PlanImplementation.ConcatenationDescriptor concatDescriptor = planImplementation
                        .createConcatenationDescriptor(openOutputSlot, emptyGroupInputs);
                concatGroup2concatDescriptor.putSingle(concatDescriptor.groupDescriptor, concatDescriptor);
                enum2concatGroup.putSingle(planImplementation.getPlanEnumeration(), concatDescriptor.groupDescriptor);
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("Concatenating {}={} concatenation groups ({} -> {} inputs).",
                    enum2concatGroup.values().stream().map(groups -> String.valueOf(groups.size()))
                            .collect(Collectors.joining("*")),
                    enum2concatGroup.values().stream().mapToInt(Set::size).reduce(1, (a, b) -> a * b),
                    openOutputSlot,
                    targetEnumerations.size());
        }

        // Enumerate all combinations of the PlanEnumerations.
        final List<PlanEnumeration> orderedEnumerations = new ArrayList<>(enum2concatGroup.keySet());
        orderedEnumerations.remove(this);
        orderedEnumerations.add(0, this); // Make sure that the base enumeration is in the beginning.
        final List<LinkedHashSet<PlanImplementation.ConcatenationGroupDescriptor>> orderedConcatGroups = new ArrayList<>(
                orderedEnumerations.size());
        for (final PlanEnumeration enumeration : orderedEnumerations) {
            orderedConcatGroups.add(enum2concatGroup.get(enumeration));
        }
        for (final List<PlanImplementation.ConcatenationGroupDescriptor> concatGroupCombo : WayangCollections
                .streamedCrossProduct(orderedConcatGroups)) {
            // Determine the execution output along with its OptimizationContext.
            final PlanImplementation.ConcatenationGroupDescriptor baseConcatGroup = concatGroupCombo.get(0);
            final OutputSlot<?> execOutput = baseConcatGroup.execOutput;
            final LinkedHashSet<PlanImplementation.ConcatenationDescriptor> baseConcatDescriptors = concatGroup2concatDescriptor
                    .get(baseConcatGroup);
            final PlanImplementation innerPlanImplementation = WayangCollections
                    .getAny(baseConcatDescriptors).execOutputPlanImplementation;
            // The output should reside in the same OptimizationContext in all
            // PlanImplementations.
            assert baseConcatDescriptors.stream()
                    .map(cd -> cd.execOutputPlanImplementation)
                    .map(PlanImplementation::getOptimizationContext)
                    .collect(Collectors.toSet()).size() == 1;

            // Determine the execution OutputSlots.
            final List<InputSlot<?>> execInputs = new ArrayList<>(inputs.size());
            for (final PlanImplementation.ConcatenationGroupDescriptor concatGroup : concatGroupCombo) {
                for (final LinkedHashSet<InputSlot<?>> execInputSet : concatGroup.execInputs) {
                    if (execInputSet != null)
                        execInputs.addAll(execInputSet);
                }
            }

            // Construct a Junction between the ExecutionOperators.
            final Operator outputOperator = execOutput.getOwner();
            assert outputOperator.isExecutionOperator()
                    : String.format("Expected execution operator, found %s.", outputOperator);
            final TimeMeasurement channelConversionMeasurement = concatenationMeasurement == null ? null
                    : concatenationMeasurement.start("Channel Conversion");
            final Junction junction = openChannels == null || openChannels.isEmpty()
                    ? channelConversionGraph.findMinimumCostJunction(
                            execOutput,
                            execInputs,
                            innerPlanImplementation.getOptimizationContext(),
                            isRequestBreakpoint)
                    : channelConversionGraph.findMinimumCostJunction(
                            execOutput,
                            openChannels,
                            execInputs,
                            innerPlanImplementation.getOptimizationContext());
            if (channelConversionMeasurement != null)
                channelConversionMeasurement.stop();
            if (junction == null)
                continue;

            // If we found a junction, then we can enumerate all PlanImplementation
            // combinations.
            final List<LinkedHashSet<PlanImplementation>> groupPlans = WayangCollections.map(
                    concatGroupCombo,
                    concatGroup -> {
                        final LinkedHashSet<PlanImplementation.ConcatenationDescriptor> concatDescriptors = concatGroup2concatDescriptor
                                .get(concatGroup);
                        final LinkedHashSet<PlanImplementation> planImplementations = new LinkedHashSet<>(concatDescriptors.size());
                        for (final PlanImplementation.ConcatenationDescriptor concatDescriptor : concatDescriptors) {
                            planImplementations.add(concatDescriptor.getPlanImplementation());
                        }
                        return planImplementations;
                    });

            for (final List<PlanImplementation> planCombo : WayangCollections.streamedCrossProduct(groupPlans)) {
                final PlanImplementation basePlan = planCombo.get(0);
                final List<PlanImplementation> targetPlans = planCombo.subList(0, planCombo.size());
                final PlanImplementation concatenatedPlan = basePlan.concatenate(targetPlans, junction, basePlan,
                        concatenationEnumeration);
                if (concatenatedPlan != null) {
                    result.add(concatenatedPlan);
                }
            }
        }

        return result;
    }

    /**
     * Add a {@link PlanImplementation} to this instance.
     *
     * @param planImplementation to be added
     */
    public void add(final PlanImplementation planImplementation) {
        // TODO: Check if the plan conforms to this instance.
        this.planImplementations.add(planImplementation);
        assert planImplementation.getTimeEstimate() != null;
        planImplementation.setPlanEnumeration(this);
    }

    /**
     * Creates a new instance for exactly one {@link ExecutionOperator}.
     *
     * @param executionOperator   will be wrapped in the new instance
     * @param optimizationContext
     * @return the new instance
     */
    private PlanImplementation createSingletonPartialPlan(final ExecutionOperator executionOperator,
            final OptimizationContext optimizationContext) {
        return new PlanImplementation(
                this,
                new LinkedHashMap<>(0),
                Collections.singletonList(executionOperator),
                optimizationContext);
    }

    /**
     * Unions the {@link PlanImplementation}s of this and {@code that} instance. The
     * operation is in-place, i.e., this instance
     * is modified to form the result.
     *
     * @param that the instance to compute the union with
     */
    public void unionInPlace(final PlanEnumeration that) {
        assertMatchingInterface(this, that);
        this.scope.addAll(that.scope);
        that.planImplementations.forEach(partialPlan -> {
            this.planImplementations.add(partialPlan);
            partialPlan.setPlanEnumeration(this);
        });
        that.planImplementations.clear();
    }

    /**
     * Create a new instance that equals this instance but redirects via
     * {@link OperatorAlternative.Alternative#getSlotMapping()}.
     *
     * @param alternative the alternative to escape or {@code null} if none (in that
     *                    case, this method returns the
     *                    this instance)
     */
    public PlanEnumeration escape(final OperatorAlternative.Alternative alternative) {
        if (alternative == null)
            return this;
        final PlanEnumeration escapedInstance = new PlanEnumeration();
        final OperatorAlternative operatorAlternative = alternative.getOperatorAlternative();

        // Copy and widen the scope.
        escapedInstance.scope.addAll(this.scope);
        escapedInstance.scope.add(operatorAlternative);

        // Escape the input slots.
        for (final InputSlot inputSlot : this.requestedInputSlots) {
            final InputSlot escapedInput = alternative.getSlotMapping().resolveUpstream(inputSlot);
            if (escapedInput != null) {
                escapedInstance.requestedInputSlots.add(escapedInput);
            }
        }

        // Escape the output slots.
        for (final Tuple<OutputSlot<?>, InputSlot<?>> link : this.servingOutputSlots) {
            if (link.field1 != null) {
                throw new IllegalStateException("Cannot escape a connected output slot.");
            }
            final Collection<OutputSlot<Object>> resolvedOutputSlots = alternative.getSlotMapping()
                    .resolveDownstream(link.field0.unchecked());
            for (final OutputSlot escapedOutput : resolvedOutputSlots) {
                final List<InputSlot<?>> occupiedInputs = escapedOutput.getOccupiedSlots();
                if (occupiedInputs.isEmpty()) {
                    escapedInstance.servingOutputSlots.add(new Tuple<>(escapedOutput, null));
                } else {
                    for (final InputSlot inputSlot : occupiedInputs) {
                        escapedInstance.servingOutputSlots.add(new Tuple<>(escapedOutput, inputSlot));
                    }
                }
            }
        }

        // Escape the PlanImplementation instances.
        for (final PlanImplementation planImplementation : this.planImplementations) {
            escapedInstance.planImplementations.add(planImplementation.escape(alternative, escapedInstance));
        }

        return escapedInstance;
    }

    public List<PlanImplementation> getPlanImplementations() {
        return this.planImplementations;
    }

    public LinkedHashSet<InputSlot<?>> getRequestedInputSlots() {
        return this.requestedInputSlots;
    }

    public LinkedHashSet<Tuple<OutputSlot<?>, InputSlot<?>>> getServingOutputSlots() {
        return this.servingOutputSlots;
    }

    public LinkedHashSet<OperatorAlternative> getScope() {
        return this.scope;
    }

    public LinkedHashMap<ExecutionOperator, ExecutionTask> getExecutedTasks() {
        return this.executedTasks;
    }

    @Override
    public String toString() {
        return this.toIOString();
    }

    @SuppressWarnings("unused")
    private String toIOString() {
        return String.format("%s[%dx, inputs=%s, outputs=%s]", this.getClass().getSimpleName(),
                this.getPlanImplementations().size(),
                this.requestedInputSlots, this.servingOutputSlots.stream()
                        .map(Tuple::getField0)
                        .distinct()
                        .collect(Collectors.toList()));
    }

    @SuppressWarnings("unused")
    private String toScopeString() {
        return String.format("%s[%dx %s]", this.getClass().getSimpleName(),
                this.getPlanImplementations().size(),
                this.scope);
    }
}
