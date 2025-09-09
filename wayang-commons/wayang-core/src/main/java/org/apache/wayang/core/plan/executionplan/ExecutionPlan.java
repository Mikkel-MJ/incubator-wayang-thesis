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

package org.apache.wayang.core.plan.executionplan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.core.optimizer.enumeration.ExecutionTaskFlow;
import org.apache.wayang.core.optimizer.enumeration.StageAssignmentTraversal;
import org.apache.wayang.core.util.Counter;

/**
 * Represents an executable, cross-platform data flow. Consists of muliple
 * {@link PlatformExecution}s.
 */
public class ExecutionPlan {

    /**
     * Creates a new instance from the given {@link ExecutionTaskFlow}.
     *
     * @param executionTaskFlow       should be converted into an
     *                                {@link ExecutionPlan}
     * @param stageSplittingCriterion defines where to install
     *                                {@link ExecutionStage} boundaries
     * @return the new instance
     */
    public static ExecutionPlan createFrom(final ExecutionTaskFlow executionTaskFlow,
            final StageAssignmentTraversal.StageSplittingCriterion stageSplittingCriterion) {
        return StageAssignmentTraversal.assignStages(executionTaskFlow, stageSplittingCriterion);
    }

    private final Logger logger = LogManager.getLogger(this.getClass());

    /**
     * All {@link ExecutionStage}s without predecessors that need be executed at
     * first.
     */
    private final Collection<ExecutionStage> startingStages = new LinkedList<>();

    public void addStartingStage(final ExecutionStage executionStage) {
        this.startingStages.add(executionStage);
    }

    public Collection<ExecutionStage> getStartingStages() {
        return this.startingStages;
    }

    /**
     * Creates a {@link String} representation (not strictly ordered) of this
     * instance.
     *
     * @return the {@link String} representation
     */
    public String toExtensiveString() {
        return this.toExtensiveString(false);
    }

    /**
     * Creates a {@link String} representation of this instance.
     *
     * @param isStriclyOrdering whether {@link ExecutionStage}s should be listed
     *                          only after <i>all</i> their predecessors
     * @return the {@link String} representation
     */
    public String toExtensiveString(final boolean isStriclyOrdering) {
        final StringBuilder sb = new StringBuilder();
        final Counter<ExecutionStage> stageActivationCounter = new Counter<>();
        final Queue<ExecutionStage> activatedStages = new LinkedList<>(this.startingStages);
        final Set<ExecutionStage> seenStages = new HashSet<>();
        while (!activatedStages.isEmpty()) {
            while (!activatedStages.isEmpty()) {
                final ExecutionStage stage = activatedStages.poll();
                if (!seenStages.add(stage))
                    continue;
                sb.append(">>> ").append(stage).append(":\n");
                stage.getPlanAsString(sb, "> ");
                sb.append("\n");

                for (final ExecutionStage successor : stage.getSuccessors()) {
                    final int count = stageActivationCounter.add(successor, 1);
                    if (!isStriclyOrdering || count == successor.getPredecessors().size() || successor.isLoopHead()) {
                        activatedStages.add(successor);
                    }
                }
            }
        }

        return sb.toString();
    }

    /**
     * TODO: ExecutionPlan.toJsonList add documentation
     * labels:documentation,todo
     *
     * @return
     */
    public List<Map> toJsonList() {
        final Counter<ExecutionStage> stageActivationCounter = new Counter<>();
        final Queue<ExecutionStage> activatedStages = new LinkedList<>(this.startingStages);
        final Set<ExecutionStage> seenStages = new HashSet<>();
        final ArrayList<Map> allStages = new ArrayList<>();

        while (!activatedStages.isEmpty()) {
            final ExecutionStage stage = activatedStages.poll();
            if (!seenStages.add(stage))
                continue;
            final Map stageMap = stage.toJsonMap();

            // Better way to put sequence number ?
            stageMap.put("sequence_number", allStages.size());
            allStages.add(stageMap);
            for (final ExecutionStage successor : stage.getSuccessors()) {
                final int count = stageActivationCounter.add(successor, 1);
                if (count == successor.getPredecessors().size() || successor.isLoopHead()) {
                    activatedStages.add(successor);
                }
            }
        }

        return allStages;
    }

    /**
     * Scrap {@link Channel}s and {@link ExecutionTask}s that are not within the
     * given {@link ExecutionStage}s.
     *
     * @return {@link Channel}s from that consumer {@link ExecutionTask}s have been
     *         removed
     */
    public Set<Channel> retain(final Set<ExecutionStage> retainableStages) {
        final Set<Channel> openChannels = new HashSet<>();
        for (final ExecutionStage stage : retainableStages) {
            for (final Channel channel : stage.getOutboundChannels()) {
                if (channel.retain(retainableStages)) {
                    openChannels.add(channel);
                }
            }
            stage.retainSuccessors(retainableStages);
            stage.getPlatformExecution().retain(retainableStages);
        }
        return openChannels;
    }

    /**
     * Collects all {@link ExecutionStage}s in this instance.
     *
     * @return the {@link ExecutionStage}s
     */
    public Set<ExecutionStage> getStages() {
        final Set<ExecutionStage> seenStages = new HashSet<>();
        final Queue<ExecutionStage> openStages = new LinkedList<>(this.getStartingStages());
        while (!openStages.isEmpty()) {
            final ExecutionStage stage = openStages.poll();
            if (seenStages.add(stage)) {
                openStages.addAll(stage.getSuccessors());
            }
        }
        return seenStages;
    }

    /**
     * Collects all {@link ExecutionTask}s of this instance.
     *
     * @return the {@link ExecutionTask}s
     */
    public LinkedHashSet<ExecutionTask> collectAllTasks() {
        final LinkedHashSet<ExecutionTask> allTasks = new LinkedHashSet<>();
        for (final ExecutionStage stage : this.getStages()) {
            allTasks.addAll(stage.getAllTasks());
        }
        return allTasks;
    }

    /**
     * The given instance should build upon the open {@link Channel}s of this
     * instance. Then, this instance will be
     * expanded with the content of the given instance.
     *
     * @param expansion extends this instance, but they are not overlapping
     */
    public void expand(final ExecutionPlan expansion) {
        for (final Channel openChannel : expansion.getOpenInputChannels()) {
            openChannel.mergeIntoOriginal();
            final Channel original = openChannel.getOriginal();
            final ExecutionStage producerStage = original.getProducer().getStage();
            assert producerStage != null : String.format("No stage found for %s.", original.getProducer());
            for (final ExecutionTask consumer : original.getConsumers()) {
                final ExecutionStage consumerStage = consumer.getStage();
                assert consumerStage != null : String.format("No stage found for %s.", consumer);
                // Equal stages possible on "partially open" Channels.
                if (producerStage != consumerStage) {
                    producerStage.addSuccessor(consumerStage);
                }
            }
        }
    }

    /**
     * Collects all {@link Channel}s that are copies. We recognize them as
     * {@link Channel}s that are open.
     */
    public Collection<Channel> getOpenInputChannels() {
        return this.collectAllTasks().stream()
                .flatMap(task -> Arrays.stream(task.getInputChannels()))
                .filter(Channel::isCopy)
                .collect(Collectors.toList());
    }

    /**
     * Implements various sanity checks. Problems are logged.
     */
    public boolean isSane() {
        // 1. Check if every ExecutionTask is assigned an ExecutionStage.
        final Set<ExecutionTask> allTasks = this.collectAllTasks();
        final boolean isAllTasksAssigned = allTasks.stream().allMatch(task -> task.getStage() != null);
        if (!isAllTasksAssigned) {
            this.logger.error("There are tasks without stages.");
        }

        final Set<Channel> allChannels = allTasks.stream()
                .flatMap(task -> Stream.concat(Arrays.stream(task.getInputChannels()),
                        Arrays.stream(task.getOutputChannels())))
                .collect(Collectors.toSet());

        final boolean isAllChannelsOriginal = allChannels.stream()
                .allMatch(channel -> !channel.isCopy());
        if (!isAllChannelsOriginal) {
            this.logger.error("There are channels that are copies.");
        }

        boolean isAllSiblingsConsistent = true;
        for (final Channel channel : allChannels) {
            for (final Channel sibling : channel.getSiblings()) {
                if (!allChannels.contains(sibling)) {
                    this.logger.error("A sibling of {}, namely {}, seems to be invalid.", channel, sibling);
                    isAllSiblingsConsistent = false;
                }
            }
        }

        return isAllTasksAssigned && isAllChannelsOriginal && isAllSiblingsConsistent;
    }
}
