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

package org.apache.wayang.ml.training;

import org.apache.wayang.ml.encoding.OneHotMappings;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.ml.util.Jobs;
import org.apache.wayang.api.DataQuanta;
import org.apache.wayang.api.PlanBuilder;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.optimizer.enumeration.PlanEnumerator;
import org.apache.wayang.core.optimizer.enumeration.PlanEnumeration;
import org.apache.wayang.commons.util.profiledb.instrumentation.StopWatch;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Subject;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.optimizer.enumeration.ExecutionTaskFlow;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.ml.util.CardinalitySampler;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.logging.log4j.Level;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Set;
import java.time.Instant;
import java.time.Duration;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.Collection;
import java.util.Collections;

public class Encode {

    public static void main(String[] args) {
        encodeJob(args[0], args[1], args[2], Integer.valueOf(args[3]), true);
    }

    /*
     * args format:
     * 1: platforms, comma sep. (string)
     * 2: tpch file path
     * 3: encode to file path (string)
     * 4: job index for the job to run (int)
     * 5: overwrite cardinalities (boolean)
     **/
    public static void encodeJob(String platforms, String dataPath, String encodePath, int index, boolean rewriteCardinalities) {
        int counter = 0;
        Class<? extends GeneratableJob> job = Jobs.getJob(index);

        try {
            FileWriter fw = new FileWriter(encodePath, true);
            BufferedWriter writer = new BufferedWriter(fw);

            System.out.println(job);

            Constructor<?> cnstr = job.getDeclaredConstructors()[0];
            GeneratableJob createdJob = (GeneratableJob) cnstr.newInstance();
            String[] jobArgs = {platforms, dataPath};

            DataQuanta<?> quanta = createdJob.buildPlan(jobArgs);
            PlanBuilder builder = quanta.getPlanBuilder();
            WayangContext context = builder.getWayangContext();
            Configuration config = context.getConfiguration();
            WayangPlan plan = builder.build();
            Job wayangJob = context.createJob("", plan, "");
            context.setLogLevel(Level.ERROR);
            buildPlanImplementations(wayangJob, plan, context, writer);
                //CardinalitySampler.readFromFile(path);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static void buildPlanImplementations(Job job, WayangPlan wayangPlan, WayangContext wayangContext, BufferedWriter writer) {
        ExecutionPlan baseplan = job.buildInitialExecutionPlan();
        OneHotMappings.setOptimizationContext(job.getOptimizationContext());
        TreeNode wayangNode = TreeEncoder.encode(wayangPlan);

        Experiment experiment = new Experiment("wayang-ml-test", new Subject("Wayang", "0.1"));
        StopWatch stopWatch = new StopWatch(experiment);
        TimeMeasurement optimizationRound = stopWatch.getOrCreateRound("optimization");
        final PlanEnumerator planEnumerator = new PlanEnumerator(wayangPlan, job.getOptimizationContext());

        final TimeMeasurement enumerateMeasurment = optimizationRound.start("Create Initial Execution Plan", "Enumerate");
        planEnumerator.setTimeMeasurement(enumerateMeasurment);
        final PlanEnumeration comprehensiveEnumeration = planEnumerator.enumerate(true);
        planEnumerator.setTimeMeasurement(null);
        optimizationRound.stop("Create Initial Execution Plan", "Enumerate");

        Collection<PlanImplementation> planImplementations = comprehensiveEnumeration.getPlanImplementations();

        System.out.println("Encoding " + planImplementations.size() + " execution plans");

        for (PlanImplementation planImplementation : planImplementations) {
            try {
                TreeNode planImplNode = TreeEncoder.encode(planImplementation).withIdsFrom(wayangNode);
                writer.write(String.format("%s:%s:%d", wayangNode.toStringEncoding(), planImplNode.toStringEncoding(), planImplementation.getTimeEstimate().getUpperEstimate()));
                writer.newLine();
                writer.flush();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        planImplementations = Collections.emptyList();
    }
}
