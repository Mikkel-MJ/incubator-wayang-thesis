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
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.ml.util.CardinalitySampler;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.logging.log4j.Level;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.ElementaryOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorBase;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.reflect.Constructor;
import java.util.Set;
import java.util.Collection;
import java.time.Instant;
import java.time.Duration;
import java.util.Comparator;
import java.util.stream.Collectors;

public class Training {

    /*
     * args format:
     * 1: platforms, comma sep. (string)
     * 2: tpch file path
     * 3: encode to file path (string)
     * 4: job index for the job to run (int)
     * 5: overwrite skipConversions (boolean)
     **/
    public static void main(String[] args) {
        Class<? extends GeneratableJob> job = Jobs.getJob(Integer.parseInt(args[3]));

        System.out.println("Running job " + args[3] + " : " + job.getName());
        try {
            Constructor<?> cnstr = job.getDeclaredConstructors()[0];
            GeneratableJob createdJob = (GeneratableJob) cnstr.newInstance();
            String[] jobArgs = {args[0], args[1]};
            FileWriter fw = new FileWriter(args[2], true);
            BufferedWriter writer = new BufferedWriter(fw);

            String[] jars = new String[]{
                ReflectionUtils.getDeclaringJar(Training.class),
                ReflectionUtils.getDeclaringJar(createdJob.getClass()),
            };

            boolean skipConversions = false;

            if (args.length == 5) {
                skipConversions = Boolean.valueOf(args[4]);
            }
            /*
            * TODO:
            *  - Get DataQuanta's WayangPlan :done:
            *  - Encode WayangPlan and print/store :done:
            *  -- We need to run it once before, so that we can get card estimates :done:
            *  - Call .buildInitialExecutionPlan for the WayangPlan :done:
            *  - Encode the ExecutionPlan and print/store :done:
            *  - Make cardinalities configurable
            */

            DataQuanta<?> quanta = createdJob.buildPlan(jobArgs);
            PlanBuilder builder = quanta.getPlanBuilder();
            WayangContext context = builder.getWayangContext();
            Configuration config = context.getConfiguration();
            config.setProperty("wayang.ml.experience.enabled", "false");
            config.setProperty("spark.master", "spark://spark-cluster:7077");
            config.setProperty("spark.app.name", "TPC-H Benchmark Query " + args[3]);
            config.setProperty("spark.executor.memory", "16g");
            config.setProperty("wayang.flink.run", "distribution");
            config.setProperty("wayang.flink.parallelism", "1");
            config.setProperty("wayang.flink.master", "flink-cluster");
            config.setProperty("wayang.flink.port", "6123");
            config.setProperty("spark.app.name", "TPC-H Benchmark Query " + args[3]);
            config.setProperty("spark.executor.memory", "16g");

            WayangPlan plan = builder.build();

            /*int hashCode = new HashCodeBuilder(17, 37).append(plan).toHashCode();
            String path = "/var/www/html/data/" + hashCode + "-cardinalities.json";*/
            long execTime = 0;

            //if (overwriteCardinalities) {
                //CardinalitySampler.configureWriteToFile(config, path);
                //
            Job wayangJob = context.createJob("", plan, jars);
            ExecutionPlan exPlan = wayangJob.buildInitialExecutionPlan();
            OneHotMappings.setOptimizationContext(wayangJob.getOptimizationContext());
            TreeNode wayangNode = TreeEncoder.encode(plan);
            TreeNode execNode = TreeEncoder.encode(exPlan, skipConversions).withIdsFrom(wayangNode);
            System.out.println(exPlan.toExtensiveString());

            quanta = createdJob.buildPlan(jobArgs);
            builder = quanta.getPlanBuilder();
            context = builder.getWayangContext();
            context.setLogLevel(Level.INFO);
            config = context.getConfiguration();
            config.setProperty("wayang.ml.experience.enabled", "false");
            config.setProperty("spark.master", "spark://spark-cluster:7077");
            config.setProperty("spark.app.name", "TPC-H Benchmark Query " + args[3]);
            config.setProperty("spark.executor.memory", "16g");
            config.setProperty("wayang.flink.run", "distribution");
            config.setProperty("wayang.flink.parallelism", "1");
            config.setProperty("wayang.flink.master", "flink-cluster");
            config.setProperty("wayang.flink.port", "6123");
            config.setProperty("spark.app.name", "TPC-H Benchmark Query " + args[3]);
            config.setProperty("spark.executor.memory", "16g");
            plan = builder.build();

            /*
            Instant start = Instant.now();
            context.execute(plan, jars);
            Instant end = Instant.now();
            execTime = Duration.between(start, end).toMillis();*/

            //CardinalitySampler.readFromFile(path);

            //writer.write(String.format("%s:%s:%d", wayangNode.toString(), execNode.toString(), execTime));
            writer.write(String.format("%s:%s:%d", wayangNode.toString(), execNode.toString(), 1_000_000));
            writer.newLine();
            writer.flush();
          } catch(Exception e) {
              e.printStackTrace();
          }
    }
}
