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

package org.apache.wayang.ml.benchmarks;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml.MLContext;
import org.apache.wayang.spark.Spark;

import org.apache.wayang.api.python.executor.ProcessFeeder;
import org.apache.wayang.api.python.executor.ProcessReceiver;

import org.apache.wayang.apps.util.Parameters;
import org.apache.wayang.core.util.ExplainUtils;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.ml.costs.PairwiseCost;
import org.apache.wayang.ml.costs.PointwiseCost;
import org.apache.wayang.ml.training.LSBO;
import org.apache.wayang.ml.training.TPCH;
import org.apache.wayang.apps.tpch.queries.Query1Wayang;
import org.apache.wayang.apps.tpch.queries.Query3;
import org.apache.wayang.apps.tpch.queries.Query5;
import org.apache.wayang.apps.tpch.queries.Query6;
import org.apache.wayang.apps.tpch.queries.Query10;
import org.apache.wayang.apps.tpch.queries.Query12;
import org.apache.wayang.apps.tpch.queries.Query14;
import org.apache.wayang.apps.tpch.queries.Query19;
import org.apache.wayang.ml.encoding.OneHotMappings;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.api.DataQuanta;
import org.apache.wayang.api.PlanBuilder;
import org.apache.wayang.ml.training.GeneratableJob;
import org.apache.wayang.ml.util.Jobs;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.Duration;
import java.net.InetAddress;
import java.net.Socket;
import java.net.ServerSocket;

import scala.collection.Seq;
import scala.collection.JavaConversions;
import com.google.protobuf.ByteString;

/**
 * TODO:
 *  - Move this to a class so that LSBO is a utility function
 *  -- Takes wayang plan as input
 *  -- Encodes it and sends it to python
 *  -- Receives set of encoded strings from latent space
 *  -- Executes each of those on the original plan
 */
public class LSBORunner {

    public static void main(String[] args) {
        List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(args[0]));
        Class<? extends GeneratableJob> job = Jobs.getJob(Integer.parseInt(args[2]));
        Configuration config = new Configuration();
        config.load(ReflectionUtils.loadResource("wayang-api-python-defaults.properties"));
        config.setProperty("spark.master", "spark://spark-cluster:7077");
        config.setProperty("spark.app.name", "TPC-H Benchmark Query " + args[2]);
        config.setProperty("spark.executor.memory", "16g");
        config.setProperty("wayang.flink.run", "distribution");
        config.setProperty("wayang.flink.parallelism", "1");
        config.setProperty("wayang.flink.master", "flink-cluster");
        config.setProperty("wayang.flink.port", "6123");
        config.setProperty("spark.executor.memory", "16g");
        config.setProperty("spark.driver.maxResultSize", "4G");

        String[] jars = new String[]{
            ReflectionUtils.getDeclaringJar(LSBORunner.class),
            ReflectionUtils.getDeclaringJar(Query1Wayang.class),
            ReflectionUtils.getDeclaringJar(Query3.class),
            ReflectionUtils.getDeclaringJar(Query5.class),
            ReflectionUtils.getDeclaringJar(Query6.class),
            ReflectionUtils.getDeclaringJar(Query10.class),
            ReflectionUtils.getDeclaringJar(Query12.class),
            ReflectionUtils.getDeclaringJar(Query14.class),
            ReflectionUtils.getDeclaringJar(Query19.class),
            ReflectionUtils.getDeclaringJar(DataQuanta.class),
        };

        /*
        HashMap<String, WayangPlan> plans = TPCH.createPlans(args[1]);
        WayangPlan plan = plans.get("query" + args[2]);
        */

        try {
            Constructor<?> cnstr = job.getDeclaredConstructors()[0];
            GeneratableJob createdJob = (GeneratableJob) cnstr.newInstance();
            String[] jobArgs = {args[0], args[1]};
            DataQuanta<?> quanta = createdJob.buildPlan(jobArgs);
            PlanBuilder builder = quanta.getPlanBuilder();
            WayangPlan plan = builder.build();

            LSBO.process(plan, config, plugins, jars);
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }

    }
}
