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
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml.MLContext;
import org.apache.wayang.spark.Spark;

import org.apache.wayang.apps.util.Parameters;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.ml.costs.PairwiseCost;
import org.apache.wayang.ml.costs.PointwiseCost;
import org.apache.wayang.ml.training.TPCH;

import java.util.HashMap;
import java.util.List;
import scala.collection.Seq;
import scala.collection.JavaConversions;

public class TPCHBenchmarks {

    /**
     * 0: platforms
     * 1: TPCH data set directory path
     * 2: Directory to write timings to
     * 3: query number
     * 4: model type
     * 5: model path
     */
    public static void main(String[] args) {
        try {
            List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(args[0]));
            Configuration config = new Configuration();
            String modelType = "";

            if (args.length > 4) {
                modelType = args[4];
            }

            if (args.length > 5) {
                TPCHBenchmarks.setMLModel(config, modelType, args[5]);
            }

            String executionTimeFile = args[2] + "query" + args[3] + "-executions";
            String optimizationTimeFile = args[2] + "query" + args[3] + "-optimizations";

            if (!"".equals(modelType)) {
                executionTimeFile += "-" + modelType;
                optimizationTimeFile += "-" + modelType;
            }

            config.setProperty(
                "wayang.ml.executions.file",
                executionTimeFile + ".txt"
            );

            config.setProperty(
                "wayang.ml.optimizations.file",
                optimizationTimeFile + ".txt"
            );

            final MLContext wayangContext = new MLContext(config);
            plugins.stream().forEach(plug -> wayangContext.register(plug));

            HashMap<String, WayangPlan> plans = TPCH.createPlans(args[1]);
            WayangPlan plan = plans.get("query" + args[3]);

            System.out.println(modelType);
            if (!"vae".equals(modelType)) {
                System.out.println("Executing query " + args[3]);
                wayangContext.execute(plan, "");
                System.out.println("Finished execution");
            } else {
                System.out.println("Using vae cost model");
                System.out.println("Executing query " + args[3]);
                wayangContext.executeVAE(plan, "");
                System.out.println("Finished execution");

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setMLModel(Configuration config, String modelType, String path) {
        config.setProperty(
            "wayang.ml.model.file",
            path
        );

        switch(modelType) {
            case "cost":
                config.setCostModel(new PointwiseCost());
                System.out.println("Using cost ML Model");

                break;
            case "pairwise":
                config.setCostModel(new PairwiseCost());

                System.out.println("Using pairwise ML Model");
                break;
            case "vae":
                System.out.println("Using vae ML Model");
                break;
            default:
                System.out.println("Using default cost Model");
                break;
        }

    }

}
