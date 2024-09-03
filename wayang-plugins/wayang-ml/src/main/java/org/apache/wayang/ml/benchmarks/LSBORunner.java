
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

import org.apache.wayang.api.python.executor.PythonWorkerManager;

import org.apache.wayang.apps.util.Parameters;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.ml.costs.PairwiseCost;
import org.apache.wayang.ml.costs.PointwiseCost;
import org.apache.wayang.ml.training.LSBO;
import org.apache.wayang.ml.training.TPCH;
import org.apache.wayang.ml.encoding.OneHotMappings;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
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
 *  -- Samples each runtime and saves the best
 *  -- Starts the retraining and replaces model
 */
public class LSBORunner {
    public static void main(String[] args) {
        List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(args[0]));
        Configuration config = new Configuration();
        config.load(ReflectionUtils.loadResource("wayang-api-python-defaults.properties"));

        config.setProperty(
            "wayang.api.python.worker",
            "/var/www/html/wayang-plugins/wayang-ml/src/main/python/python-ml/src/lsbo_worker.py"
        );

        config.setProperty(
            "wayang.api.python.path",
            "/var/www/html/wayang-plugins/wayang-ml/src/main/python/python-ml/venv/bin/python3.11"
        );

        HashMap<String, WayangPlan> plans = TPCH.createPlans("/var/www/html/data");
        WayangPlan plan = plans.get("query1");
        Job wayangJob = new WayangContext(config).createJob("", plan, "");
        wayangJob.estimateKeyFigures();
        OneHotMappings.setOptimizationContext(wayangJob.getOptimizationContext());
        OneHotMappings.encodeIds = true;
        TreeNode wayangNode = TreeEncoder.encode(plan);

        Tuple<TreeNode, List<String>> sampled = LSBO.process(plan, config, plugins);

        // reconstruct tensors from the json sampled plans
        List<WayangPlan> decodedPlans = LSBO.decodePlans(sampled.field1, sampled.field0);

        // execute each WayangPlan and sample latency
        // encode the best one
        WayangContext executionContext = new WayangContext(config);

        ArrayList<String> resampleEncodings = new ArrayList<>();

        for (WayangPlan sampledPlan: decodedPlans) {
            TreeNode encoded = TreeEncoder.encode(sampledPlan);
            executionContext.execute(sampledPlan, "");

            resampleEncodings.add(wayangNode.toString() + ":" + encoded.toString() + ":3000");
        }

        config.setProperty(
            "wayang.api.python.worker",
            "/var/www/html/wayang-plugins/wayang-ml/src/main/python/python-ml/src/lsbo_resampler.py"
        );

        Tuple<TreeNode, List<String>> useless = LSBO.process(plan, config, plugins);
    }
}
