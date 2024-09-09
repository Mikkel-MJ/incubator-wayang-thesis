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
import org.apache.wayang.api.python.executor.ProcessFeeder;
import org.apache.wayang.api.python.executor.ProcessReceiver;

import org.apache.wayang.apps.util.Parameters;
import org.apache.wayang.core.util.ExplainUtils;
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
 *  -- Samples each runtime and saves the best
 *  -- Starts the retraining and replaces model
 */
public class LSBOSampler {

    public static void main(String[] args) {
        try {
            Socket socket = setupSocket();
            List<String> testMessage = new ArrayList<String>();
            testMessage.add("Hello from Wayang!");

            List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(args[0]));
            Configuration config = new Configuration();
            config.load(ReflectionUtils.loadResource("wayang-api-python-defaults.properties"));

            HashMap<String, WayangPlan> plans = TPCH.createPlans("/var/www/html/data");
            WayangPlan plan = plans.get("query1");
            Job wayangJob = new WayangContext(config).createJob("", plan, "");
            wayangJob.estimateKeyFigures();
            OneHotMappings.setOptimizationContext(wayangJob.getOptimizationContext());
            OneHotMappings.encodeIds = true;
            TreeNode wayangNode = TreeEncoder.encode(plan);
            String encodedInput = wayangNode.toString() + ":" + wayangNode.toString() + ":1";
            ArrayList<String> input = new ArrayList<>();
            input.add(encodedInput);

            ProcessFeeder<String, String> feed = new ProcessFeeder<>(
                socket,
                ByteString.copyFromUtf8(""),
                input
            );

            feed.send();

            // Wait for a sampled plan
            ProcessReceiver<String> r = new ProcessReceiver<>(socket);
            List<String> lsboSamples = new ArrayList<>();
            r.getIterable().iterator().forEachRemaining(lsboSamples::add);
            List<WayangPlan> decodedPlans = LSBO.decodePlans(lsboSamples, wayangNode);
            WayangPlan sampledPlan = decodedPlans.get(0);

            // execute each WayangPlan and sample latency
            // encode the best one
            ArrayList<String> resampleEncodings = new ArrayList<>();

            // Get the initial plan created by the LSBO loop
            WayangContext executionContext = new WayangContext(config);
            plugins.stream().forEach(plug -> executionContext.register(plug));

            ExplainUtils.parsePlan(sampledPlan, false);
            TreeNode encoded = TreeEncoder.encode(sampledPlan);
            long execTime = Long.MAX_VALUE;

            try {
                Instant start = Instant.now();
                executionContext.execute(sampledPlan, "");
                Instant end = Instant.now();
                execTime = Duration.between(start, end).toMillis();

                encodedInput = wayangNode.toString() + ":" + encoded.toString() + ":" + execTime;
                System.out.println(encodedInput);

                ArrayList<String> latency = new ArrayList<>();
                latency.add(encodedInput);

                System.out.println(input);

                ProcessFeeder<String, String> latencyFeed = new ProcessFeeder<>(
                    socket,
                    ByteString.copyFromUtf8(""),
                    latency
                );

                latencyFeed.send();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(e);
            }

        } catch(Exception e) {
            e.printStackTrace();
            System.out.println(e);
        }

        //TODO:
        // Return execution time back to python
        // Exit
    }

    private static Socket setupSocket() throws IOException {
        Socket socket;
        ServerSocket serverSocket;

        byte[] addr = new byte[4];
        addr[0] = 127; addr[1] = 0; addr[2] = 0; addr[3] = 1;

        /*TODO should NOT be assigned an specific port, set port as 0 (zero)*/
        serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(addr));
        serverSocket.setSoTimeout(10000);

        // This is read from the python process to retrieve it
        System.out.println(serverSocket.getLocalPort());

        socket = serverSocket.accept();
        serverSocket.setSoTimeout(0);

        return socket;
    }
}
