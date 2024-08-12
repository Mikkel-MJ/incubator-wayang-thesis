
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
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml.MLContext;
import org.apache.wayang.spark.Spark;

import org.apache.wayang.api.python.executor.PythonWorkerManager;

import org.apache.wayang.apps.util.Parameters;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.ml.costs.PairwiseCost;
import org.apache.wayang.ml.costs.PointwiseCost;
import org.apache.wayang.ml.training.TPCH;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import scala.collection.Seq;
import scala.collection.JavaConversions;
import com.google.protobuf.ByteString;

public class LSBO {
    public static void main(String[] args) {
        Configuration config = new Configuration();
        config.load(ReflectionUtils.loadResource("wayang-api-python-defaults.properties"));

        config.setProperty(
            "wayang.api.python.worker",
            "/var/www/html/wayang-plugins/wayang-ml/src/main/python/worker.py"
        );

        final ArrayList<String> input = new ArrayList<>();
        input.add("INPUT1");
        input.add("INPUT2");

        final PythonWorkerManager<String, String> manager = new PythonWorkerManager<>(ByteString.copyFromUtf8(""), input, config);
        final Iterable<String> output = manager.execute();

        if (output.iterator().hasNext()) {
            System.out.println("Python worker output: " + output.iterator().next());
        }
    }
}
