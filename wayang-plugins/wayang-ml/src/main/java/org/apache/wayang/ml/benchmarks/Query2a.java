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
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.flink.Flink;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.ml.MLContext;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.api.DataQuanta;
import org.apache.wayang.api.PlanBuilder;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.apps.util.Parameters;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.ml.costs.PairwiseCost;
import org.apache.wayang.ml.costs.PointwiseCost;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.util.ExplainUtils;
import org.apache.wayang.api.sql.context.SqlContext;
import org.apache.wayang.apps.tpch.queries.Query1Wayang;
import org.apache.wayang.api.DataQuanta;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.calcite.sql.parser.SqlParseException;

import org.apache.wayang.basic.operators.*;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.apps.imdb.data.*;

import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import scala.collection.Seq;
import scala.collection.JavaConversions;
import java.util.Collection;
import java.io.BufferedWriter;
import java.io.FileWriter;

public class Query2a {

    /**
     * 0: platforms
     * 1: Data directory
     */
    public static String psqlUser = "ucloud";
    public static String psqlPassword = "ucloud";

    public static void main(String[] args) {
        List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(args[0]));
        Configuration config = new Configuration();

        config.setProperty("spark.master", "spark://spark-cluster:7077");
        config.setProperty("spark.app.name", "JOB Query");
        config.setProperty("spark.rpc.message.maxSize", "2047");
        config.setProperty("spark.executor.memory", "32g");
        config.setProperty("spark.executor.cores", "6");
        config.setProperty("spark.executor.instances", "1");
        config.setProperty("spark.default.parallelism", "8");
        config.setProperty("spark.driver.maxResultSize", "16g");
        config.setProperty("spark.dynamicAllocation.enabled", "true");
        config.setProperty("wayang.flink.mode.run", "distribution");
        config.setProperty("wayang.flink.parallelism", "4");
        config.setProperty("wayang.flink.master", "flink-cluster");
        config.setProperty("wayang.flink.port", "7071");
        config.setProperty("wayang.flink.rest.client.max-content-length", "200MiB");
        config.setProperty("wayang.ml.experience.enabled", "false");
        /*
        config.setProperty(
            "wayang.core.optimizer.pruning.strategies",
            "org.apache.wayang.core.optimizer.enumeration.TopKPruningStrategy,org.apache.wayang.core.optimizer.enumeration.LatentOperatorPruningStrategy"
        );
        config.setProperty("wayang.core.optimizer.pruning.topk", "100");
        */
        config.setProperty("org.apache.calcite.sql.parser.parserTracing", "true");
        config.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://job:5432/job"); config.setProperty("wayang.postgres.jdbc.user", psqlUser);
        config.setProperty("wayang.postgres.jdbc.password", psqlPassword);

        String dataPath = args[1];

        TextFileSource companyName = new TextFileSource(dataPath + "company_name.csv");
        TableSource keyword = new PostgresTableSource("keyword");
        TableSource movieCompanies = new PostgresTableSource("movie_companies");
        TableSource movieKeyword = new PostgresTableSource("movie_keyword");
        TableSource movieKeywordTwo = new PostgresTableSource("movie_keyword");
        TableSource title = new PostgresTableSource("title");

        MapOperator<String, Record> parser = new MapOperator<>(
            (line) -> {
                return new Record(CompanyName.toArray(CompanyName.parseCsv(line)));
            },
            String.class,
            Record.class
        );

        companyName.connectTo(0, parser, 0);

        FilterOperator<Record> cnFilter = new FilterOperator<>(
            (rec) -> rec.getString(2) != null ? rec.getString(2).equals("[de]") : false,
            Record.class
        );

        parser.connectTo(0, cnFilter, 0);

        FilterOperator<Record> kFilter = new FilterOperator<>(
            (rec) -> rec.getString(1) != null ? rec.getString(1).equals("character-name-in-title") : false,
            Record.class
        );

        keyword.connectTo(0, kFilter, 0);

        JoinOperator<Record, Record, Integer> cnmcJoin = new JoinOperator<>(
            (cn) -> cn.getInt(0),
            (mc) -> mc.getInt(2),
            Record.class,
            Record.class,
            Integer.class
        );

        cnFilter.connectTo(0, cnmcJoin, 0);
        movieCompanies.connectTo(0, cnmcJoin, 1);

        JoinOperator<Tuple2<Record, Record>, Record, Integer> mctJoin = new JoinOperator<>(
            (mc) -> mc.getField1().getInt(1),
            (t) -> t.getInt(0),
            ReflectionUtils.specify(Tuple2.class),
            Record.class,
            Integer.class
        );

        cnmcJoin.connectTo(0, mctJoin, 0);
        title.connectTo(0, mctJoin, 1);

        JoinOperator<Tuple2<Tuple2<Record, Record>, Record>, Record, Integer> tmkJoin = new JoinOperator<>(
            (t) -> t.getField1().getInt(0),
            (mk) -> mk.getInt(1),
            ReflectionUtils.specify(Tuple2.class),
            Record.class,
            Integer.class
        );

        mctJoin.connectTo(0, tmkJoin, 0);
        movieKeyword.connectTo(0, tmkJoin, 1);

        JoinOperator<Tuple2<Tuple2<Tuple2<Record, Record>, Record>, Record>, Record, Integer> mkkJoin = new JoinOperator<>(
            (mk) -> mk.getField1().getInt(2),
            (k) -> k.getInt(0),
            ReflectionUtils.specify(Tuple2.class),
            Record.class,
            Integer.class
        );

        tmkJoin.connectTo(0, mkkJoin, 0);
        kFilter.connectTo(0, mkkJoin, 1);

        JoinOperator<Tuple2<Tuple2<Tuple2<Tuple2<Record, Record>, Record>, Record>, Record>, Record, Integer> mcmkJoin = new JoinOperator<>(
            (mc) -> mc.getField0().getField0().getField0().getField1().getInt(1),
            (mk) -> mk.getInt(1),
            ReflectionUtils.specify(Tuple2.class),
            Record.class,
            Integer.class
        );

        mkkJoin.connectTo(0, mcmkJoin, 0);
        movieKeywordTwo.connectTo(0, mcmkJoin, 1);

        MapOperator<Tuple2<Tuple2<Tuple2<Tuple2<Tuple2<Record, Record>, Record>, Record>, Record>, Record>, String> flatten = new MapOperator<>(
            (tup) -> tup.getField0().getField0().getField0().getField1().getString(1),
            ReflectionUtils.specify(Tuple2.class),
            String.class
        );

        mcmkJoin.connectTo(0, flatten, 0);

        ReduceByOperator<String, String> aggregation = new ReduceByOperator<>(
            (tup) -> tup,
            (t1, t2) -> t1.compareTo(t2) < 0 ? t1 : t2,
            String.class,
            String.class
        );

        flatten.connectTo(0, aggregation, 0);

        LocalCallbackSink<String> sink = LocalCallbackSink.createStdoutSink(String.class);

        aggregation.connectTo(0, sink, 0);

        WayangPlan plan = new WayangPlan(sink);

        WayangContext context = new WayangContext(config);
        plugins.stream().forEach(plug -> context.register(plug));

        String[] jars = ArrayUtils.addAll(
            ReflectionUtils.getAllJars(Query2a.class),
            ReflectionUtils.getLibs(Query2a.class)
        );

        context.execute(plan, jars);
    }
}
