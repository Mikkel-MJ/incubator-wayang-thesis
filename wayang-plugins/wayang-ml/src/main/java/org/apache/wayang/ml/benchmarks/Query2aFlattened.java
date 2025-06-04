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
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.JoinFlattenResult;
import org.apache.wayang.api.sql.calcite.converter.joinhelpers.MultiConditionJoinKeyExtractor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.util.ExplainUtils;

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

public class Query2aFlattened {

    /**
     * 0: platforms
     * 1: Data directory
     */
    public static String psqlUser = "postgres";
    public static String psqlPassword = "postgres";

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

        TransformationDescriptor<Record, Integer> leftKeyDescriptor = new TransformationDescriptor<>(
            (mc) -> mc.getInt(1),
            Record.class,
            Integer.class
        ).withSqlImplementation("movie_companies AS movie_companies", "movie_id");

        TransformationDescriptor<Record, Integer> rightKeyDescriptor = new TransformationDescriptor<>(
            (mk) -> mk.getInt(1),
            Record.class,
            Integer.class
        ).withSqlImplementation("movie_keyword AS movie_keyword", "movie_id");

        JoinOperator<Record, Record, Integer> mcmkJoin = new JoinOperator<Record, Record, Integer>(
            leftKeyDescriptor,
            rightKeyDescriptor
        );

        movieCompanies.connectTo(0, mcmkJoin, 0);
        movieKeyword.connectTo(0, mcmkJoin, 1);

        MapOperator<Tuple2<Record, Record>, Record> mcmkFlatten = new MapOperator<Tuple2<Record, Record>, Record>(
                new JoinFlattenResult(),
                ReflectionUtils.specify(Tuple2.class),
                Record.class);

        mcmkJoin.connectTo(0, mcmkFlatten, 0);

        JoinOperator<Record, Record, Integer> mcmkkJoin = new JoinOperator<>(
            (mcmk) -> mcmk.getInt(7),
            (k) -> k.getInt(0),
            Record.class,
            Record.class,
            Integer.class
        );

        mcmkFlatten.connectTo(0, mcmkkJoin, 0);
        kFilter.connectTo(0, mcmkkJoin, 1);

        MapOperator<Tuple2<Record, Record>, Record> mcmkkFlatten = new MapOperator<Tuple2<Record, Record>, Record>(
                new JoinFlattenResult(),
                ReflectionUtils.specify(Tuple2.class),
                Record.class);

        mcmkkJoin.connectTo(0, mcmkkFlatten, 0);

        JoinOperator<Record, Record, Integer> mcmkkcnJoin = new JoinOperator<>(
            (mcmkk) -> mcmkk.getInt(5),
            (cn) -> cn.getInt(0),
            Record.class,
            Record.class,
            Integer.class
        );

        mcmkkFlatten.connectTo(0, mcmkkcnJoin, 0);
        cnFilter.connectTo(0, mcmkkcnJoin, 1);

        MapOperator<Tuple2<Record, Record>, Record> mcmkkcnFlatten = new MapOperator<Tuple2<Record, Record>, Record>(
                new JoinFlattenResult(),
                ReflectionUtils.specify(Tuple2.class),
                Record.class);

        mcmkkcnJoin.connectTo(0, mcmkkcnFlatten, 0);

        JoinOperator<Record, Record, Integer> mcmkkcntJoin = new JoinOperator<>(
            (mcmkkcn) -> mcmkkcn.getInt(11),
            (t) -> t.getInt(0),
            Record.class,
            Record.class,
            Integer.class
        );

        mcmkkcnFlatten.connectTo(0, mcmkkcntJoin, 0);
        title.connectTo(0, mcmkkcntJoin, 1);

        MapOperator<Tuple2<Record, Record>, Record> mcmkkcntFlatten = new MapOperator<Tuple2<Record, Record>, Record>(
                new JoinFlattenResult(),
                ReflectionUtils.specify(Tuple2.class),
                Record.class);

        mcmkkcntJoin.connectTo(0, mcmkkcntFlatten, 0);

        MapOperator<Record, String> flatten = new MapOperator<Record, String>(
                (t) -> t.getString(19),
                Record.class,
                String.class);

        mcmkkcntFlatten.connectTo(0, flatten, 0);

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

        ExplainUtils.parsePlan(plan, false);

        WayangContext context = new WayangContext(config);
        plugins.stream().forEach(plug -> context.register(plug));

        String[] jars = ArrayUtils.addAll(
            ReflectionUtils.getAllJars(Query2aFlattened.class),
            ReflectionUtils.getLibs(Query2aFlattened.class)
        );

        context.execute(plan, jars);
    }
}
