package org.apache.wayang.ml.benchmarks;

import com.google.common.io.Resources;

import org.apache.commons.lang.StringUtils;
import org.apache.wayang.api.sql.context.SqlContext;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;


public class IMDBJOBenchmark {
    /**
     * Benchmarking tool for the imdb/jo benchmark, Calcite dictates that every jo query follows
     * the schema, "schema_name.table_name". The tool searches for the queries in resources/calcite-ready-job-queries
     * @param args args[0]: path to calcite-job-ready-queries/*.sql
     */
    public static void main(String[] args){
        try {
            Configuration configuration = new Configuration();

            String calciteModel = Resources.toString(
                IMDBJOBenchmark.class.getResource("/calcite-model.json"),
                Charset.defaultCharset());

            configuration.setProperty("wayang.calcite.model", calciteModel);
            configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://host.docker.internal:5432/job");
            configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
            configuration.setProperty("wayang.postgres.jdbc.password", "postgres");

            configuration.setProperty(
                "wayang.ml.executions.file",
                "mle" + ".txt"
            );

            configuration.setProperty(
                "wayang.ml.optimizations.file",
                "mlo" + ".txt"
            );

            configuration.setProperty("wayang.ml.experience.enabled", "false");


            SqlContext sqlContext = new SqlContext(configuration);

            Path pathToQuery = Paths.get(args[0]);
            String query = StringUtils.chop(Files.readString(pathToQuery).stripTrailing()); //need to chop off the last ';' otherwise sqlContext cant parse it
            System.out.println("Read query: " + query);
            
            Collection<Record> result = sqlContext.executeSql(
                query
            );

            result.stream().forEach(e -> System.out.println(e));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
 