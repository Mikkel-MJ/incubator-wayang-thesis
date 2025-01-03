package org.apache.wayang.ml.benchmarks;

import com.amazonaws.services.iot.model.SqlParseException;

import org.apache.calcite.runtime.CalciteException;
import org.apache.commons.lang.StringUtils;
import org.apache.wayang.api.sql.context.SqlContext;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.flink.Flink;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.spark.Spark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.stream.Collectors;

public class IMDBJOBenchmark {
    /**
     * Benchmarking tool for the imdb/jo benchmark, Calcite dictates that every jo
     * query follows
     * the schema, "schema_name.table_name". The tool searches for the queries in
     * resources/calcite-ready-job-queries
     *
     * @param args args[0]: path to calcite-job-ready-queries/*.sql
     */
    public static void main(final String[] args) throws Exception {
        try {
            final Configuration configuration = new Configuration();

            final String calciteModel = "{\n" +
                    "    \"version\": \"1.0\",\n" +
                    "    \"defaultSchema\": \"wayang\",\n" +
                    "    \"schemas\": [\n" +
                    "        {\n" +
                    "            \"name\": \"postgres\",\n" +
                    "            \"type\": \"custom\",\n" +
                    "            \"factory\": \"org.apache.wayang.api.sql.calcite.jdbc.JdbcSchema$Factory\",\n" +
                    "            \"operand\": {\n" +
                    "                \"jdbcDriver\": \"org.postgresql.Driver\",\n" +
                    "                \"jdbcUrl\": \"jdbc:postgresql://job:5432/job\",\n" +
                    "                \"jdbcUser\": \"postgres\",\n" +
                    "                \"jdbcPassword\": \"postgres\"\n" +
                    "            }\n" +
                    "        }\n" +
                    "    ]\n" +
                    "}";


            configuration.setProperty("org.apache.calcite.sql.parser.parserTracing", "true");
            configuration.setProperty("wayang.calcite.model", calciteModel);
            configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://job:5432/job");
            configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
            configuration.setProperty("wayang.postgres.jdbc.password", "postgres");

            configuration.setProperty(
                    "wayang.ml.executions.file",
                    "mle" + ".txt");

            configuration.setProperty(
                    "wayang.ml.optimizations.file",
                    "mlo" + ".txt");

            configuration.setProperty("wayang.ml.experience.enabled", "false");

            final SqlContext sqlContext = new SqlContext(configuration, Spark.basicPlugin(), Postgres.plugin(),
                    Java.basicPlugin(), Flink.basicPlugin());
            // SqlContext sqlContext = new SqlContext(configuration, Postgres.plugin(),
            // Flink.basicPlugin(), Flink.conversionPlugin(),
            // Java.channelConversionPlugin());
            // SqlContext sqlContext = new SqlContext(configuration, Postgres.plugin(),
            // Spark.basicPlugin(), Spark.conversionPlugin(), Flink.conversionPlugin(),
            // Java.channelConversionPlugin());
            // SqlContext sqlContext = new SqlContext(configuration, Postgres.plugin(),
            // Java.channelConversionPlugin());

            final Path pathToQuery = Paths.get(args[0]);
            final String query = StringUtils.chop(Files.readString(pathToQuery).stripTrailing()); // need to chop off the last
                                                                                            // ';' otherwise sqlContext
                                                                                            // cant parse it
            System.out.println("Read query: " + query);

            final Collection<Record> result = sqlContext.executeSql(
                    query);

            System.out.println(result.stream().limit(50).collect(Collectors.toList()));
            System.out.println("\nResults: " + " amount of records: " + result.size());
        } catch (final IndexOutOfBoundsException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (final CalciteException e) {
            e.printStackTrace();
            System.exit(2);
        } catch (final SqlParseException e) {
            e.printStackTrace();
            System.exit(3);
        } catch (final Exception e) {
            e.printStackTrace();
            System.exit(4);
        } catch (final Error e) {
            e.printStackTrace();
            System.exit(5);
        }
    }
}
