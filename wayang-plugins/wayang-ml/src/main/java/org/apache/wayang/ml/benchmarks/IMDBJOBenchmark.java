package org.apache.wayang.ml.benchmarks;

import org.apache.commons.lang.StringUtils;
import org.apache.wayang.api.sql.context.SqlContext;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.flink.Flink;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.apps.imdb.data.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;

public class IMDBJOBenchmark {
    public static SqlContext sqlContext;

    public static WayangPlan getWayangPlan(
        final String path,
        final Configuration configuration,
        final Plugin[] plugins,
        final String... udfJars
    ) throws SQLException, IOException, org.apache.calcite.sql.parser.SqlParseException {
        sqlContext = new SqlContext(configuration, plugins);
        final Path pathToQuery = Paths.get(path);

        // need to chop off the last ';' otherwise sqlContext cant parse it
        final String query = StringUtils.chop(Files.readString(pathToQuery).stripTrailing());
        System.out.println("[Query]: " + query);

        WayangPlan plan = sqlContext.buildWayangPlan(query, udfJars);

        //((LinkedList<Operator> )plan.getSinks()).get(0).addTargetPlatform(Java.platform());

        return plan;
    }

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
            final String query = StringUtils.chop(Files.readString(pathToQuery).stripTrailing()); // need to chop off
                                                                                                  // the last
            // ';' otherwise sqlContext
            // cant parse it
            System.out.println("Read query: " + query);

            final Collection<Record> result = sqlContext.executeSql(
                    query);

            System.out.println(result.stream().limit(50).collect(Collectors.toList()));
            System.out.println("\nResults: " + " amount of records: " + result.size());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(5);
        }
    }

    // Only source in postgres, compute elsewhere
    public static void setSources(WayangPlan plan, String dataPath) {
        /*
        final Collection<Operator> operators = PlanTraversal.upstream().traverse(plan.getSinks()).getTraversedNodes();

        operators.forEach(o -> {
            if (!(o.isSource() || o.isSink())) {
                o.addTargetPlatform(Spark.platform());
                o.addTargetPlatform(Flink.platform());
                o.addTargetPlatform(Java.platform());
            }
        });*/

        final Collection<Operator> sources = plan.collectReachableTopLevelSources();

        sources.stream().forEach(op -> {
            if (op instanceof TableSource) {
                String tableName = ((TableSource) op).getTableName();
                String filePath = dataPath + tableName + ".csv";
                TextFileSource replacement = new TextFileSource(filePath, "UTF-8");

                MapOperator<String, Record> parser;

                /*
                if (tableName.equals("movie_companies")) {
                    parser = new MapOperator<String, Record>(
                        new TransformationDescriptor<>(
                            line -> {
                                //System.out.println(line);
                                //System.out.println(line.replaceAll("\"", "\\\""));
                                //Object[] values = new Object[]{25010,104018,6,1,"(2008) (USA) (TV)"};
                                //Record comp = new Record(values);
                                Object[] parsed = MovieCompanies.toArray(MovieCompanies.parseCsv(line));
                                Record record = new Record(parsed);

                                return record;
                            },
                            DataUnitType.createBasic(String.class),
                            DataUnitType.createBasicUnchecked(Record.class)
                        ),
                        DataSetType.createDefault(String.class),
                        DataSetType.createDefault(new RecordType(MovieCompanies.getFields()))
                    );

                    MapOperator<Record, Record> projection = MapOperator.createProjection(
                            (RecordType) ((TableSource) op).getType().getDataUnitType(),
                            MovieCompanies.getFields()
                    );

                    assert projection.getOutputType().getDataUnitType().equals(((TableSource) op).getType().getDataUnitType());

                    replacement.connectTo(0, parser, 0);
                    parser.connectTo(0, projection, 0);
                    System.out.println("Replacing: " + op);
                    System.out.println("Type: " + ((TableSource) op).getType().getDataUnitType());
                    System.out.println("Replacement Type: " + parser.getOutputType().getDataUnitType());

                    OutputSlot.stealConnections(op, projection);
                }*/

                switch (tableName) {
                    /*
                    case "movie_companies": parser = new MapOperator<>(
                            (line) -> {
                                Record record = new Record(MovieCompanies.toArray(MovieCompanies.parseCsv(line)));
                                return record;
                            },
                            String.class,
                            Record.class
                        );
                        OutputSlot.stealConnections(op, parser);

                        replacement.connectTo(0, parser, 0);

                        break;
                    case "aka_name":
                        parser = new MapOperator<>(
                            (line) -> {
                                return new Record(AkaName.toArray(AkaName.parseCsv(line)));
                            },
                            String.class,
                            Record.class
                        );
                        OutputSlot.stealConnections(op, parser);

                        replacement.connectTo(0, parser, 0);
                        break;
                    */
                    case "comp_cast_type":
                        parser = new MapOperator<>(
                            (line) -> { return new Record(CompCastType.toArray(CompCastType.parseCsv(line)));
                            },
                            String.class,
                            Record.class
                        );
                        OutputSlot.stealConnections(op, parser);

                        replacement.connectTo(0, parser, 0);
                        break;
                    case "company_name":
                        parser = new MapOperator<>(
                            (line) -> {
                                return new Record(CompanyName.toArray(CompanyName.parseCsv(line)));
                            },
                            String.class,
                            Record.class
                        );
                        OutputSlot.stealConnections(op, parser);

                        replacement.connectTo(0, parser, 0);
                        break;
                    case "info_type":
                        parser = new MapOperator<>(
                            (line) -> {
                                return new Record(InfoType.toArray(InfoType.parseCsv(line)));
                            },
                            String.class,
                            Record.class
                        );
                        OutputSlot.stealConnections(op, parser);

                        replacement.connectTo(0, parser, 0);
                        break;
                    /*
                    case "movie_info":
                        parser = new MapOperator<>(
                            (line) -> {
                                return new Record(MovieInfo.toArray(MovieInfo.parseCsv(line)));
                            },

                            String.class,
                            Record.class
                        );
                        OutputSlot.stealConnections(op, parser);

                        replacement.connectTo(0, parser, 0);
                        break;
                    case "person_info":
                        parser = new MapOperator<>(
                            (line) -> {
                                return new Record(PersonInfo.toArray(PersonInfo.parseCsv(line)));
                            },
                            String.class,
                            Record.class
                        );
                        OutputSlot.stealConnections(op, parser);

                        replacement.connectTo(0, parser, 0);
                        break;
                    case "movie_keyword":
                        parser = new MapOperator<>(
                            (line) -> {
                                return new Record(MovieKeyword.toArray(MovieKeyword.parseCsv(line)));
                            },
                            String.class,
                            Record.class
                        );

                        replacement.connectTo(0, parser, 0);

                        OutputSlot.stealConnections(op, parser);

                        break;
                    case "cast_info":
                        parser = new MapOperator<>(
                            (line) -> {
                                return new Record(CastInfo.toArray(CastInfo.parseCsv(line)));
                            },
                            String.class,
                            Record.class
                        );
                        OutputSlot.stealConnections(op, parser);

                        replacement.connectTo(0, parser, 0);
                        break;
                    */
                    case "movie_link":
                        parser = new MapOperator<>(
                            (line) -> {
                                return new Record(MovieLink.toArray(MovieLink.parseCsv(line)));
                            },
                            String.class,
                            Record.class
                        );
                        OutputSlot.stealConnections(op, parser);

                        replacement.connectTo(0, parser, 0);
                        break;
                    default:
                        break;
                }
            }
        });
    }
}
