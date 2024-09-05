package org.apache.wayang.ml.benchmarks;

import com.google.common.io.Resources;
import org.apache.wayang.api.sql.context.SqlContext;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;

import java.nio.charset.Charset;
import java.util.Collection;


public class IMDBJOBenchmark {
    public static void main(String[] args){
        try {
            Configuration configuration = new Configuration();
            configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/job");
            configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
            configuration.setProperty("wayang.postgres.jdbc.password", "postgres");

            String calciteModel = Resources.toString(
                    IMDBJOBenchmark.class.getResource("/calcite-model.json"),
                    Charset.defaultCharset());
            configuration.setProperty("wayang.calcite.model", calciteModel);

            SqlContext sqlContext = new SqlContext(configuration);

            Collection<Record> result = sqlContext.executeSql(
                "select * from job.aka_name"
            );

            result.stream().forEach(e -> System.out.println(e));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
