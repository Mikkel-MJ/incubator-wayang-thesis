export WORKDIR=/work/lsbo-paper
export SPARK_HOME=/opt/spark
export HADOOP_HOME=/opt/hadoop
export PATH="$PATH:${SPARK_HOME}/bin"
export SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*"
export FLINK_VERSION=1.20.0
export FLINK_HOME=/opt/flink
export PATH="$PATH:${FLINK_HOME}/bin"
export GIRAPH_VERSION=1.3.0
export GIRAPH_HOME=/opt/giraph
export PATH="$PATH:${GIRAPH_HOME}/bin"

cd ${WORKDIR}
cd wayang-0.7.1

data_path=/work/lsbo-paper/data/JOBenchmark/data
timings_path=/work/lsbo-paper/data/JOBenchmark/data/executions
test_path=/work/lsbo-paper/data/JOBenchmark/queries/BaseQuerySplit/test
model_path=/work/lsbo-paper/data/models/imdb/bqs/bvae.onnx

echo "Running JOBenchmark"

#for i in {0..5}; do
    for query in "$test_path"/*.sql; do
        ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.JOBenchmark java,spark,flink,postgres $data_path/ $timings_path/ $query
    done

    #for query in "$test_path"/*.sql; do
    #        ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.JOBenchmark java,spark,flink,postgres $data_path/ $timings_path $query bvae $model_path $data_path
    #done
#done

