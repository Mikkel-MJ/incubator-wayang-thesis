export WORKDIR=/work/lsbo-paper
export DEPENDENCIES_DIR="${WORKDIR}/dependencies"
export HADOOP_HOME="${DEPENDENCIES_DIR}/hadoop"
export SPARK_HOME="${DEPENDENCIES_DIR}/spark"
export PATH="$PATH:${SPARK_HOME}/bin"
export SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*"
export FLINK_VERSION=1.20.0
export FLINK_HOME="${DEPENDENCIES_DIR}/flink"
export PATH="$PATH:${FLINK_HOME}/bin"
export GIRAPH_VERSION=1.3.0
export GIRAPH_HOME="${DEPENDENCIES_DIR}/giraph"
export PATH="$PATH:${GIRAPH_HOME}/bin"

cd ${WORKDIR}
cd wayang-0.7.1

data_path=/work/lsbo-paper/data/JOBenchmark/data
timings_path=/work/lsbo-paper/data/JOBenchmark/data/executions
test_path=/work/lsbo-paper/data/JOBenchmark/queries
model_path=/work/lsbo-paper/data/models/imdb/bqs/bvae.onnx

echo "Running JOBenchmark"

    #for query in $(ls -1 "$test_path"/*.sql | tail -n 85); do
    for query in "$test_path"/*.sql; do
    #for query in $(ls "$test_path"/*.sql | tail -n +14); do
    #for query_name in "${selected_queries[@]}"; do

    #    query="$test_path/${query_name}.sql"

        #timeout --kill-after=30m --foreground 30m ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.JOBenchmark java,spark,flink,postgres file://$data_path/ $timings_path/ $query
        #timeout --kill-after=10m --foreground 10m ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.JOBenchmark java,spark,flink,postgres file://$data_path/ $timings_path/ $query
        #if [ $? -eq 124 ]; then
        #    echo "Query ${query} timed out"
        #fi
        #timeout --kill-after=30m --foreground 30m ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.JOBenchmark java,spark,flink,postgres file://$data_path/ $timings_path/bvae/ $query bvae /work/lsbo-paper/python-ml/src/Models/imdb/bvae-1.onnx $data_path/experience/
        timeout --kill-after=30m --foreground 30m ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.JOBenchmark java,spark,flink,postgres file://$data_path/ $timings_path/bvae/retrained/new_data_only/1/ $query bvae /work/lsbo-paper/python-ml/src/Models/imdb/retrained/new_data_only/bvae-1.onnx $data_path/experience/
        timeout --kill-after=30m --foreground 30m ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.JOBenchmark java,spark,flink,postgres file://$data_path/ $timings_path/bvae/retrained/new_data_only/2/ $query bvae /work/lsbo-paper/python-ml/src/Models/imdb/retrained/new_data_only/bvae-2.onnx $data_path/experience/
        timeout --kill-after=30m --foreground 30m ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.JOBenchmark java,spark,flink,postgres file://$data_path/ $timings_path/bvae/retrained/new_hyperparameters/1/ $query bvae /work/lsbo-paper/python-ml/src/Models/imdb/retrained/new_hyperparameters/bvae-1.onnx $data_path/experience/
        timeout --kill-after=30m --foreground 30m ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.JOBenchmark java,spark,flink,postgres file://$data_path/ $timings_path/bvae/retrained/new_hyperparameters/2/ $query bvae /work/lsbo-paper/python-ml/src/Models/imdb/retrained/new_hyperparameters/bvae-2.onnx $data_path/experience/


        # Lord forgive me - for Flink has sinned
        sudo ssh -o StrictHostKeyChecking=no root@flink-cluster sudo /opt/flink/bin/stop-cluster.sh
        sudo ssh -o StrictHostKeyChecking=no root@flink-cluster sudo /opt/flink/bin/start-cluster.sh
        sleep 1s
    done

