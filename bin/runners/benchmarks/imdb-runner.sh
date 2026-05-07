

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
timings_path=/work/lsbo-paper/data/benchmarks/job/complex
test_path=/work/lsbo-paper/data/JOBenchmark/queries/complex

classifier_path=/work/lsbo-paper/python-ml/src/Models/imdb/classifier.onnx
retrained_classifier_path=/work/lsbo-paper/python-ml/src/Models/imdb/retrain.classifier.onnx
cost_path=/work/lsbo-paper/python-ml/src/Models/imdb/nativeml.onnx

echo "Running JOBenchmark"

#for query in $(ls -1 "$test_path"/*.sql | tail -n 85); do
for query in "$test_path"/*.sql; do
#for i in {0..2}; do
#for query in $(ls "$test_path"/*.sql | tail -n +14); do
#for query_name in "${queries[@]}"; do
#    query="$test_path"/"$query_name".sql
        #./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.JOBenchmark java,spark,flink,postgres file://$data_path/ $timings_path/native/ $query
        #./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.JOBenchmark java,spark,flink,postgres file://$data_path/ $timings_path/classifier/ $query bvae $classifier_path $data_path/experience/
        #./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.JOBenchmark java,spark,flink,postgres file://$data_path/ $timings_path/classifier/retrained/ $query bvae $retrained_classifier_path $data_path/experience/
        ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.JOBenchmark java,spark,postgres file://$data_path/ $timings_path/cost/ $query cost $cost_path $data_path/experience/cost/


        # Lord forgive me - for Flink has sinned
        #sudo ssh -o StrictHostKeyChecking=no root@flink-cluster sudo /opt/flink/bin/stop-cluster.sh
        #sudo ssh -o StrictHostKeyChecking=no root@flink-cluster sudo /opt/flink/bin/start-cluster.sh
        #sleep 1s
    #done
done
