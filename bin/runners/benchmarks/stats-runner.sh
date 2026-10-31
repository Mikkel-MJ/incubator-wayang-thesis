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

data_path=/work/lsbo-paper/data/benchmarks/stats/data
timings_path=/work/lsbo-paper/data/benchmarks/stats
test_path=/work/lsbo-paper/data/benchmarks/stats/queries
experience_path=/work/lsbo-paper/data/experience/

classifier_path=/work/lsbo-paper/python-ml/src/Models/stats/classifier.onnx
job_classifier_path=/work/lsbo-paper/python-ml/src/Models/imdb/classifier.onnx
retrained_classifier_path=/work/lsbo-paper/python-ml/src/Models/stats/retrain.classifier.onnx
cost_path=/work/lsbo-paper/python-ml/src/Models/stats/retrain.cost.onnx

echo "Running STATSBenchmark"

skip=0  # Number of queries to skip

i=0
for query in "$test_path"/*.sql; do
    if (( i < skip )); then
        echo "Skipping $query"
        (( i++ ))
        continue
    fi

    for j in {0..2}; do
        timeout --kill-after=30m --foreground 30m ./bin/wayang-submit -Xmx16g org.apache.wayang.ml.benchmarks.STATSBenchmark java,spark,postgres file://$data_path/ $timings_path/native/spark/ $query
        #timeout --kill-after=30m --foreground 30m ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.STATSBenchmark java,spark,postgres file://$data_path/ $timings_path/classifier/retrained/ $query bvae $retrained_classifier_path $experience_path
        #timeout --kill-after=30m --foreground 30m ./bin/wayang-submit -Xmx16g org.apache.wayang.ml.benchmarks.STATSBenchmark java,spark,postgres file://$data_path/ $timings_path/cost/retrained/ $query cost $cost_path $experience_path
    done

    (( i++ ))
done

