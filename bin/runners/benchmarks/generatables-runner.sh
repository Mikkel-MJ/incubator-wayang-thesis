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

classifier_path=/work/lsbo-paper/python-ml/src/Models/tpch/classifier.onnx
retrained_classifier_path=/work/lsbo-paper/python-ml/src/Models/tpch/fresh-retrained.classifier.onnx

data_path=/work/lsbo-paper/data/benchmarks/tpch/data
experience_path=/work/lsbo-paper/data/experience/

for query in {0..29}; do
    echo "Benchmarking Test data with native optimizer"
    #./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.GeneratableBenchmarks java,spark,flink,postgres file://$data_path/ /work/lsbo-paper/data/benchmarks/tpch/native/ $query
    ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.GeneratableBenchmarks java,spark,flink,postgres file://$data_path/ /work/lsbo-paper/data/benchmarks/tpch/classifier/ $query bvae $classifier_path  $experience_path
    ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.GeneratableBenchmarks java,spark,flink,postgres file://$data_path/ /work/lsbo-paper/data/benchmarks/tpch/classifier/retrained/ $query bvae $retrained_classifier_path $experience_path
done

