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

bvae_path=/work/lsbo-paper/data/models/bvae.onnx
cost_poath=/work/lsbo-paper/data/models/cost.onnx

data_path=/work/lsbo-paper/data
experience_path=/work/lsbo-paper/data/experience/

echo "Benchmarking Test data with BVAE"
for query in {801..900}; do
    ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.GeneratableBenchmarks java,spark,flink,giraph $data_path/ $data_path/benchmarks/generatables/ $query bvae $bvae_path $experience_path
done

echo "Benchmarking Test data with Cost model"
for query in {801..900}; do
    ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.GeneratableBenchmarks java,spark,flink,giraph $data_path/ $data_path/benchmarks/generatables/ $query cost $cost_path $experience_path
done
