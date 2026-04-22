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
train_encode_path=/work/lsbo-paper/data/benchmarks/stats/encodings/classifier.train.txt
train_path=/work/lsbo-paper/data/benchmarks/stats/queries

echo "Encoding training data with native optimizer"

for query in "$train_path"/*.sql; do
#for query in {0..30}; do
    #for i in {0..99}; do
    ./bin/wayang-submit org.apache.wayang.ml.training.TrainingCandidates java,spark,flink,postgres file:///work/lsbo-paper/data/benchmarks/stats/data/ $train_encode_path $query 0
    #done
done
