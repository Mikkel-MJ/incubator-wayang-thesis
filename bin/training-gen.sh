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

echo "DataFarming Training data"
for i in {0..700}; do
    ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.training.Training java,spark,flink file://${WORKDIR}/data/ ${WORKDIR}/data/generated/train.txt $i true
done

echo "DataFarming Validation data"
for i in {701..800}; do
    ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.training.Training java,spark,flink file://${WORKDIR}/data/ ${WORKDIR}/data/generated/validate.txt $i true
done

echo "DataFarming Test data"
for i in {801..900}; do
    ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.training.Training java,spark,flink file://${WORKDIR}/data/ ${WORKDIR}/data/generated/test.txt $i true
done

echo "DataFarming Retraining data"
for i in {901..1000}; do
    ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.training.Training java,spark,flink file://${WORKDIR}/data/ ${WORKDIR}/data/generated/retrain.txt $i true
done
