export WORKDIR=/work/lsbo-paper
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin
export HADOOP_HOME=/opt/hadoop
export FLINK_VERSION=1.20.0
export FLINK_HOME=/opt/flink
export PATH="$PATH:${FLINK_HOME}/bin"
export GIRAPH_VERSION=1.3.0
export GIRAPH_HOME=/opt/giraph
export PATH="$PATH:${GIRAPH_HOME}/bin"

cd ${WORKDIR}
cd wayang-0.7.1

for i in {201..300}; do
    ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.training.Training java,spark,flink,giraph file://${WORKDIR}/data/ ${WORKDIR}/data/naive-lsbo-2.txt $i true
done
