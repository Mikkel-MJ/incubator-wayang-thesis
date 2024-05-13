#!/bin/bash

sudo apt update
echo "Installing Java"

sudo apt install default-jre --yes

java -version

echo "Installing Spark"
SHELL_PROFILE="$HOME/.bashrc"

wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz

tar -xzf spark-3.5.1-bin-hadoop3.tgz
sudo mv spark-3.5.1-bin-hadoop3 /opt/spark
rm spark-3.5.1-bin-hadoop3.tgz

export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin

echo "Installing Hadoop"

wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz

tar -xzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 /opt/hadoop
rm hadoop-3.3.6.tar.gz

export HADOOP_HOME=/opt/hadoop

echo "Installing Flink"

# "Installing Flink"
export FLINK_VERSION=1.17.2
export FLINK_HOME=/opt/flink
export PATH="$PATH:${FLINK_HOME}/bin"

curl https://dlcdn.apache.org/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz --output flink-${FLINK_VERSION}-bin-scala_2.12.tgz &&
        tar -zxf flink-${FLINK_VERSION}-bin-scala_2.12.tgz &&
        rm flink-${FLINK_VERSION}-bin-scala_2.12.tgz &&
        mv flink-${FLINK_VERSION} ${FLINK_HOME}

echo "Installing Giraph"

export GIRAPH_VERSION=1.3.0
export GIRAPH_HOME=/opt/giraph
export PATH="$PATH:${GIRAPH_HOME}/bin"

curl https://archive.apache.org/dist/giraph/giraph-${GIRAPH_VERSION}/giraph-dist-${GIRAPH_VERSION}-hadoop1-bin.tar.gz --output giraph-dist-${GIRAPH_VERSION}-hadoop1-bin.tar.gz &&
        tar -zxf giraph-dist-${GIRAPH_VERSION}-hadoop1-bin.tar.gz &&
        rm giraph-dist-${GIRAPH_VERSION}-hadoop1-bin.tar.gz &&
        mv giraph-${GIRAPH_VERSION}-hadoop1-for-hadoop-1.2.1 ${GIRAPH_HOME}

cd ../work/thesis
#cp spark-defaults.conf /opt/spark/conf
tar -xvf apache-wayang-assembly-0.7.1-SNAPSHOT-incubating-dist.tar.gz

cd wayang-0.7.1-SNAPSHOT

for i in {2001..3000}; do
    ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.training.Training java,spark,flink,giraph file:///work/thesis/data/ /work/thesis/data/no-co-encodings-2.txt $i true
done
