#!/bin/bash

sudo apt update
echo "Installing Java"

sudo apt install openjdk-11-jdk --yes

java -version

echo "Installing Python 3.11"
sudo apt update
sudo apt upgrade -y
sudo apt-get install -y software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt-get install -y python3.11-full python3.11-dev python3-pip
python3.11 --version

SHELL_PROFILE="$HOME/.bashrc"
export WORKDIR=/work/lsbo-paper

echo "Installing Hadoop"

export HADOOP_VERSION=3.3.0
export HADOOP_HOME=/opt/hadoop

wget https://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
tar -zxf hadoop-${HADOOP_VERSION}.tar.gz
mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME}
rm hadoop-${HADOOP_VERSION}.tar.gz

echo "Installing Spark"
export SPARK_VERSION=3.5.3
export SPARK_HOME=/opt/spark

wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz
tar -zxf spark-${SPARK_VERSION}-bin-hadoop3.tgz
rm spark-${SPARK_VERSION}-bin-hadoop3.tgz
mv spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}

export PATH=$PATH:$SPARK_HOME/bin

echo "Installing Flink"

# "Installing Flink"
export FLINK_VERSION=1.20.0
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

cd ${WORKDIR}
#cp spark-defaults.conf /opt/spark/conf
tar -xvf apache-wayang-assembly-0.7.1-incubating-dist.tar.gz
