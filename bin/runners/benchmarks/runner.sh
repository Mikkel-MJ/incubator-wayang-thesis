#!/bin/bash
sudo apt update
echo "Installing Java"

#sudo apt install default-jre --yes
sudo apt install -y openjdk-11-jdk

java -version

# Install Scala kernel
ENV SCALA_VERSION=2.12.4 \
    SCALA_HOME=/usr/share/scala

echo "Installing Scala"

# NOTE: bash is used by scala/scalac scripts, and it cannot be easily replaced with ash.
apt-get install -y wget ca-certificates && \
    apt-get install -y bash curl jq && \
    cd "/tmp" && \
    wget --no-verbose "https://downloads.typesafe.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.tgz" && \
    tar xzf "scala-${SCALA_VERSION}.tgz" && \
    mkdir "${SCALA_HOME}" && \
    rm "/tmp/scala-${SCALA_VERSION}/bin/"*.bat && \
    mv "/tmp/scala-${SCALA_VERSION}/bin" "/tmp/scala-${SCALA_VERSION}/lib" "${SCALA_HOME}" && \
    ln -s "${SCALA_HOME}/bin/"* "/usr/bin/" && \
    rm -rf "/tmp/"*

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

cd ../work/thesis/benchmarks
#cp spark-defaults.conf /opt/spark/conf
tar -xvf apache-wayang-assembly-0.7.1-SNAPSHOT-incubating-dist.tar.gz

cd wayang-0.7.1-SNAPSHOT

export OMP_NUM_THREADS=16 # or adjust to however many threads

queries=(1 3 6 10 12 14 19)

vae_path=/work/thesis/data/models/vae-full.onnx
cost_path=/work/thesis/data/models/cost.onnx
pairwise_path=/work/thesis/data/models/pairwise.onnx

for query in ${queries[@]}; do
    for i in {0..10}; do
        ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.TPCHBenchmarks java,spark,flink,giraph /work/thesis/data/ /work/thesis/data/benchmarks/query-$query-timings-default.txt $query
    done

    for i in {0..10}; do
        ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.TPCHBenchmarks java,spark,flink,giraph /work/thesis/data/ /work/thesis/data/benchmarks/query-$query-timings-cost.txt $query cost $cost_path
    done

    for i in {0..10}; do
        ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.TPCHBenchmarks java,spark,flink,giraph /work/thesis/data/ /work/thesis/data/benchmarks/query-$query-timings-pairwise.txt $query pairwise $pairwise_path
    done

    for i in {0..10}; do
        ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.TPCHBenchmarks java,spark,flink,giraph /work/thesis/data/ /work/thesis/data/benchmarks/query-$query-timings-vae.txt $query vae $vae_path
    done
done

