#!/bin/bash


echo "Setting up Spark Environment"

export SPARK_MASTER_URL=spark://spark-cluster:7077
export SPARK_WORKER_MEMORY=16G
export SPARK_RPC_AUTHENTICATION_ENABLED=no
export SPARK_RPC_ENCRYPTION_ENABLED=no
export SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
export SPARK_SSL_ENABLED=no
export SPARK_USER=spark
export SPARK_OPTS="--driver-java-options=-Xms16384M --driver-java-options=-Xmx16384M --driver-java-options=-Dlog4j.logLevel=info"
export HOSTNAME=node1

printenv
