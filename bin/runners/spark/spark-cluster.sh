#!/bin/bash

sudo apt update
echo "Installing Java"

sudo apt install openjdk-11-jdk --yes

java -version

SHELL_PROFILE="$HOME/.bashrc"
export WORKDIR=/work/lsbo-paper

echo "Installing Spark"
export SPARK_VERSION=3.5.5
export SPARK_HOME=/opt/spark

echo "Printing env"
printenv

echo "Printing Hostname"
echo $HOSTNAME

export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

echo "Downloading Spark"
cd /tmp
wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz
tar -zxf spark-${SPARK_VERSION}-bin-hadoop3.tgz
rm spark-${SPARK_VERSION}-bin-hadoop3.tgz
mv spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}

sudo chown -R ucloud /opt/spark

if [ $UCLOUD_RANK = 0 ]; then
    cd $WORKDIR
    echo "Setting up keys"
    ssh-keygen -t rsa -q -f /home/ucloud/.ssh/id_rsa
    sudo cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

    echo "Configuring workers"
    cp /work/lsbo-paper/runners/spark/workers /opt/spark/conf

    sleep 100s

    echo "Starting master"
    sudo $SPARK_HOME/sbin/start-all.sh
fi
