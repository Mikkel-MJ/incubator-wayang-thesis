#!/bin/bash

sudo apt update
echo "Installing Java"

sudo apt install openjdk-11-jdk --yes

java -version

SHELL_PROFILE="$HOME/.bashrc"
export WORKDIR=/work/lsbo-paper

echo "Printing env"
printenv

echo "Printing Hostname"
echo $HOSTNAME

export FLINK_VERSION=1.20.0
export FLINK_HOME=/opt/flink
export PATH="$PATH:${FLINK_HOME}/bin"

echo "Installing Flink"
cd /tmp

curl https://dlcdn.apache.org/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz --output flink-${FLINK_VERSION}-bin-scala_2.12.tgz
tar -zxf flink-${FLINK_VERSION}-bin-scala_2.12.tgz
rm flink-${FLINK_VERSION}-bin-scala_2.12.tgz
mv flink-${FLINK_VERSION} ${FLINK_HOME}

sudo chown -R ucloud /opt/flink

if [ $UCLOUD_RANK = 0 ]; then
    cd $WORKDIR
    echo "Setting up keys"
    ssh-keygen -t rsa -q -f /home/ucloud/.ssh/id_rsa
    sudo cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

    echo "Configuring master"
    cp /work/lsbo-paper/runners/flink/masters /opt/flink/conf

    echo "Configuring workers"
    cp /work/lsbo-paper/runners/flink/workers /opt/flink/conf

    sleep 30s

    echo "Starting master"
    sudo $FLINK_HOME/bin/start-cluster.sh
fi
