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

export FLINK_VERSION=1.20.1
export FLINK_HOME=/opt/flink
export PATH="$PATH:${FLINK_HOME}/bin"
export FLINK_PROPERTIES="rest.port: 8081
rest.address: 0.0.0.0
rest.bind-address: 0.0.0.0
jobmanager.bind-address: 0.0.0.0
"

echo "Installing Flink"
cd /tmp

curl https://dlcdn.apache.org/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz --output flink-${FLINK_VERSION}-bin-scala_2.12.tgz
tar -zxf flink-${FLINK_VERSION}-bin-scala_2.12.tgz
rm flink-${FLINK_VERSION}-bin-scala_2.12.tgz
mv flink-${FLINK_VERSION} ${FLINK_HOME}

sudo chown -R ucloud /opt/flink

# Copy configs for tasksmanagers
if [ $UCLOUD_RANK = 1 ]; then
    echo "Copying task config"
    cp -r /work/lsbo-paper/runners/flink/node1.yaml /opt/flink/conf/config.yaml
fi

if [ $UCLOUD_RANK = 2 ]; then
    echo "Copying task config"
    cp -r /work/lsbo-paper/runners/flink/node2.yaml /opt/flink/conf/config.yaml

fi

if [ $UCLOUD_RANK = 0 ]; then
    cd $WORKDIR
    echo "Setting up keys"
    ssh-keygen -t rsa -q -f /home/ucloud/.ssh/id_rsa
    sudo cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

    echo "Copying jobmanager config"
    cp -r /work/lsbo-paper/runners/flink/node0.yaml /opt/flink/conf/config.yaml

    echo "Configuring master"
    cp /work/lsbo-paper/runners/flink/masters /opt/flink/conf

    echo "Configuring workers"
    cp /work/lsbo-paper/runners/flink/workers /opt/flink/conf

    sleep 180s


    echo "Starting cluster"
    sudo $FLINK_HOME/bin/start-cluster.sh
    #sudo $FLINK_HOME/bin/jobmanager.sh start node0
    #sudo ssh root@node1 $FLINK_HOME/bin/taskmanager.sh start node1
    #sudo ssh root@node2 $FLINK_HOME/bin/taskmanager.sh start node2
fi
