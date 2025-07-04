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

if [ ! -d python-ml ]; then
    echo "Unpacking python-ml"
    tar -zxf python-ml.tar
fi

cd python-ml

if [ ! -d venv ]; then
    echo "Setting up pyenv"
    python3.11 -m venv ./venv
    echo "Installing python requirements"
    ./venv/bin/python3.11 -m pip install -r requirements.txt
fi

#queries=(901 950 999)

bvae_1_path=/work/lsbo-paper/python-ml/src/Models/imdb/bvae-1.onnx
bvae_32_48_path=/work/lsbo-paper/python-ml/src/Models/imdb/bvae-32-48.onnx
bvae_5_path=/work/lsbo-paper/python-ml/src/Models/imdb/bvae-5.onnx
bvae_10_path=/work/lsbo-paper/python-ml/src/Models/imdb/bvae-10.onnx

#data_path=/work/lsbo-paper/data
data_path=/work/lsbo-paper/data/JOBenchmark/data
experience_path=/work/lsbo-paper/data/experience/
train_path=/work/lsbo-paper/data/JOBenchmark/queries/Training
test_path=/work/lsbo-paper/data/JOBenchmark/queries

#for query in {949..999}; do
for query in "$test_path"/*.sql; do
#for query in $(ls -1 "$train_path"/*.sql | tail -n 50); do
    echo "Start LSBO loop for query ${query}"
    ./venv/bin/python3.11 ./src/init_lsbo.py --model bvae --time 10 --query $query --memory="-Xmx32g --illegal-access=permit" --exec="/work/lsbo-paper/wayang-0.7.1/bin/wayang-submit" --args="java,spark,flink,postgres file:///work/lsbo-paper/data/JOBenchmark/data/" --model-path $bvae_32_48_path --stats="./src/Data/splits/imdb/training/stats-32-48.txt" --trainset="./src/Data/splits/imdb/training/retrain-32-48.txt" --zdim 35
    #./venv/bin/python3.11 ./src/init_lsbo.py --model bvae --time 10 --query $query --memory="-Xmx32g --illegal-access=permit" --exec="/work/lsbo-paper/wayang-0.7.1/bin/wayang-submit" --args="java,spark,flink,postgres file:///work/lsbo-paper/data/JOBenchmark/data/" --model-path $bvae_5_path --stats="./src/Data/splits/imdb/training/stats-5.txt" --trainset="./src/Data/splits/imdb/training/retrain-5.txt" --zdim 28
    #./venv/bin/python3.11 ./src/init_lsbo.py --model bvae --time 10 --query $query --memory="-Xmx32g --illegal-access=permit" --exec="/work/lsbo-paper/wayang-0.7.1/bin/wayang-submit" --args="java,spark,flink,postgres file:///work/lsbo-paper/data/JOBenchmark/data/" --model-path $bvae_10_path --stats="./src/Data/splits/imdb/training/stats-10.txt" --trainset="./src/Data/splits/imdb/training/retrain-10.txt" --zdim 23

    # Lord forgive me - for Flink has sinned
    sudo ssh -o StrictHostKeyChecking=no root@flink-cluster sudo /opt/flink/bin/stop-cluster.sh
    sudo ssh -o StrictHostKeyChecking=no root@flink-cluster sudo /opt/flink/bin/start-cluster.sh
done
