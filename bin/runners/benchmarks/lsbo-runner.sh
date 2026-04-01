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

bvae_1_path=/work/lsbo-paper/python-ml/src/Models/tpch/carbvae.onnx
bvae_5_path=/work/lsbo-paper/python-ml/src/Models/imdb/bvae-5.onnx
bvae_10_path=/work/lsbo-paper/python-ml/src/Models/imdb/bvae-10.onnx

#data_path=/work/lsbo-paper/data
data_path=/work/lsbo-paper/data/JOBenchmark/data
experience_path=/work/lsbo-paper/data/experience/
train_path=/work/lsbo-paper/data/JOBenchmark/queries/complex
test_path=/work/lsbo-paper/data/JOBenchmark/queries/complex

# Seeded random selection of 15 queries from 30
#mapfile -t selected_queries < <(
#    awk 'BEGIN {
#        srand(42)
#        for (i = 1; i <= 30; i++) arr[i] = i
#        for (i = 30; i >= 2; i--) {
#            j = int(rand() * i) + 1
#            tmp = arr[i]; arr[i] = arr[j]; arr[j] = tmp
#        }
#        for (i = 1; i <= 15; i++) print arr[i]
#    }' | sort -n
#)

echo "Selected queries: ${selected_queries[*]}"

selected_queries=( {0..29} )

skip=0  # Number of already-completed queries to skip

for i in "${!selected_queries[@]}"; do
    if (( i < skip )); then
        echo "Skipping already-completed query ${selected_queries[$i]}.sql"
        continue
    fi

    num="${selected_queries[$i]}"
    #query="$test_path/${num}.sql"
    #query_name="${num}.sql"
    query=${num}
    query_name=$(( num + 1 ))
    echo "Start LSBO loop for query ${query}"
    ./venv/bin/python3.11 ./src/init_lsbo.py --model carbvae --query $query --memory="-Xmx32g --illegal-access=permit" --exec="/work/lsbo-paper/wayang-0.7.1/bin/wayang-submit" --args="java,spark,flink,postgres file:///work/lsbo-paper/data/benchmarks/tpch/data/" --model-path $bvae_1_path --parameters="./src/HyperparameterLogs/tpch/CarbVAE.json" --stats="./src/Data/splits/tpch/stats.${query_name}.txt" --trainset="./src/Data/splits/tpch/retrain.txt" --zdim 32 --steps 1000 --initialization="./src/Data/splits/tpch/initialization/${query_name}.txt" --experience="./src/Data/splits/tpch/experience/${query_name}.txt"
done


#for query in {949..999}; do
#for query in "$test_path"/*.sql; do
#for query in $(ls -1 "$train_path"/*.sql | tail -n 50); do
#for ((query=900; query<=999; query+=4)); do
#for query in ${queries[@]}; do
#    query_name=$(basename "$query")
#    echo "Start LSBO loop for query ${query}"
#    ./venv/bin/python3.11 ./src/init_lsbo.py --model carbvae --query $query --memory="-Xmx32g --illegal-access=permit" --exec="/work/lsbo-paper/wayang-0.7.1/bin/wayang-submit" --args="java,spark,flink,postgres file:///work/lsbo-paper/data/JOBenchmark/data/" --model-path $bvae_1_path --parameters="./src/HyperparameterLogs/imdb/CarbVAE.json" --stats="./src/Data/splits/imdb/complex/stats.${query_name}.txt" --trainset="./src/Data/splits/imdb/complex/retrain.txt" --zdim 32 --steps 1000 --initialization="./src/Data/splits/imdb/complex/initialization/${query_name}.txt" --experience="./src/Data/splits/imdb/complex/experience/${query_name}.txt"

    # Lord forgive me - for Flink has sinned
#    sudo ssh -o StrictHostKeyChecking=no root@flink-cluster sudo /opt/flink/bin/stop-cluster.sh
#    sudo ssh -o StrictHostKeyChecking=no root@flink-cluster sudo /opt/flink/bin/start-cluster.sh

#done


