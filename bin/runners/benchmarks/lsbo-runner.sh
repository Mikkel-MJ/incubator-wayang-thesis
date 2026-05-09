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

carbvae_path=/work/lsbo-paper/python-ml/src/Models/tpch/carbvae.onnx

#data_path=/work/lsbo-paper/data
data_path=/work/lsbo-paper/data/benchmarks/tpch/data
experience_path=/work/lsbo-paper/data/experience/
test_path=/work/lsbo-paper/data/benchmarks/stats/queries

mapfile -t queries < <(ls "$test_path"/*.sql | xargs -n1 basename | sort)

excluded_queries=("q595820.sql")

filtered_queries=()
for query in "${queries[@]}"; do
    skip=0
    for excluded in "${excluded_queries[@]}"; do
        if [[ "$query" == "$excluded" ]]; then
            skip=1
            break
        fi
    done
    [[ $skip -eq 0 ]] && filtered_queries+=("$query")
done


# Seeded random selection of 15 queries from 142
#mapfile -t selected_queries < <(
#    awk 'BEGIN {
#        srand(42)
#        for (i = 1; i <= 142; i++) arr[i] = i
#        for (i = 142; i >= 2; i--) {
#            j = int(rand() * i) + 1
#            tmp = arr[i]; arr[i] = arr[j]; arr[j] = tmp
#        }
#        for (i = 1; i <= 30; i++) print arr[i]
#    }' | sort -n
#)

selected_queries=("q113925678.sql")
filtered_queries=("q113925678.sql")

echo "Selected queries: ${selected_queries[*]}"

skip=0  # Number of already-completed queries to skip

#for i in "${!selected_queries[@]}"; do
#    if (( i < skip )); then
#        echo "Skipping already-completed query ${selected_queries[$i]}.sql"
#        continue
#    fi
#    #num="${selected_queries[$i]}"
#    query_name="${filtered_queries[$i]}"
#    #query_name="${filtered_queries[$num]}"
#    query="$test_path/${filtered_queries[$i]}"
#    #query="$test_path/${filtered_queries[$num]}"
#    #query=${num}
#    #query_name=$(( num + 1 ))
#    echo "Start LSBO loop for query ${query}"
#    # random run
#    ./venv/bin/python3.11 ./src/init_lsbo.py --model carbvae --query $query --memory="-Xmx16g --illegal-access=permit" --exec="/work/lsbo-paper/wayang-0.7.1/bin/wayang-submit" --args="java,spark,flink,postgres file:///work/lsbo-paper/data/benchmarks/stats/data/" --model-path $carbvae_path --parameters="./src/HyperparameterLogs/stats/CarbVAE.json" --stats="./src/Data/splits/stats/rerun/random.${query_name}.txt" --trainset="./src/Data/splits/stats/rerun/retrain.txt" --zdim 32 --steps 1000 --experience="./src/Data/splits/stats/rerun/experience/random.${query_name}.txt" --acqf="random"
#    #./venv/bin/python3.11 ./src/init_lsbo.py --model carbvae --query $query --memory="-Xmx16g --illegal-access=permit" --exec="/work/lsbo-paper/wayang-0.7.1/bin/wayang-submit" --args="java,spark,flink,postgres file:///work/lsbo-paper/data/benchmarks/stats/data/" --model-path $carbvae_path --parameters="./src/HyperparameterLogs/stats/CarbVAE.json" --stats="./src/Data/splits/stats/rerun/stats.${query_name}.txt" --trainset="./src/Data/splits/stats/rerun/retrain.txt" --zdim 32 --steps 1000 --initialization="./src/Data/splits/stats/initialization/${query_name}.txt" --experience="./src/Data/splits/stats/rerun/experience/${query_name}.txt"
#done

queries=(12)

for query in $queries; do
    query_name=($query + 0)
    # random run
    ./venv/bin/python3.11 ./src/init_lsbo.py --model carbvae --query $query --memory="-Xmx16g --illegal-access=permit" --exec="/work/lsbo-paper/wayang-0.7.1/bin/wayang-submit" --args="java,spark,flink,postgres file:///work/lsbo-paper/data/benchmarks/tpch/data/" --model-path $carbvae_path --parameters="./src/HyperparameterLogs/tpch/CarbVAE.json" --stats="./src/Data/splits/tpch/random.${query_name}.txt" --trainset="./src/Data/splits/tpch/retrain.txt" --zdim 32 --steps 1000 --experience="./src/Data/splits/tpch/experience/random.${query_name}.txt" --acqf="random"
    #./venv/bin/python3.11 ./src/init_lsbo.py --model carbvae --query $query --memory="-Xmx16g --illegal-access=permit" --exec="/work/lsbo-paper/wayang-0.7.1/bin/wayang-submit" --args="java,spark,flink,postgres file:///work/lsbo-paper/data/benchmarks/tpch/data/" --model-path $carbvae_path --parameters="./src/HyperparameterLogs/tpch/CarbVAE.json" --stats="./src/Data/splits/tpch/stats.${query_name}.txt" --trainset="./src/Data/splits/tpch/retrain.txt" --zdim 32 --steps 1000 --initialization="./src/Data/splits/tpch/initialization/${query_name}.txt" --experience="./src/Data/splits/tpch/experience/${query_name}.txt"
done

