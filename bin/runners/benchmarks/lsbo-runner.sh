export WORKDIR=/work/lsbo-paper
export SPARK_HOME=/opt/spark
export HADOOP_HOME=/opt/hadoop
export PATH="$PATH:${SPARK_HOME}/bin"
export SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*"
export FLINK_VERSION=1.20.0
export FLINK_HOME=/opt/flink
export PATH="$PATH:${FLINK_HOME}/bin"
export GIRAPH_VERSION=1.3.0
export GIRAPH_HOME=/opt/giraph
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

queries=(901 950 999)

bvae_path=/work/lsbo-paper/data/models/bvae.onnx

data_path=/work/lsbo-paper/data
experience_path=/work/lsbo-paper/data/experience/

for query in ${queries[@]}; do
    echo "Start LSBO loop for query ${query}"
    #./venv/bin/python3.11 ./src/init_lsbo.py --model bvae --time 10 --query $query --memory='-Xmx32g' --exec='/work/lsbo-paper/wayang-0.7.1/bin/wayang-submit' --args='java,spark,flink,giraph file:///work/lsbo-paper/data/' --parameters="./src/HyperparameterLogs/zdim/BVAE-16.json" --model-path="./src/Models/zdim/bvae-16.onnx" --zdim 16 --stats="./src/Data/splits/tpch/bvae/stats/zdim/stats-16"
    ./venv/bin/python3.11 ./src/init_lsbo.py --model bvae --time 10 --query $query --memory='-Xmx32g --illegal-access=permit' --exec='/work/lsbo-paper/wayang-0.7.1/bin/wayang-submit' --args='java,spark,flink,giraph file:///work/lsbo-paper/data/' --parameters="./src/HyperparameterLogs/zdim/BVAE-31.json" --model-path="./src/Models/zdim/bvae-31.onnx" --zdim 31 --stats="./src/Data/splits/tpch/bvae/zdim/stats-31.txt"
    ./venv/bin/python3.11 ./src/init_lsbo.py --model bvae --time 10 --query $query --memory='-Xmx32g --illegal-access=permit' --exec='/work/lsbo-paper/wayang-0.7.1/bin/wayang-submit' --args='java,spark,flink,giraph file:///work/lsbo-paper/data/' --parameters="./src/HyperparameterLogs/zdim/BVAE-64.json" --model-path="./src/Models/zdim/bvae-64.onnx" --zdim 64 --stats="./src/Data/splits/tpch/bvae/zdim/stats-64.txt"
    ./venv/bin/python3.11 ./src/init_lsbo.py --model bvae --time 10 --query $query --memory='-Xmx32g --illegal-access=permit' --exec='/work/lsbo-paper/wayang-0.7.1/bin/wayang-submit' --args='java,spark,flink,giraph file:///work/lsbo-paper/data/' --parameters="./src/HyperparameterLogs/zdim/BVAE-128.json" --model-path="./src/Models/zdim/bvae-128.onnx" --zdim 128 --stats="./src/Data/splits/tpch/bvae/zdim/stats-128.txt"
    ./venv/bin/python3.11 ./src/init_lsbo.py --model bvae --time 10 --query $query --memory='-Xmx32g --illegal-access=permit' --exec='/work/lsbo-paper/wayang-0.7.1/bin/wayang-submit' --args='java,spark,flink,giraph file:///work/lsbo-paper/data/' --parameters="./src/HyperparameterLogs/zdim/BVAE-256.json" --model-path="./src/Models/zdim/bvae-256.onnx" --zdim 256 --stats="./src/Data/splits/tpch/bvae/zdim/stats-256.txt"
done
