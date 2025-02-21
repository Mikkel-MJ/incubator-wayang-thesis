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
cd wayang-0.7.1

bvae_1_path=/work/lsbo-paper/python-ml/src/Models/tpch/bvae-b-1.onnx
bvae_5_path=/work/lsbo-paper/python-ml/src/Models/tpch/bvae-b-5.onnx
bvae_10_path=/work/lsbo-paper/python-ml/src/Models/tpch/bvae-b-10.onnx

retrained_bvae_1_path=/work/lsbo-paper/python-ml/src/Models/tpch/retrained/bvae-b-1.onnx
retrained_bvae_5_path=/work/lsbo-paper/python-ml/src/Models/tpch/retrained/bvae-b-5.onnx
retrained_bvae_10_path=/work/lsbo-paper/python-ml/src/Models/tpch/retrained/bvae-b-10.onnx

cost_path=/work/lsbo-paper/data/models/cost.onnx

data_path=/work/lsbo-paper/data
experience_path=/work/lsbo-paper/data/experience/

for query in {801..900}; do
    for i in {0..2}; do
        echo "Benchmarking Test data with native optimizer"
        ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.GeneratableBenchmarks java,spark,flink,giraph file://$data_path/ $data_path/benchmarks/generatables/ $query

        echo "Benchmarking Test data with bvae-b-1"
        ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.GeneratableBenchmarks java,spark,flink,giraph file://$data_path/ $data_path/benchmarks/generatables/bvae/b1/ $query bvae $bvae_1_path $experience_path

        echo "Benchmarking Test data with bvae-b-5"
        ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.GeneratableBenchmarks java,spark,flink,giraph file://$data_path/ $data_path/benchmarks/generatables/bvae/b5/ $query bvae $bvae_5_path $experience_path

        echo "Benchmarking Test data with bvae-b-10"
        ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.GeneratableBenchmarks java,spark,flink,giraph file://$data_path/ $data_path/benchmarks/generatables/bvae/b10/ $query bvae $bvae_10_path $experience_path
        #echo "Benchmarking Test data with Cost model"
        #./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.GeneratableBenchmarks java,spark,flink,giraph file://$data_path/ $data_path/benchmarks/generatables/ $query cost $cost_path $experience_path

        echo "Benchmarking Test data with retrained bvaes"
        ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.GeneratableBenchmarks java,spark,flink,giraph file://$data_path/ $data_path/benchmarks/generatables/bvae/retrained/b1/ $query bvae $retrained_bvae_1_path $experience_path
        ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.GeneratableBenchmarks java,spark,flink,giraph file://$data_path/ $data_path/benchmarks/generatables/bvae/retrained/b5/ $query bvae $retrained_bvae_5_path $experience_path
        ./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.GeneratableBenchmarks java,spark,flink,giraph file://$data_path/ $data_path/benchmarks/generatables/bvae/retrained/b10/ $query bvae $retrained_bvae_10_path $experience_path

        # Lord forgive me - for Flink has sinned
        sudo ssh -o StrictHostKeyChecking=no root@flink-cluster sudo /opt/flink/bin/stop-cluster.sh
        sudo ssh -o StrictHostKeyChecking=no root@flink-cluster sudo /opt/flink/bin/start-cluster.sh
        sleep 5s
    done
done
