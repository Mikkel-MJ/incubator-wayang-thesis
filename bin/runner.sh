#!/bin/bash

cd /var/www/html/wayang-assembly/target/wayang-0.7.1-SNAPSHOT/

queries=(1 3 6 10 12 14 19)

vae_path=/var/www/html/wayang-plugins/wayang-ml/src/main/resources/benchmark_models/vae.onnx
cost_path=/var/www/html/wayang-plugins/wayang-ml/src/main/resources/benchmark_models/cost-0_1.onnx
pairwise_path=/var/www/html/wayang-plugins/wayang-ml/src/main/resources/benchmark_models/pairwise.onnx
bvae_path=/var/www/html/wayang-plugins/wayang-ml/src/main/resources/benchmark_models/bvae.onnx

data_path=/var/www/html/data/
experience_path=/var/www/html/data/experience/

for query in ${queries[@]}; do
    for i in {0..10}; do
        ./bin/wayang-submit -Xmx8g org.apache.wayang.ml.benchmarks.TPCHBenchmarks java,spark,flink,giraph $data_path $data_path/benchmarks/ $query
    done

    for i in {0..10}; do
        ./bin/wayang-submit -Xmx8g org.apache.wayang.ml.benchmarks.TPCHBenchmarks java,spark,flink,giraph $data_path $data_path/benchmarks/ $query cost $cost_path $experience_path
    done

    for i in {0..10}; do
        ./bin/wayang-submit -Xmx8g org.apache.wayang.ml.benchmarks.TPCHBenchmarks java,spark,flink,giraph $data_path $data_path/benchmarks/ $query pairwise $pairwise_path $experience_path
    done

    for i in {0..10}; do
        ./bin/wayang-submit -Xmx8g org.apache.wayang.ml.benchmarks.TPCHBenchmarks java,spark,flink,giraph $data_path $data_path/benchmarks/ $query vae $vae_path $experience_path
    done

    for i in {0..10}; do
        ./bin/wayang-submit -Xmx8g org.apache.wayang.ml.benchmarks.TPCHBenchmarks java,spark,flink,giraph $data_path $data_path/benchmarks/ $query bvae $bvae_path $experience_path
    done
done

