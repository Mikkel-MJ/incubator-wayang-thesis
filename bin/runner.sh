#!/bin/bash

cd /var/www/html/wayang-assembly/target/wayang-0.7.1-SNAPSHOT/

queries=(1 3 6 10 12 14 19)

vae_path=/var/www/html/wayang-plugins/wayang-ml/src/main/resources/vae-full.onnx
cost_path=/var/www/html/wayang-plugins/wayang-ml/src/main/resources/cost.onnx
pairwise_path=/var/www/html/wayang-plugins/wayang-ml/src/main/resources/pairwise.onnx

for query in ${queries[@]}; do
    for i in {0..1}; do
        ./bin/wayang-submit -Xmx8g org.apache.wayang.ml.benchmarks.TPCHBenchmarks java,spark,flink,giraph /var/www/html/data/ /var/www/html/data/benchmarks/query-$query-timings-default.txt $query
    done

    for i in {0..1}; do
        ./bin/wayang-submit -Xmx8g org.apache.wayang.ml.benchmarks.TPCHBenchmarks java,spark,flink,giraph /var/www/html/data/ /var/www/html/data/benchmarks/query-$query-timings-cost.txt $query cost $cost_path
    done

    for i in {0..1}; do
        ./bin/wayang-submit -Xmx8g org.apache.wayang.ml.benchmarks.TPCHBenchmarks java,spark,flink,giraph /var/www/html/data/ /var/www/html/data/benchmarks/query-$query-timings-pairwise.txt $query pairwise $pairwise_path
    done

    for i in {0..1}; do
        ./bin/wayang-submit -Xmx8g org.apache.wayang.ml.benchmarks.TPCHBenchmarks java,spark,flink,giraph /var/www/html/data/ /var/www/html/data/benchmarks/query-$query-timings-vae.txt $query vae $vae_path
    done
done


