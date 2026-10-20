#!/bin/bash

export WAYANG_DIR=/var/www/html/wayang-assembly/target/wayang-0.7.1

data_path=/opt/data/tpch/
timings_path=/var/www/html/data/tpch/logs/
platforms=java,spark,flink,postgres
experience_path=/var/www/html/data/tpch/logs/
output_path=/var/www/html/data/tpch/logs/

classifier_path=/var/www/html/wayang-plugins/wayang-ml/src/main/python/python-ml/src/Models/tpch/classifier.onnx

echo "Clearing data from $timings_path"
rm -rf $timings_path/*

cd ${WAYANG_DIR}

echo "Running TPC-H Benchmark"

for query in $(seq 0 29); do
    echo "Running query $query"
    # Run wayang
    ./bin/wayang-submit org.apache.wayang.ml.benchmarks.GeneratableBenchmarks $platforms file://$data_path $output_path $query >> $output_path/wayang.$query.log
    # Run varibo
    ./bin/wayang-submit org.apache.wayang.ml.benchmarks.GeneratableBenchmarks $platforms file://$data_path $output_path $query bvae $classifier_path $output_path >> $output_path/varibo.$query.log
done
