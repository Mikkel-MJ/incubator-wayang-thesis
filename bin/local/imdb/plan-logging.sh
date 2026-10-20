#!/bin/bash

export WAYANG_DIR=/var/www/html/wayang-assembly/target/wayang-0.7.1

data_path=/opt/data/stats/
timings_path=/var/www/html/data/imdb/logs/
platforms=java,spark,flink,postgres
experience_path=/var/www/html/data/imdb/logs/
output_path=/var/www/html/data/imdb/logs/

classifier_path=/var/www/html/wayang-plugins/wayang-ml/src/main/python/python-ml/src/Models/imdb/classifier.onnx

test_path=/var/www/html/wayang-plugins/wayang-ml/src/main/resources/benchmarks/job/complex

echo "Clearing data from $timings_path"
rm -rf $timings_path/*

cd ${WAYANG_DIR}

echo "Running STATS Benchmark"

#for query in "${queries[@]}"; do
for query_file in "$test_path"/*.sql; do
    query=$(basename "$query_file" .sql)
    echo "Running $query"
    # Run wayang
    ./bin/wayang-submit org.apache.wayang.ml.benchmarks.JOBenchmark $platforms file://$data_path $output_path $query_file >> $output_path/wayang.$query.log
    # Run varibo
    ./bin/wayang-submit org.apache.wayang.ml.benchmarks.JOBenchmark $platforms file://$data_path $output_path $query_file bvae $classifier_path $output_path >> $output_path/varibo.$query.log
done
