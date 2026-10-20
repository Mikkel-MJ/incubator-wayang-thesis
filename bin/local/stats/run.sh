#!/bin/bash

export WAYANG_DIR=/var/www/html/wayang-assembly/target/wayang-0.7.1

data_path=/opt/data/stats/
timings_path=/var/www/html/data/stats/logs/retrained/
platforms=java,spark,flink,postgres
experience_path=/var/www/html/data/stats/logs/retrained/

classifier_path=/var/www/html/wayang-plugins/wayang-ml/src/main/python/python-ml/src/Models/stats/classifier.onnx
retrained_classifier_path=/var/www/html/wayang-plugins/wayang-ml/src/main/python/python-ml/src/Models/stats/retrain.classifier.onnx

test_path=/var/www/html/wayang-plugins/wayang-ml/src/main/resources/benchmarks/stats

echo "Clearing data from $timings_path"
rm -rf $timings_path/*

cd ${WAYANG_DIR}

echo "Running STATS Benchmark"

for query in "$test_path"/*.sql; do
    ./bin/wayang-submit org.apache.wayang.ml.benchmarks.STATSBenchmark $platforms file://$data_path $timings_path $query vae $retrained_classifier_path $experience_path
done
