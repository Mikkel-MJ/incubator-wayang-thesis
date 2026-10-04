#!/bin/bash

export WAYANG_DIR=/var/www/html/wayang-assembly/target/wayang-0.7.1

data_path=/opt/data/tpch/
timings_path=/var/www/html/data/tpch/logs/
platforms=java,spark,flink
experience_path=/var/www/html/data/tpch/logs/

classifier_path=/var/www/html/wayang-plugins/wayang-ml/src/main/python/python-ml/src/Models/tpch/classifier.onnx
retrained_classifier_path=/var/www/html/wayang-plugins/wayang-ml/src/main/python/python-ml/src/Models/tpch/retrain.classifier.onnx

echo "Clearing data from $timings_path"
rm -rf $timings_path/*

cd ${WAYANG_DIR}

echo "Running JOBenchmark"

for query in {0..29}; do
    ./bin/wayang-submit org.apache.wayang.ml.benchmarks.GeneratableBenchmarks $platforms file://$data_path $timings_path $query vae $classifier_path $experience_path
done
