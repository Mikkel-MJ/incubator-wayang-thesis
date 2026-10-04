#!/bin/bash

export WAYANG_DIR=/var/www/html/wayang-assembly/target/wayang-0.7.1

data_path=/opt/data/imdb/
timings_path=/var/www/html/data/imdb/logs/retrained/
platforms=java,spark,flink,postgres
experience_path=/var/www/html/data/imdb/logs/retrained/

classifier_path=/var/www/html/wayang-plugins/wayang-ml/src/main/python/python-ml/src/Models/imdb/ucloud/classifier.onnx
retrained_classifier_path=/var/www/html/wayang-plugins/wayang-ml/src/main/python/python-ml/src/Models/imdb/ucloud/retrain.classifier.onnx

test_path=/var/www/html/wayang-plugins/wayang-ml/src/main/resources/benchmarks/job/complex

echo "Clearing data from $timings_path"
rm -rf $timings_path/*

cd ${WAYANG_DIR}

echo "Running JOBenchmark"

for query in "$test_path"/*.sql; do
    ./bin/wayang-submit org.apache.wayang.ml.benchmarks.JOBenchmark $platforms file://$data_path $timings_path $query vae $retrained_classifier_path $experience_path
done
