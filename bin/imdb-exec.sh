#!/bin/bash

# Move over to current build
cd /var/www/html/wayang-assembly/target/wayang-0.7.1/

# Directory containing the SQL files
DIRECTORY="/var/www/html/wayang-plugins/wayang-ml/src/main/resources/calcite-ready-job-queries"

# Loop over each file in the directory
for FILE in "$DIRECTORY"/*.sql
do
  # Execute the wayang-submit command with the current file as an argument
  ./bin/wayang-submit org.apache.wayang.ml.benchmarks.IMDBJOBenchmark "$FILE"
  echo $? >> imdb-exec-output.txt
  echo "$FILE" >> imdb-exec-output.txt
done
