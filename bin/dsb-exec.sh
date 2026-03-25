#!/bin/bash
> dsb-exec-output.txt

touch dsb-exec-output.txt

# Store Path
output_path="$(pwd)/dsb-exec-output.txt"

# Move over to current build
cd /var/www/html/wayang-assembly/target/wayang-0.7.1/

# Directory containing the SQL files
DIRECTORY="/var/www/html/wayang-plugins/wayang-ml/src/main/resources/benchmarks/dsb/queries"

# Loop over each file in the directory
for FILE in "$DIRECTORY"/*.sql
do
  # Measure the time taken for the wayang-submit command

  SECONDS=0
  # Execute the wayang-submit command with the current file as an argument
  output="$(./bin/wayang-submit -Xmx8g org.apache.wayang.ml.benchmarks.DSBenchmark java,postgres file:///var/www/html/data/job/ /var/www/html/data/ "$FILE" 2>&1 | tail -n 40)"

  # Output the time taken, exit status, and file name
  echo "Time taken: ${SECONDS}s" >> "$output_path"
  echo "Exitcode: {$?}" >> "$output_path"
  echo "Filepath: {$FILE}" >> "$output_path"
  echo -e "Last 10 output lines: ${output}\n" >> "$output_path"
done
