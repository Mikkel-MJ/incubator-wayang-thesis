export WORKDIR=/work/lsbo-paper
export SPARK_HOME=/opt/spark
export HADOOP_HOME=/opt/hadoop
export PATH="$PATH:${SPARK_HOME}/bin"
export SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*"
export FLINK_VERSION=1.20.0
export FLINK_HOME=/opt/flink
export PATH="$PATH:${FLINK_HOME}/bin"
export GIRAPH_VERSION=1.3.0
export GIRAPH_HOME=/opt/giraph
export PATH="$PATH:${GIRAPH_HOME}/bin"

sudo apt update
sudo apt-get install -y postgresql

cd ${WORKDIR}
cd wayang-0.7.1

# Directory containing the SQL files
schema_path=/work/lsbo-paper/data/JOBenchmark/schema
queries_path=/work/lsbo-paper/data/JOBenchmark/queries
output_dir=
output_dir=/work/lsbo-paper/data/JOBenchmark
output_path="${output_dir}/imdb-output.txt"

cd ${output_dir} && touch imdb-output.txt
cd ${WORKDIR}
cd wayang-0.7.1

schema_path=/work/lsbo-paper/data/JOBenchmark/schema

export PGPASSWORD=ucloud

#echo "Setting up postgres schema"

#psql -h postgres -U ucloud -d job -a -f $schema_path/1_schema.sql

#echo "Adding FK indexes"

#psql -h postgres -U ucloud -d job -a -f $schema_path/2_fkindexes.sql

#echo "Copying data"

#psql -h postgres -U ucloud -d job -a -f $schema_path/3_copy_data.sql

#echo "Add FKs"

#psql -h postgres -U ucloud -d job -a -f $schema_path/4_add_fks.sql

# Loop over each file in the directory
for FILE in "$queries_path"/*.sql
do
  echo $FILE
  # Measure the time taken for the wayang-submit command

  SECONDS=0

  # Execute the wayang-submit command with the current file as an argument
  output="$(./bin/wayang-submit -Xmx32g org.apache.wayang.ml.benchmarks.IMDBJOBenchmark "$FILE" 2>&1)"
  # Output the time taken, exit status, and file name
  echo "Time taken: ${SECONDS}s" >> "$output_path"
  echo "Exitcode: {$?}" >> "$output_path"
  echo "Filepath: {$FILE}" >> "$output_path"
  echo -e "Last 10 output lines: ${output}\n" >> "$output_path"
done
