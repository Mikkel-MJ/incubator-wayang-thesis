#!/bin/bash

schema_path=/work/lsbo-paper/data/benchmarks/stats/schema

echo "Setting up postgres schema"

psql -U ucloud -d stats -a -f $schema_path/1_schema.sql

echo "Adding FK indexes"

psql -U ucloud -d stats -a -f $schema_path/2_stats_index.sql

echo "Copying data"

psql -U ucloud -d stats -a -f $schema_path/3_stats_load.sql
