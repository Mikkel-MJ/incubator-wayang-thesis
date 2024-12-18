#!/bin/bash

schema_path=/work/lsbo-paper/data/JOBenchmark/schema

echo "Setting up postgres schema"

psql -h postgres -U ucloud -d job -a -f $schema_path/1_schema.sql

echo "Adding FK indexes"

psql -h postgres -U ucloud -d job -a -f $schema_path/2_fkindexes.sql

echo "Copying data"

psql -h postgres -U ucloud -d job -a -f $schema_path/3_copy_data.sql

echo "Add FKs"

psql -postgres -U ucloud -d job -a -f $schema_path/4_add_fks.sql
