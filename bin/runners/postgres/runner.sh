#!/bin/bash

schema_path=/work/lsbo-paper/data/JOBenchmark/schema

echo "Setting up postgres schema"

psql -U ucloud -d job -a -f $schema_path/1_schema.sql

echo "Adding FK indexes"

psql -U ucloud -d job -a -f $schema_path/2_fkindexes.sql

echo "Copying data"

psql -U ucloud -d job -a -f $schema_path/3_copy_data.sql

echo "Add FKs"

psql -U ucloud -d job -a -f $schema_path/4_add_fks.sql
