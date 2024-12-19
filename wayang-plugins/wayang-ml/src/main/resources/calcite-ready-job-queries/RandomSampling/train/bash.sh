#!/bin/bash

source_dir="/home/mads/projects/incubator-wayang-thesis/wayang-plugins/wayang-ml/src/main/resources/calcite-ready-job-queries"
test_dir="/home/mads/projects/incubator-wayang-thesis/wayang-plugins/wayang-ml/src/main/resources/calcite-ready-job-queries/RandomSampling/test"
train_dir="/home/mads/projects/incubator-wayang-thesis/wayang-plugins/wayang-ml/src/main/resources/calcite-ready-job-queries/RandomSampling/train"

# Find all files in the source directory
all_files=$(find "$source_dir" -type f)

# Filter out files that are in the test directory
files_to_copy=$(echo "$all_files" | grep -v "^$test_dir")

# Copy the filtered files to the train directory
while IFS= read -r file; do
    relative_path="${file#$source_dir/}"
    destination="$train_dir/$relative_path"
    mkdir -p "$(dirname "$destination")"
    cp "$file" "$destination"
done <<< "$files_to_copy"

echo "Files copied successfully!"