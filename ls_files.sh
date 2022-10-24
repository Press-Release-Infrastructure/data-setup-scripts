#!/bin/bash

disk_dir="/Volumes/Press Release Lexis Nexis/"
extracted_dir="${disk_dir}/extracted_headlines"
ls_output="./ls_output.txt"

for entry in "${extracted_dir}"/*
do
    base_name=$(basename "$entry")
    echo $base_name
    echo "${base_name}" >> $ls_output
done;

echo "Done!"