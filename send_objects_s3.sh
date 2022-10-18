#!/bin/bash

disk_dir="/Volumes/Press Release Lexis Nexis/"
extracted_dir="${disk_dir}/extracted_headlines"
last_processed="/Volumes/Press Release Lexis Nexis//extracted_headlines/94b18cbd-5716-4ae7-b600-9c4edd967376.gz"
flag=0

for entry in "$extracted_dir"/*
do 
    if [ $flag -eq 1 ]; then 
        aws s3 cp "${entry}" s3://pressreleasedata
        echo "Copied over $entry"
    fi

    if [ "${entry}" = "${last_processed}" ]; then 
        flag=1
    fi
done
