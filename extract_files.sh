#!/bin/bash

disk_dir="/Volumes/Press Release Lexis Nexis/"
extracted_dir="${disk_dir}/extracted_headlines"
mkdir $extracted_dir
i=0

find "${disk_dir}" -regex ".*.zip" | while IFS= read file 
do {
    flag=0
    unzip -Z1 "$file" | sed 1d | while read result ; do {
        first_file=$([[ $result =~ \/.*$ ]] && echo $BASH_REMATCH)
        echo $first_file
        sliced_file=${first_file:1:500}
        echo $sliced_file
        compressed_file="${extracted_dir}/${sliced_file}.gz"
        echo $compressed_file
        if ! (test -f "${compressed_file}"); then
            flag=1
        fi
    };
    done 
    if [ "${flag}" -eq "0" ]; then 
        continue;
    fi
    unzip -j -n "$file" -d "$extracted_dir";
    i=$((i + 1))
    echo "Uncompressed dir ${file}"
    echo "${i} dirs done"
};
done;