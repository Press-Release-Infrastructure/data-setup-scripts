#!/bin/bash

disk_dir="/Volumes/Press Release Lexis Nexis/"
extracted_dir="${disk_dir}/extracted_headlines"
i=0

find "${extracted_dir}" -regex ".*[0-9a-z]*-[0-9a-z]*-[0-9a-z]*-[0-9a-z]*-[0-9a-z]*$" | while IFS= read file 
do {
    gzip "${file}" #> "${file}.xml.gz"
    echo "Compressed ${file}"
    i=$((i + 1))
    echo "${i} files done"
};
done;

# remove all .xml.gz 0byte files at the end
