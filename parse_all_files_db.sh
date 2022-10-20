#!/bin/bash

data_dir="./press-release-data"
i=0
lim=10

cat "./data-setup-scripts/ls_output.txt" | while read file; 
do
	echo $file
	python3 "./data-setup-scripts/parse_gz_headline_files.py" "./press-release-data/${file}"
	if [ "$i" -eq "$lim" ]; then
		break;
	fi
	i=$((i+1))
done;
