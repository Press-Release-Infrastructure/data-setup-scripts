#!/bin/bash

data_dir="/home/ec2-user/press-release-data"
script_dir="/home/ec2-user/data-setup-scripts"
i=0
lim=10

python3 "${script_dir}/create_press_release_db.py"

cat "${script_dir}/ls_output.txt" | while read file; 
do
	echo $file
	python3 "${script_dir}/parse_gz_headline_files.py" "${data_dir}/${file}"
	if [ "$i" -eq "$lim" ]; then
		break;
	fi
	i=$((i+1))
done;
