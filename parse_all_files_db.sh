#!/bin/bash

data_dir="/home/ec2-user/press_release_data"
script_dir="/home/ec2-user/data_setup_scripts"
i=0
lim=2000

rm press_release_headlines.db 
rm *_time_overall*
rm *_time_so_far*
rm db_progress.txt

python3 "${script_dir}/create_press_release_db.py"

cat "${script_dir}/ls_output.txt" | while read file; 
do
	python3 "${script_dir}/parse_gz_headline_files.py" "${data_dir}/${file}" "${i}"
	if [ "$i" -eq "$lim" ]; then
		break;
	fi
	i=$((i+1))
done;
