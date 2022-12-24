#!/bin/bash

data_dir="/home/ec2-user/press_release_data"
script_dir="/home/ec2-user/data_setup_scripts"

#rm press_release_headlines.db 
rm *_time_overall*
rm *_time_so_far*
rm db_progress.txt

python3 "${script_dir}/create_press_release_db.py"

# for index in {152..1500}
# do
# 	start=$(($index * 100))
# 	end=$(($start + 100))
# 	sed -n -e "${start},${end}p" files_todo.csv > curr_gz_batch.txt
# 	python3 "${script_dir}/__pycache__/parse_gz_headlines_batched.cpython-37.pyc" $start
# done
i=0

cat "${script_dir}/files_todo.csv" | while read file; 
do
	#if (( i < 40585 )); then 
	# 	i=$((i+1))
	# 	continue;
	#fi

	echo $i $file
	python3 "${script_dir}/__pycache__/parse_gz_headline_files.cpython-37.pyc" "${data_dir}/${file}" "${i}"
	# if [ "$i" -eq "$lim" ]; then
	# 	break;
	# fi
	i=$((i+1))
	break
done;
