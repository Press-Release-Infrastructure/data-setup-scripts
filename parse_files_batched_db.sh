#!/bin/bash

data_dir="/home/ec2-user/press_release_data"
script_dir="/home/ec2-user/data_setup_scripts"

rm press_release_headlines.db 

python3 "${script_dir}/create_press_release_db.py"

ls $data_dir | while read folder;
do 
    if [ -d "${data_dir}/${folder}" ]; then
        echo "${data_dir}/${folder}" 
        python3 parse_gz_headlines_batched.py "${data_dir}/${folder}"
    fi
    break
done;
