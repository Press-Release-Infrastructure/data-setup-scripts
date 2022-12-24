#!/bin/bash

data_dir="/home/ec2-user/press_release_data"

cat "ls_output.txt" | while read file;
do 
    folder_name=${file:0:3}
    if [ ! -d "${data_dir}/${folder_name}" ]; then
        mkdir "${data_dir}/${folder_name}"
    fi
    if [ -f "${data_dir}/${folder_name}/${file}" ]; then 
        rm "${data_dir}/${file}"
        continue
    fi
    mv "${data_dir}/${file}" "${data_dir}/${folder_name}/${file}"
    echo "${file}"
done;
