#!/bin/bash

data_dir="/home/ec2-user/press_release_data"

7za a "${data_dir}/press_release_headlines.db.7z" "${data_dir}/press_release_headlines.db"