#!/bin/bash

s3_bucket=($(aws s3 ls s3://pressreleasedata))
hard_drive=($(find "/Volumes/Press Release Lexis Nexis/extracted_headlines" -exec basename \{} \;))

for i in $hard_drive; do 
    if ! [[ " ${s3_bucket[*]} " =~ " ${i} " ]]; then
        echo $i; 
    fi; 
done