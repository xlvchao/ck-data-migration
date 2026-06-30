#!/bin/bash

clear_redundant_data_start="20250211"
end_date=$(date +%Y%m%d)


date_to_timestamp() {
    date -d "${1:0:4}-${1:4:2}-${1:6:2}" +%s
}

current_ts=$(date_to_timestamp "$clear_redundant_data_start")
end_ts=$(date_to_timestamp "$end_date")


while [[ $current_ts -le $end_ts ]]
do
    cleardate=$(date -d @$current_ts +%Y%m%d)
    echo $cleardate
    current_ts=$((current_ts + 86400))
done
