#!/bin/bash

username="default"
password="Aiopschuat@2023"
database="aiops_local_test"
tables=("aiops_collect_1" "aiops_collect_1_mv_day1" "aiops_collect_1_mv_day2" "aiops_collect_1_mv_minute1" "aiops_collect_1_mv_minute2" "aiops_collect_1_mv_minute3" "aiops_collect_1_mv_minute4")

echo "start"
for table in ${tables[@]}; do
  # target
  target_sql="select data_paths || 'detached/' from system.tables ARRAY JOIN data_paths where database='${database}' and name = '${table}'"
  target_path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$target_sql")
  # echo "${target_path}"

  # move
  mv /data01/chwork/data/${table}/* ${target_path}
  rm -rf ${target_path}detached
  rm -rf ${target_path}format_version.txt
  echo "${table} finish"
done
echo "all finish"