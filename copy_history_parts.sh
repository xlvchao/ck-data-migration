#!/bin/bash

username="default"
password="Aiopschuat@2023"
database="aiops_local_test"
tables=("aiops_collect_1" "aiops_collect_1_mv_day1" "aiops_collect_1_mv_day2" "aiops_collect_1_mv_minute1" "aiops_collect_1_mv_minute2" "aiops_collect_1_mv_minute3" "aiops_collect_1_mv_minute4")

echo "start"
rm -rf /data01/chwork/data/
mkdir -p /data01/chwork/data/

current_date=$(date +'%Y%m%d')

for table in ${tables[@]}; do
  # source
  sql="select data_paths || '*' from system.tables ARRAY JOIN data_paths where database='${database}' and name='${table}'"
  path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$sql")
  # echo "${path}"

  # copy
  mkdir /data01/chwork/data/${table}/
  cp -a ${path} /data01/chwork/data/${table}/

  # detele today file
  rm -rf /data01/chwork/data/${table}/${current_date}*
  rm -rf /data01/chwork/data/${table}/detached
  rm -rf /data01/chwork/data/${table}/format_version.txt
  echo "${table} finish"
done
echo "all finish"