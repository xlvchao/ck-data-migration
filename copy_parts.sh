#!/bin/bash

username="default"
password="Aiopschuat@2023"
database="aiops_local_test"
tables=("aiops_collect_1" "aiops_collect_1_mv_minute1")

echo "start"
rm -rf /data01/chwork/data/
mkdir -p /data01/chwork/data/

for table in ${tables[@]}; do
  # source
  sql="select data_paths || '*' from system.tables ARRAY JOIN data_paths where database='${database}' and name='${table}'"
  path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$sql")
  # echo "${path}"
  
  # copy
  mkdir /data01/chwork/data/${table}/
  cp -a ${path} /data01/chwork/data/${table}/
done
echo "finish"