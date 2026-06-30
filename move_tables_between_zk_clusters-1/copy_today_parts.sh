#!/bin/bash

username="default"
password="Aiopschpro@2022"
database="aiops_local_prd"

echo "start"
current_date=$(date +'%Y%m%d')

tables=("aiops_collect_900000127")
for table in ${tables[@]}; do
  # detele today file
  rm -rf /data01/chwork/data/${table}/${current_date}*

  # source
  sql="select data_paths from system.tables ARRAY JOIN data_paths where database='${database}' and name='${table}'"
  path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$sql")
  # echo "${path}"
  
  # copy
  cp -a ${path}/${current_date}* /data01/chwork/data/${table}/
  echo "${table} finish"
done

tables=("aiops_collect_900000127_mv_minute1")
for table in ${tables[@]}; do
  # detele today file
  rm -rf /data01/chwork/data/${table}/${current_date}*

  # source
  sql="select data_paths from system.tables ARRAY JOIN data_paths where database='${database}' and name like '.inner%' and engine_full like '%${table}\'%'"
  path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$sql")
  # echo "${path}"
  
  # copy
  cp -a ${path}/${current_date}* /data01/chwork/data/${table}/
  echo "${table} finish"
done
echo "all finish"