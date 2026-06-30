#!/bin/bash

username="default"
password="Aiopschpro@2022"
database="aiops_local_prd"

echo "start"

tables=("aiops_collect_900000127")
for table in ${tables[@]}; do
  # target
  target_sql="select data_paths || 'detached/' from system.tables ARRAY JOIN data_paths where database='${database}' and name = '${table}_20240321'"
  target_path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$target_sql")
  # echo "${target_path}"

  # move
  mv /data01/chwork/data/${table}/* ${target_path}
  rm -rf ${target_path}detached
  rm -rf ${target_path}format_version.txt
  echo "${table} finish"
done

tables=("aiops_collect_900000127_mv_minute1")
for table in ${tables[@]}; do
  # target
  target_sql="select data_paths || 'detached/' from system.tables ARRAY JOIN data_paths where database='${database}' and name like '.inner%' and engine_full like '%${table}_20240321\'%'"
  target_path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$target_sql")
  # echo "${target_path}"

  # move
  mv /data01/chwork/data/${table}/* ${target_path}
  rm -rf ${target_path}detached
  rm -rf ${target_path}format_version.txt

  # rebuild detached dir
  sql="select data_paths from system.tables ARRAY JOIN data_paths where database='${database}' and name like '.inner%' and engine_full like '%${table}\'%'"
  path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$sql")
  if [ ! -d "${path}/detached" ]; then
    mkdir -p ${path}/detached
    chown -R clickhouse:clickhouse ${path}/detached
  fi

  echo "${table} finish"
done
echo "all finish"