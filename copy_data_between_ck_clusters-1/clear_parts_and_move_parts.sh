#!/bin/bash

source_host="xx.xx.xx.xx"
username="default"
password="Aiopschpro@2022"
database="aiops_local_prd"
clear_redundant_data_start="20250211"
clear_redundant_data_end=$(date +%Y%m%d)

echo "all start"

date_to_timestamp() {
    date -d "${1:0:4}-${1:4:2}-${1:6:2}" +%s
}

# Table
sql="select name from system.tables where database='${database}' and engine!='MaterializedView' and name not like '%inner%';"
tables=($(clickhouse-client --host 127.0.0.1 --port 9000 --user ${username} --password ${password} --query "$sql"))
for table in ${tables[@]}; do
  echo "${table} start..."

  # source path
  sql="select data_paths from system.tables ARRAY JOIN data_paths where database='${database}' and name='${table}'"
  path=$(clickhouse-client --host $source_host --port 9000 --user $username --password $password --query "$sql")

  # data file passed by rsync is stored at '/data01/chwork/store/'
  path=$(echo "$path" | sed 's/clickhouse/chwork/')

  # delete redundant data files
  rm -rf ${path}detached
  rm -rf ${path}format_version.txt
  current_ts=$(date_to_timestamp "$clear_redundant_data_start")
  end_ts=$(date_to_timestamp "$clear_redundant_data_end")
  while [[ $current_ts -le $end_ts ]]
  do
    cleardate=$(date -d @$current_ts +%Y%m%d)
    # echo ${path}${cleardate}*
    rm -rf ${path}${cleardate}*
    current_ts=$((current_ts + 86400))
  done

  # target path
  target_sql="select data_paths || 'detached/' from system.tables ARRAY JOIN data_paths where database='${database}' and name = '${table}'"
  target_path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$target_sql")

  # move data files
  mv ${path}*  ${target_path}

  echo "${table} finish!"
done

# Materialized view table
sql="select name from system.tables where database='${database}' and engine='MaterializedView';"
mv_tables=($(clickhouse-client --host 127.0.0.1 --port 9000 --user ${username} --password ${password} --query "$sql"))
for table in ${mv_tables[@]}; do
  echo "${table} start..."

  # source path
  sql="select data_paths from system.tables ARRAY JOIN data_paths where database='${database}' and name like '.inner%' and engine_full like '%${table}%'"
  path=$(clickhouse-client --host $source_host --port 9000 --user $username --password $password --query "$sql")

  # data file passed by rsync is stored at '/data01/chwork/store/'
  path=$(echo "$path" | sed 's/clickhouse/chwork/')
  
  # delete recent-date-data-file that needn't attach
  rm -rf ${path}detached
  rm -rf ${path}format_version.txt
  current_ts=$(date_to_timestamp "$clear_redundant_data_start")
  end_ts=$(date_to_timestamp "$clear_redundant_data_end")
  while [[ $current_ts -le $end_ts ]]
  do
    cleardate=$(date -d @$current_ts +%Y%m%d)
    # echo ${path}${cleardate}*
    rm -rf ${path}${cleardate}*
    current_ts=$((current_ts + 86400))
  done

  # target path
  target_sql="select data_paths || 'detached/' from system.tables ARRAY JOIN data_paths where database='${database}' and name like '.inner%' and engine_full like '%${table}%'"
  target_path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$target_sql")

  # move data files
  mv ${path}*  ${target_path}

  echo "${table} finish!"
done

echo "all finish"