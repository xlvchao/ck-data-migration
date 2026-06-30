#!/bin/bash

comma=","
database_pro="aiops_local_pro"
database_prd="aiops_local_prd"
tables=("aiops_collect_100000083")
partitions=("20230605" "20230606")

for table in ${tables[@]}; do
  #获取所有列，以逗号分割
  all_columns=""
  sql="select name from system.columns where database='${database_prd}' and table='${table}';"
  columns=($(clickhouse-client --host 127.0.0.1 --port 9000 --user default --password Aiopschpro@2022 --query "$sql"))
  for column in ${columns[@]}; do
    all_columns=$all_columns$comma$column
  done
  #echo ${all_columns:1}

  #从老库往新库导数据
  for partition in ${partitions[@]}; do
    clickhouse-client --host 127.0.0.1 --port 9000 --user default --password Aiopschpro@2022 --query "INSERT INTO ${database_prd}.${table} (${all_columns:1}) SELECT ${all_columns:1} FROM ${database_pro}.${table} where toYYYYMMDD(${database_pro}.${table}.time)=${partition}"
  done
  echo "${database_prd}.${table} data has been restored successfully!"
done
