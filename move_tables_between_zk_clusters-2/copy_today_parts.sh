#!/bin/bash

# username="default"
# password="Aiopschuat@2023"
# database="aiops_local_test"

username="default"
password="Aiopschpro@2022"
database="aiops_local_prd"

target_table_suffix="20240415"

echo "all start"

rm -rf /data01/chwork/sql/
mkdir -p /data01/chwork/sql/

tables=("aiops_collect_900000350" "aiops_collect_100000093")
for table in ${tables[@]}; do
  echo "${table} start"

  # source
  sql="select data_paths from system.tables ARRAY JOIN data_paths where database='${database}' and name='${table}'"
  path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$sql")

  # target
  target_sql="select data_paths || 'detached' from system.tables ARRAY JOIN data_paths where database='${database}' and name = '${table}_${target_table_suffix}'"
  target_path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$target_sql")

  # copy & attach
  tdate=$(date -d "0 days ago" "+%Y%m%d")
  echo "${table}-${tdate} start"

  if [ "$(find "${path}" -maxdepth 1 -type d -name "${tdate}*" -o -type f -name "${tdate}*")" ]; then
    cp -a ${path}${tdate}*  ${target_path}
    attach_script=/data01/chwork/sql/${table}/${tdate}/attach_parts.sql
    if [ ! -f ${attach_script} ]; then
      mkdir -p /data01/chwork/sql/${table}/${tdate}/
      touch ${attach_script}
    fi
    files=$(find "${target_path}" -name "${tdate}*")
    for file in $files; do
      filename=$(basename "$file")
      attach_sql="ALTER TABLE ${database}.\`${table}_${target_table_suffix}\` ATTACH PART '${filename}';"
      echo "$attach_sql" | awk '{print $0}' >> ${attach_script}
    done
    clickhouse --client --host 127.0.0.1 --port 9000 --user $username --password $password --ignore-error --multiquery < ${attach_script}
  else
    echo "${table}-${tdate} parts doesn't found!"
  fi
  echo "${table}-${tdate} finish"

  echo "${table} finish"
done


tables=("aiops_collect_900000350_mv_day1" "aiops_collect_900000350_mv_hour1" "aiops_collect_900000350_mv_minute1" "aiops_collect_900000350_mv_minute2" "aiops_collect_900000350_mv_minute3" "aiops_collect_900000350_mv_minute4" "aiops_collect_100000093_mv_var1" "aiops_collect_100000093_mv_var2" "aiops_collect_100000093_mv_var3" "aiops_collect_100000093_mv_minute1" "aiops_collect_100000093_mv_minute2" "aiops_collect_100000093_mv_minute3" "aiops_collect_100000093_mv_minute4")
for table in ${tables[@]}; do
  echo "${table} start"

  # source
  sql="select data_paths from system.tables ARRAY JOIN data_paths where database='${database}' and name like '.inner%' and engine_full like '%${table}\'%'"
  path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$sql")
  echo "${path}"

  # target
  target_sql="select data_paths || 'detached/' from system.tables ARRAY JOIN data_paths where database='${database}' and name like '.inner%' and engine_full like '%${table}_${target_table_suffix}\'%'"
  target_path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$target_sql")
  echo "${target_path}"

  # copy & attach
  tdate=$(date -d "0 days ago" "+%Y%m%d")
  echo "${table}-${tdate} start"

  if [ "$(find "${path}" -maxdepth 1 -type d -name "${tdate}*" -o -type f -name "${tdate}*")" ]; then
    cp -a ${path}${tdate}*  ${target_path}
    attach_script=/data01/chwork/sql/${table}/${tdate}/attach_parts.sql
    if [ ! -f ${attach_script} ]; then
      mkdir -p /data01/chwork/sql/${table}/${tdate}/
      touch ${attach_script}
    fi
    files=$(find "${target_path}" -name "${tdate}*")
    for file in $files; do
      filename=$(basename "$file")
      attach_sql="ALTER TABLE ${database}.\`${table}_${target_table_suffix}\` ATTACH PART '${filename}';"
      echo "$attach_sql" | awk '{print $0}' >> ${attach_script}
    done
    clickhouse --client --host 127.0.0.1 --port 9000 --user $username --password $password --ignore-error --multiquery < ${attach_script}
  else
    echo "${table}-${tdate} parts doesn't found!"
  fi
  echo "${table}-${tdate} finish"

  echo "${table} finish"
done

echo "all finish"