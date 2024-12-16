#!/bin/bash

source_host="127.0.0.1"
username="default"
password="xxxxxx@2022"
database="aiops_local_prd"
tables=("aiops_ckdata_statistics" "aiops_collect_1" "aiops_collect_100000002" "aiops_collect_100000003" "aiops_collect_100000004" "aiops_collect_100000005" "aiops_collect_100000006" "aiops_collect_100000007" "aiops_collect_100000008" "aiops_collect_100000009" "aiops_collect_100000015" "aiops_collect_100000016" "aiops_collect_100000017" "aiops_collect_100000018" "aiops_collect_100000019" "aiops_collect_100000020" "aiops_collect_100000021" "aiops_collect_100000022" "aiops_collect_100000023" "aiops_collect_100000024" "aiops_collect_100000025" "aiops_collect_100000026" "aiops_collect_100000027" "aiops_collect_100000028" "aiops_collect_100000029" "aiops_collect_100000030" "aiops_collect_100000031" "aiops_collect_100000032" "aiops_collect_100000033" "aiops_collect_100000035" "aiops_collect_100000036" "aiops_collect_100000037" "aiops_collect_100000038" "aiops_collect_100000039" "aiops_collect_100000040" "aiops_collect_100000041" "aiops_collect_100000042" "aiops_collect_100000043" "aiops_collect_100000045" "aiops_collect_100000046" "aiops_collect_100000048" "aiops_collect_100000049" "aiops_collect_100000050" "aiops_collect_100000051" "aiops_collect_100000052" "aiops_collect_100000053" "aiops_collect_100000055" "aiops_collect_100000056" "aiops_collect_100000066" "aiops_collect_100000069" "aiops_collect_100000070" "aiops_collect_100000071" "aiops_collect_100000074" "aiops_collect_100000075" "aiops_collect_100000077" "aiops_collect_100000078" "aiops_collect_100000079" "aiops_collect_100000081" "aiops_collect_100000083")
mv_tables=("aiops_collect_1_mv_day1" "aiops_collect_100000093_mv_minute1" "aiops_collect_100000093_mv_minute2" "aiops_collect_100000093_mv_minute3" "aiops_collect_100000093_mv_minute4" "aiops_collect_12_mv_day1" "aiops_collect_12_mv_day2" "aiops_collect_12_mv_minute1" "aiops_collect_12_mv_minute2" "aiops_collect_12_mv_minute3" "aiops_collect_12_mv_minute4" "aiops_collect_12_mv_minute5" "aiops_collect_12_mv_minute6" "aiops_collect_1_mv_day2" "aiops_collect_1_mv_minute1" "aiops_collect_1_mv_minute2" "aiops_collect_1_mv_minute3" "aiops_collect_1_mv_minute4" "aiops_collect_300_mv_day1" "aiops_collect_300_mv_day2" "aiops_collect_300_mv_minute1" "aiops_collect_300_mv_minute2" "aiops_collect_300_mv_minute3" "aiops_collect_900000309_mv_day1" "aiops_collect_900000309_mv_minute1" "aiops_collect_900000309_mv_minute2" "aiops_collect_900000282_mv_minute1" "aiops_collect_900000313_mv_day1" "aiops_collect_900000313_mv_minute1" "aiops_collect_900000313_mv_minute2" "aiops_collect_900000350_mv_day1" "aiops_collect_900000350_mv_hour1" "aiops_collect_900000350_mv_minute1" "aiops_collect_900000350_mv_minute2" "aiops_collect_900000350_mv_minute3" "aiops_collect_900000350_mv_minute4")

echo "all start"
rm -rf /data01/chwork/sql/
mkdir -p /data01/chwork/sql/

attach_script=/data01/chwork/sql/attach_parts.sql
rm -rf ${attach_script}
touch ${attach_script}

# tables
for table in ${tables[@]}; do
  echo "${table} start..."

  # source path
  sql="select data_paths from system.tables ARRAY JOIN data_paths where database='${database}' and name='${table}'"
  path=$(clickhouse-client --host $source_host --port 9000 --user $username --password $password --query "$sql")

  # data file passed by rsync is stored at '/data01/chwork/store/'
  path=$(echo "$path" | sed 's/clickhouse/chwork/')
  # delete recent-date-data-file that needn't attach
  rm -rf ${path}20241204*
  rm -rf ${path}20241203*
  rm -rf ${path}20241202*
  rm -rf ${path}20241201*
  rm -rf ${path}20241130*
  rm -rf ${path}20241129*
  rm -rf ${path}detached
  rm -rf ${path}format_version.txt
  
  # target path
  target_sql="select data_paths || 'detached/' from system.tables ARRAY JOIN data_paths where database='${database}' and name = '${table}'"
  target_path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$target_sql")

  # gen & save attach sql
  files=$(find "${path}" -maxdepth 1 -type d ! -path "${path}")
  for file in $files; do
    filename=$(basename "$file")
    attach_sql="ALTER TABLE ${database}.\`${table}\` ATTACH PART '${filename}';"
    echo "$attach_sql" | awk '{print $0}' >> ${attach_script}
  done

  # move data files
  mv ${path}*  ${target_path}
  # clickhouse --client --host 127.0.0.1 --port 9000 --user $username --password $password --ignore-error --multiquery < ${attach_script}
  echo "${table} finish!"
done

# mv_tables
for table in ${mv_tables[@]}; do
  echo "${table} start..."

  # source path
  sql="select data_paths from system.tables ARRAY JOIN data_paths where database='${database}' and name like '.inner%' and engine_full like '%${table}%'"
  path=$(clickhouse-client --host $source_host --port 9000 --user $username --password $password --query "$sql")

  # data file passed by rsync is stored at '/data01/chwork/store/'
  path=$(echo "$path" | sed 's/clickhouse/chwork/')
  # delete recent-date-data-file that needn't attach
  rm -rf ${path}20241204*
  rm -rf ${path}20241203*
  rm -rf ${path}20241202*
  rm -rf ${path}20241201*
  rm -rf ${path}20241130*
  rm -rf ${path}20241129*
  rm -rf ${path}detached
  rm -rf ${path}format_version.txt
  
  # target path
  target_sql="select data_paths || 'detached/' from system.tables ARRAY JOIN data_paths where database='${database}' and name like '.inner%' and engine_full like '%${table}%'"
  target_path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$target_sql")

  # gen & save attach sql
  files=$(find "${path}")
  for file in $files; do
    filename=$(basename "$file")
    attach_sql="ALTER TABLE ${database}.\`${table}\` ATTACH PART '${filename}';"
    echo "$attach_sql" | awk '{print $0}' >> ${attach_script}
  done

  # move data files
  mv ${path}*  ${target_path}
  # clickhouse --client --host 127.0.0.1 --port 9000 --user $username --password $password --ignore-error --multiquery < ${attach_script}
  echo "${table} finish!"
done

echo "all finish"
