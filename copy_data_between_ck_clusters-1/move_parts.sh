#!/bin/bash

source_host="xxx"
username="default"
password="Aiopschpro@2022"
database="aiops_local_prd"
tables=("aiops_ckdata_statistics" "aiops_collect_1" "aiops_collect_10" "aiops_collect_100000004" "aiops_collect_100000023" "aiops_collect_100000032" "aiops_collect_100000038" "aiops_collect_100000042" "aiops_collect_100000046" "aiops_collect_100000052" "aiops_collect_100000066" "aiops_collect_100000070" "aiops_collect_100000093" "aiops_collect_100000126" "aiops_collect_100000137" "aiops_collect_100000151" "aiops_collect_100000247" "aiops_collect_101" "aiops_collect_102" "aiops_collect_11" "aiops_collect_12" "aiops_collect_15" "aiops_collect_16" "aiops_collect_17" "aiops_collect_3" "aiops_collect_300" "aiops_collect_301" "aiops_collect_302" "aiops_collect_4" "aiops_collect_400000001" "aiops_collect_400000002" "aiops_collect_400000003" "aiops_collect_400000005" "aiops_collect_400000007" "aiops_collect_400000008" "aiops_collect_400000012" "aiops_collect_400000013" "aiops_collect_400000014" "aiops_collect_400000015" "aiops_collect_400000022" "aiops_collect_400000023" "aiops_collect_400000024" "aiops_collect_400000033" "aiops_collect_400000035" "aiops_collect_400000036" "aiops_collect_400000038" "aiops_collect_400000039" "aiops_collect_400000040" "aiops_collect_400000041" "aiops_collect_400000042" "aiops_collect_400000045" "aiops_collect_400000046" "aiops_collect_400000050" "aiops_collect_400000051" "aiops_collect_400000052" "aiops_collect_400000053" "aiops_collect_400000054" "aiops_collect_400000055" "aiops_collect_400000056" "aiops_collect_400000057" "aiops_collect_400000058" "aiops_collect_400000061" "aiops_collect_400000066" "aiops_collect_400000067" "aiops_collect_400000068" "aiops_collect_400000069" "aiops_collect_400000072" "aiops_collect_400000073" "aiops_collect_400000075" "aiops_collect_5" "aiops_collect_50" "aiops_collect_54" "aiops_collect_6" "aiops_collect_6000012" "aiops_collect_6000013" "aiops_collect_6000014" "aiops_collect_6000015" "aiops_collect_6000016" "aiops_collect_6000017" "aiops_collect_6000018" "aiops_collect_6000019" "aiops_collect_6000020" "aiops_collect_6000021" "aiops_collect_6000022" "aiops_collect_6000023" "aiops_collect_6000040" "aiops_collect_62" "aiops_collect_7" "aiops_collect_77" "aiops_collect_78" "aiops_collect_79" "aiops_collect_8" "aiops_collect_80" "aiops_collect_800000001" "aiops_collect_87" "aiops_collect_9" "aiops_collect_900000020" "aiops_collect_900000133" "aiops_collect_900000134" "aiops_collect_900000157" "aiops_collect_900000170" "aiops_collect_900000190" "aiops_collect_900000194" "aiops_collect_900000197" "aiops_collect_900000202" "aiops_collect_900000209" "aiops_collect_900000212" "aiops_collect_900000213" "aiops_collect_900000214" "aiops_collect_900000215" "aiops_collect_900000217" "aiops_collect_900000223" "aiops_collect_900000227" "aiops_collect_900000245" "aiops_collect_900000246" "aiops_collect_900000254" "aiops_collect_900000258" "aiops_collect_900000259" "aiops_collect_900000270" "aiops_collect_900000271" "aiops_collect_900000277" "aiops_collect_900000279" "aiops_collect_900000297" "aiops_collect_900000416" "aiops_collect_900000417" "aiops_collect_900000421" "aiops_collect_900000422" "aiops_collect_900000423" "aiops_collect_900000426" "aiops_collect_900000443" "aiops_collect_900000494" "aiops_collect_900000545" "aiops_collect_900000560" "aiops_collect_900000563" "aiops_collect_900000564" "aiops_collect_900000565" "aiops_collect_900000594" "aiops_collect_900000811" "aiops_collect_900000857" "aiops_collect_900000997" "aiops_collect_900001225" "aiops_collect_94" "aiops_collect_95" "aiops_collect_96" "aiops_collect_97")
mv_tables=("aiops_collect_100000093_mv_minute1" "aiops_collect_100000093_mv_minute2" "aiops_collect_100000093_mv_minute3" "aiops_collect_100000093_mv_minute4" "aiops_collect_100000093_mv_var1" "aiops_collect_100000093_mv_var2" "aiops_collect_100000093_mv_var3" "aiops_collect_1_mv_day1" "aiops_collect_1_mv_day2" "aiops_collect_1_mv_minute1" "aiops_collect_1_mv_minute2" "aiops_collect_1_mv_minute3" "aiops_collect_1_mv_minute4" "aiops_collect_300_mv_day1" "aiops_collect_300_mv_day2" "aiops_collect_300_mv_minute1" "aiops_collect_300_mv_minute2" "aiops_collect_300_mv_minute3")

echo "all start"
# rm -rf /data01/chwork/sql/
# mkdir -p /data01/chwork/sql/

# attach_script=/data01/chwork/sql/attach_parts.sql
# rm -rf ${attach_script}
# touch ${attach_script}

# tables
for table in ${tables[@]}; do
  echo "${table} start..."

  # source path
  sql="select data_paths from system.tables ARRAY JOIN data_paths where database='${database}' and name='${table}'"
  path=$(clickhouse-client --host $source_host --port 9000 --user $username --password $password --query "$sql")

  # data file passed by rsync is stored at '/data01/chwork/store/'
  path=$(echo "$path" | sed 's/clickhouse/chwork/')
  # delete recent-date-data-file that needn't attach
  rm -rf ${path}20250213*
  rm -rf ${path}detached
  rm -rf ${path}format_version.txt
  
  # target path
  target_sql="select data_paths || 'detached/' from system.tables ARRAY JOIN data_paths where database='${database}' and name = '${table}'"
  target_path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$target_sql")

  # gen & save attach sql
  # files=$(find "${path}" -maxdepth 1 -type d ! -path "${path}")
  # for file in $files; do
  #   filename=$(basename "$file")
  #   attach_sql="ALTER TABLE ${database}.\`${table}\` ATTACH PART '${filename}';"
  #   echo "$attach_sql" | awk '{print $0}' >> ${attach_script}
  # done

  # move data files
  mv ${path}*  ${target_path}
  # clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --ignore-error --multiquery < ${attach_script}
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
  rm -rf ${path}20250213*
  rm -rf ${path}detached
  rm -rf ${path}format_version.txt
  
  # target path
  target_sql="select data_paths || 'detached/' from system.tables ARRAY JOIN data_paths where database='${database}' and name like '.inner%' and engine_full like '%${table}%'"
  target_path=$(clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --query "$target_sql")

  # gen & save attach sql
  # files=$(find "${path}")
  # for file in $files; do
  #   filename=$(basename "$file")
  #   attach_sql="ALTER TABLE ${database}.\`${table}\` ATTACH PART '${filename}';"
  #   echo "$attach_sql" | awk '{print $0}' >> ${attach_script}
  # done

  # move data files
  mv ${path}*  ${target_path}
  # clickhouse-client --host 127.0.0.1 --port 9000 --user $username --password $password --ignore-error --multiquery < ${attach_script}
  echo "${table} finish!"
done

echo "all finish"