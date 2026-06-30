#!/bin/bash

ip="127.0.0.1"
database_prd="aiops_local_test"
partitions=("'2024-05-15'" "'2024-05-14'" "'2024-05-13'" "'2024-05-12'" "'2024-05-11'" "'2024-05-10'" "'2024-05-09'" "'2024-05-08'" "'2024-05-07'" "'2024-05-06'" "'2024-05-05'" "'2024-05-04'" )


##aiops_collect_1_mv_minute1_20240516
for partition in ${partitions[@]}
 do
   clickhouse-client --host ${ip} --port 9000 --user default --password Aiopschuat@2023 --query "INSERT INTO aiops_local_test.aiops_collect_1_mv_minute1_20240516(product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency) SELECT product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency FROM aiops_local_test.aiops_collect_1_mv_minute1 WHERE time >=${partition} and time <date_add(HOUR, 3, toDateTime(${partition}));"
   
   clickhouse-client --host ${ip} --port 9000 --user default --password Aiopschuat@2023 --query "INSERT INTO aiops_local_test.aiops_collect_1_mv_minute1_20240516(product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency) SELECT product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency FROM aiops_local_test.aiops_collect_1_mv_minute1 WHERE time >=date_add(HOUR, 3, toDateTime(${partition})) and time <date_add(HOUR, 6, toDateTime(${partition}));"

   clickhouse-client --host ${ip} --port 9000 --user default --password Aiopschuat@2023 --query "INSERT INTO aiops_local_test.aiops_collect_1_mv_minute1_20240516(product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency) SELECT product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency FROM aiops_local_test.aiops_collect_1_mv_minute1 WHERE time >=date_add(HOUR, 6, toDateTime(${partition})) and time <date_add(HOUR, 9, toDateTime(${partition}));"

   clickhouse-client --host ${ip} --port 9000 --user default --password Aiopschuat@2023 --query "INSERT INTO aiops_local_test.aiops_collect_1_mv_minute1_20240516(product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency) SELECT product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency FROM aiops_local_test.aiops_collect_1_mv_minute1 WHERE time >=date_add(HOUR, 9, toDateTime(${partition})) and time <date_add(HOUR, 12, toDateTime(${partition}));"

   clickhouse-client --host ${ip} --port 9000 --user default --password Aiopschuat@2023 --query "INSERT INTO aiops_local_test.aiops_collect_1_mv_minute1_20240516(product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency) SELECT product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency FROM aiops_local_test.aiops_collect_1_mv_minute1 WHERE time >=date_add(HOUR, 12, toDateTime(${partition})) and time <date_add(HOUR, 15, toDateTime(${partition}));"

   clickhouse-client --host ${ip} --port 9000 --user default --password Aiopschuat@2023 --query "INSERT INTO aiops_local_test.aiops_collect_1_mv_minute1_20240516(product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency) SELECT product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency FROM aiops_local_test.aiops_collect_1_mv_minute1 WHERE time >=date_add(HOUR, 15, toDateTime(${partition})) and time <date_add(HOUR, 18, toDateTime(${partition}));"

   clickhouse-client --host ${ip} --port 9000 --user default --password Aiopschuat@2023 --query "INSERT INTO aiops_local_test.aiops_collect_1_mv_minute1_20240516(product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency) SELECT product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency FROM aiops_local_test.aiops_collect_1_mv_minute1 WHERE time >=date_add(HOUR, 18, toDateTime(${partition})) and time <date_add(HOUR, 21, toDateTime(${partition}));"

   clickhouse-client --host ${ip} --port 9000 --user default --password Aiopschuat@2023 --query "INSERT INTO aiops_local_test.aiops_collect_1_mv_minute1_20240516(product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency) SELECT product,service,itf,addn,time,total,success,fail,latency,maxLatency,minLatency,sumLatency FROM aiops_local_test.aiops_collect_1_mv_minute1 WHERE time >=date_add(HOUR, 21, toDateTime(${partition})) and time <date_add(HOUR, 24, toDateTime(${partition}));"
   
   echo "${database_prd}.aiops_collect_1_mv_minute1_20240516-${partition} data has been successfully restored!"
done
echo "${database_prd}.aiops_collect_1_mv_minute1_20240516 data has been successfully restored!"

