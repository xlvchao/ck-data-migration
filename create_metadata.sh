#!/bin/bash
password="Aiopschuat@2023"

#shard 1
newclusterip="10.70.81.29"
clickhouse-client --host ${newclusterip} --port 9000 --user default --password ${password} --ignore-error --multiquery < /root/create_table.sql > /root/create_table_${newclusterip}.log

newclusterip="10.70.81.221"
clickhouse-client --host ${newclusterip} --port 9000 --user default --password ${password} --ignore-error --multiquery < /root/create_table.sql > /root/create_table_${newclusterip}.log

#shard 2
newclusterip="10.70.85.205"
clickhouse-client --host ${newclusterip} --port 9000 --user default --password ${password} --ignore-error --multiquery < /root/create_table.sql > /root/create_table_${newclusterip}.log

newclusterip="10.70.83.169"
clickhouse-client --host ${newclusterip} --port 9000 --user default --password ${password} --ignore-error --multiquery < /root/create_table.sql > /root/create_table_${newclusterip}.log

#shard 3
newclusterip="10.70.84.166"
clickhouse-client --host ${newclusterip} --port 9000 --user default --password ${password} --ignore-error --multiquery < /root/create_table.sql > /root/create_table_${newclusterip}.log

newclusterip="10.70.80.208"
clickhouse-client --host ${newclusterip} --port 9000 --user default --password ${password} --ignore-error --multiquery < /root/create_table.sql > /root/create_table_${newclusterip}.log
