#!/bin/bash
password="Aiopschpro@2022"

#node 1
newclusterip="10.109.152.2"
clickhouse-client --host ${newclusterip} --port 9000 --user default --password ${password} --ignore-error --multiquery < /data01/chwork/sql/drop_database.sql > /data01/chwork/log/drop_database_${newclusterip}.log

#node 2
newclusterip="10.109.152.6"
clickhouse-client --host ${newclusterip} --port 9000 --user default --password ${password} --ignore-error --multiquery < /data01/chwork/sql/drop_database.sql > /data01/chwork/log/drop_database_${newclusterip}.log

