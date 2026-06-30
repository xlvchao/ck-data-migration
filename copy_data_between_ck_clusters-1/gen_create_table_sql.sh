#!/bin/bash

username="default"
password="Aiopschpro@2022"

clickhouse-client --host 127.0.0.1 --port 9000 --user ${username} --password ${password} --query "select create_table_query||';' from system.tables where database in ('aiops_dist_prd','aiops_local_prd') and name not like '%inner%' FORMAT TabSeparatedRaw;" > /data01/chwork/sql/create_table.sql
