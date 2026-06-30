#!/bin/bash

username="default"
password="Aiopschpro@2022"

predate=$(date -d "1 days ago" +%Y%m%d)

clickhouse-client --host 127.0.0.1 --port 9000 --user ${username} --password ${password} --query "select 'alter table aiops_local_prd.' || name || ' drop partition ' || '\'' || ${predate} || '\';' from system.tables where database='aiops_local_prd' and name not like '%inner%' FORMAT TabSeparatedRaw;" > /data01/chwork/sql/clear_bad_data.sql