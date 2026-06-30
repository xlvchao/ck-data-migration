#!/bin/bash
password="Aiopschuat@2023"
clickhouse-client --host 10.70.81.29 --port 9000 --user default --password ${password} --query "select create_table_query||';' from system.tables where database not in ('system','default','_temporary_and_external_tables','aiops_dist_dev','aiops_local_dev') FORMAT TabSeparatedRaw;" > /data01/chwork/sql/create_table.sql

clickhouse-client --host 10.70.81.29 --port 9000 --user default --password ${password} --query "select database||'.'||name||';' from system.tables where database not in ('system','default','_temporary_and_external_tables','aiops_dist_dev','aiops_local_dev') FORMAT TabSeparatedRaw;" > /data01/chwork/sql/drop_table.sql