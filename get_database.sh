#!/bin/bash
password="Aiopschpro@2022"
clickhouse-client --host 172.30.32.130 --port 9000 --user default --password ${password} --query "select 'create database if not exists '||name||' ;' from system.databases where name not in ('system','default','_temporary_and_external_tables') FORMAT TabSeparatedRaw;" > /data01/chwork/sql/create_database.sql

clickhouse-client --host 172.30.32.130 --port 9000 --user default --password ${password} --query "select 'drop database if exists '||name||' ;' from system.databases where name not in ('system','default','_temporary_and_external_tables') FORMAT TabSeparatedRaw;" > /data01/chwork/sql/drop_database.sql