#!/bin/bash

username="default"
password="Aiopschpro@2022"

clickhouse-client --host 127.0.0.1 --port 9000 --user ${username} --password ${password} --query "select 'create database if not exists '||name||' ;' from system.databases where name in ('aiops_local_prd','aiops_dist_prd') FORMAT TabSeparatedRaw;" > /data01/chwork/sql/create_database.sql