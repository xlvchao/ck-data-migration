#!/bin/bash

username="default"
password="Aiopschuat@2023"

echo "start"
rm -rf /data01/chwork/sql/
mkdir -p /data01/chwork/sql/

echo "create sql part"
clickhouse --client --host 127.0.0.1 --port 9000 --user "default" --password "Aiopschpro@2022" --ignore-error --query="select 'ALTER TABLE '||database||'.\`'||table||'\` ATTACH PART '''||name||''';' from system.detached_parts where partition_id <> 'inactive' and reason <> 'inactive' FORMAT TabSeparatedRaw;" > /data01/chwork/sql/attach_parts_new.sql

echo "start attach part"
clickhouse --client --host 127.0.0.1 --port 9000 --user $username --password $password --ignore-error --multiquery < /data01/chwork/sql/attach_parts.sql
echo "finish"