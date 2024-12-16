#!/bin/bash

username="default"
password="xxxxxxx@2022"

clickhouse --client --host 127.0.0.1 --port 9000 --user $username --password $password --ignore-error --query="select 'ALTER TABLE '||database||'.\`'||table||'\` ATTACH PART '''||name||''';' from system.detached_parts where partition_id <> 'inactive' and reason <> 'inactive' FORMAT TabSeparatedRaw;" > /data01/chwork/sql/attach_parts_new.sql
