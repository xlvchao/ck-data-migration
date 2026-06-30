#!/bin/bash
password="Aiopschpro@2022"

clickhouse-client --host 172.30.32.130 --port 9000 --user default --password ${password} --query "select 'mv /data01/chwork/local_data/remote_data'||substr(data_paths,19)||'* /data01/clickhouse/data/'||database||'/'||name||'/detached/' from system.tables ARRAY JOIN data_paths where engine like 'Replicated%' FORMAT TabSeparatedRaw;" > /data01/chwork/tool/mv_data_to_detached.sh

chmod +x /data01/chwork/tool/mv_data_to_detached.sh

clickhouse-client --host 172.30.32.130 --port 9000 --user default --password ${password} --query "select 'rm -rf /data01/clickhouse/data/'||database||'/'||name||'/detached/detached' from system.tables ARRAY JOIN data_paths where engine like 'Replicated%' FORMAT TabSeparatedRaw;" > /data01/chwork/tool/remove_detached.sh

clickhouse-client --host 172.30.32.130 --port 9000 --user default --password ${password} --query "select 'rm -rf /data01/clickhouse/data/'||database||'/'||name||'/detached/format_version.txt' from system.tables ARRAY JOIN data_paths where engine like 'Replicated%' FORMAT TabSeparatedRaw;" >> /data01/chwork/tool/remove_detached.sh

chmod +x /data01/chwork/tool/remove_detached.sh