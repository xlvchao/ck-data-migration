-- 2、创建读写账号：aiops/Aiops@2022
CREATE USER aiops ON CLUSTER 'cluster_2shards_2replicas' IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'Aiops@2022';
GRANT ON CLUSTER 'cluster_2shards_2replicas' REMOTE ON *.* TO aiops;
GRANT ON CLUSTER 'cluster_2shards_2replicas' ALL ON aiops_local_prd.* TO aiops WITH GRANT OPTION;
GRANT ON CLUSTER 'cluster_2shards_2replicas' ALL ON aiops_dist_prd.* TO aiops WITH GRANT OPTION;
GRANT ON CLUSTER 'cluster_2shards_2replicas' SELECT ON system.* TO aiops WITH GRANT OPTION;
