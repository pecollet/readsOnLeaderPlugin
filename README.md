# readsOnLeaderPlugin
Neo4j plugin to allow reads on cluster leader. It manipulates the routing tables to add the cluster leader in the list of members that can process READs.
By default Neo4j Causal Clusters direct clients to send reads to followers and read-replicas only.

## Notes
- this plugin is for Causal Cluster deployments
- this plugin is unnecessary for 4.2+, as the feature is part of the product
- master branch is compatible with Neo4j 4.0/4.1

## installation

### 4.0, 4.1
- compile the jar from master branch
- copy it to the plugins directories of your Neo4j instances
- set `causal_clustering.cluster_allow_reads_on_leader=true` in neo4j.conf on each instance
- start the Neo4j instances

### 3.5
- compile the jar from branch 3.5
- copy it to the plugins directories of your Neo4j instances
- set `causal_clustering.load_balancing.plugin=allow-reads-on-leader` & `causal_clustering.multi_dc_license=true` in neo4j.conf on each instance
- start the Neo4j instances
