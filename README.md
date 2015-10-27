# hdinsight-kafka-tools
Tools for Kafka clusters on Azure HDInsight

## Kafka partition replica rebalance tool
src/python/rebalance.py

### Background information
When you create a topic in Kafka, it creates partitions and place replicas across the cluster. However, Kafka does not know the topology of the cluster, so it could place all replicas of a partition on the same fault domain or update domain. When a fault or update happens in the cloud, that partition will be lost temporarily.

The provided rebalance tool reads cluster topology from Azure, reassigns replicas if such fault/update domain conflict has been found, therefore achieves high availability.

You need to run this tool after creating a new topic, or rescaling the cluster (scale up or scale down).

### How to use
Note that for now you need to run this scripts as root:
$sudo python rebalance.py <topic> [--execute]

Without "--execute" this tool only scans the current assignment generates the replica reassignment file.


