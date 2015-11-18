# hdinsight-kafka-tools
Tools for Kafka clusters on Azure HDInsight

## Kafka partition replica rebalance tool
src/python/rebalance.py

### Background information
When you create a topic in Kafka, it creates partitions and place replicas across the cluster. However, Kafka does not know the topology of the cluster, so it could place all replicas of a partition on the same fault domain or update domain. When a fault or update happens in the cloud, that partition will be lost temporarily.

The provided rebalance tool reads cluster topology from Azure, reassigns replicas if such fault/update domain conflict has been found, therefore achieves high availability.

You need to run this tool after creating a new topic, or rescaling the cluster (scale up or scale down).

### How to use
Copy the file to /usr/hdp/current/kafka-broker/bin, and run it (For now you need to run as root):

```
usage: rebalance.py [-h] [--topics TOPICS [TOPICS ...]] [--execute [EXECUTE]]
                    [--verify [VERIFY]]

Rebalance Kafka partition replicas for a list of topics to achieve High
Availability

optional arguments:
  -h, --help            show this help message and exit
  --topics TOPICS [TOPICS ...]
                        list of topics to reassign replicas, if not provided
                        reassign all topics
  --execute [EXECUTE]   whether or not to execute the reassignment plan
  --verify [VERIFY]     verify execution of the reassignment plan
```

Without "--execute" this tool only scans the current assignment generates the replica reassignment file.

To verify the rebalance progress, just do "sudo rebalance.py --verify".

## Kafka metrics retreival script
src/python/metrics.py

### Additional Information
The script queries Ambari metrics service to retreive point-in-time or time-based Kafka metrics.

### How to use
Copy the file to HDInsight Kafka (Storm) cluster headnode, and run it (For now you need to run as root):

```
usage: metrics.py [-h] [-t [TEMPORAL]]

Get Kafka metrics from Ambari

optional arguments:
  -h, --help            show this help message and exit
  -t [TEMPORAL], --temporal [TEMPORAL]
                        specify the time interval (in seconds) to get temporal
                        metrics (default: last 5 minutes metrics are
                        returned). NOT specifying this argument will return
                        current point-in-time metrics
```

### Troubleshooting
You may need to check the status of Ambari metrics service in Ambari to ensure this script works.

Known-Issues:
* Try switching between point-in-time and temporal modes for the script
* If you do not see metrics being returned for a longer period of time, try restarting the Ambari metrics service through Ambari interface.

### References
* https://cwiki.apache.org/confluence/display/AMBARI/Metrics
* https://cwiki.apache.org/confluence/display/AMBARI/Stack+Defined+Metrics
* https://cwiki.apache.org/confluence/display/AMBARI/Ambari+Metrics+API+specification
