"""
OVERVIEW:
=========
Rebalance Kafka partition replicas for a given topic to achieve High Availbility.
The rebalancing id one based on the topology info of cluster nodes i.e.
(upgrade domain/fault domain) about each broker node.

PRE-REQS:
=========
sudo apt-get install libffi-dev libssl-dev
sudo pip install --upgrade requests[security] PyOpenSSL ndg-httpsclient pyasn1

RUNNING THE SCRIPT:
===================
You need to run this script with sudo privilege due to permission issues on some python packages:
sudo python rebalance.py -h

Tested on HDInsight 3.2 (Linux Ubuntu 12) with HDP 2.2.7.1 and Kafka 0.8.1.1
"""

import logging
import sys
import json
import subprocess
import os.path
import argparse
import requests
from hdinsight_common import hdinsightlogging
from hdinsight_common.AmbariHelper import AmbariHelper

logger = logging.getLogger(__name__)

# Number of domain dimensions. currently 2: update domain, fault domain
TOPOLOGY_DIMENSION = 2
# Max number of replicas supported by this script
MAX_NUM_REPLICA = 3

LOG_INFO = True

# Used to mock topology info, [[update_domain, fault_domain], [...], ...]
# Set to None to disable mocking
#MOCKED_TOPO_INFO = [[0,0],[1,0],[2,1],[3,2]]
MOCKED_TOPO_INFO = None

REASSIGN_FILE_NAME = "/tmp/_to_move.json"
ZOOKEEPER_PORT = ":2181"
ZOOKEEPER_PARAMS = None

# Get the list of all topics in Kafka
def get_topic_list():
    global ZOOKEEPER_PARAMS
    if ZOOKEEPER_PARAMS is None:
        ZOOKEEPER_PARAMS = "--zookeeper " + get_zookeeper_connect_string()

    s = subprocess.check_output(["/usr/hdp/current/kafka-broker/bin/kafka-topics.sh",
        ZOOKEEPER_PARAMS,
        "--list"])
    if len(s) > 0:
        return s.split()
    else:
        return []

# Generate reassignment plan for multiple topics and save to a file
# Return the reassignment plan as a string
# Note: always call reassign_gen before calling reassign_exec
def reassign_gen(topics):
    rj = reassign_gen_json(topics)
    ret = None
    if rj is not None:
        ret = json.dumps(rj)
        f = open(REASSIGN_FILE_NAME, "w")
        f.write(ret)
        f.close()
    return ret

# Generate reassignment plan for multiple toppics as a dictionary
def reassign_gen_json(topics):
    global ZOOKEEPER_PARAMS
    if ZOOKEEPER_PARAMS is None:
        ZOOKEEPER_PARAMS = "--zookeeper " + get_zookeeper_connect_string()
    topo_info = MOCKED_TOPO_INFO
    vmid_to_index={i:i for i in range(len(topo_info))}
    index_to_vmid={i:i for i in range(len(topo_info))}
    if topo_info is None:
        topo_info,vmid_to_index,index_to_vmid = parse_topo_info(get_topo_json_str())
    ret = None
    for topic in topics:
        s = subprocess.check_output(["/usr/hdp/current/kafka-broker/bin/kafka-topics.sh",
            ZOOKEEPER_PARAMS,
            "--describe",
            "--topic " + topic])
        if s is None or len(s)==0:
            raise Exception("Failed to get Kafka partition info for topic " + topic)
        partitions_info = parse_partitions_info(s, vmid_to_index)

        rgen = ReassignmentGenerator(topo_info, topic, partitions_info)
        r = rgen.reassign()
        convert_reassign_json_vmid(r, index_to_vmid)
        if ret is None:
            ret = r
        else:
            #merge partitions from different topics
            ret["partitions"] += r["partitions"]
    return ret

def reassign_exec():
    global ZOOKEEPER_PARAMS
    if ZOOKEEPER_PARAMS is None:
        ZOOKEEPER_PARAMS = "--zookeeper " + get_zookeeper_connect_string()
    s = subprocess.check_output(["/usr/hdp/current/kafka-broker/bin/kafka-reassign-partitions.sh",
        ZOOKEEPER_PARAMS,
        "--reassignment-json-file " + REASSIGN_FILE_NAME,
        "--execute"])
    logger.info(s)
    if "Successfully started reassignment of partitions" not in s:
        raise Exception("Operation Failed!")

def reassign_verify():
    global ZOOKEEPER_PARAMS
    if ZOOKEEPER_PARAMS is None:
        ZOOKEEPER_PARAMS = "--zookeeper " + get_zookeeper_connect_string()
    s = subprocess.check_output(["/usr/hdp/current/kafka-broker/bin/kafka-reassign-partitions.sh",
        ZOOKEEPER_PARAMS,
        "--reassignment-json-file " + REASSIGN_FILE_NAME,
        "--verify"])
    logger.info(s)

'''
Generate reassign JSON string for Kafka partition replica reassignment tool
kafka-reassign-partitions.sh
'''
class ReassignmentGenerator:
    '''
    param: topo_info[][TOPOLOGY_DIMENSION] is the topology information of the
    cluster. It describes update domain/fault domain info of each node in the
    cluster.
    param: topic Kafka topic to reassign.
    param: partitions_info[][MAX_NUM_REPLICA] describes current replica
    assignment of each partition.
    '''
    def __init__(self, topo_info, topic, partitions_info):
        self.broker_count = len(topo_info)
        self.topo_info = topo_info
        self.topic = topic
        self.partition_count = len(partitions_info)
        self.partitions_info = partitions_info
        # broker_load keeps number of replicas on each broker
        self.broker_load = [0 for i in range(self.broker_count)]
        for pi in self.partitions_info:
            for broker in pi:
                self.broker_load[broker] += 1
        # sorted_broker is a list of broker index sorted from least number of
        # replicas to most number of replicas
        self.sorted_broker = sorted([i for i in range(self.broker_count)],
            key=lambda b:self.broker_load[b])
    
    '''
    return: dictionary that can be converted to JSON string to be used to
    reassign Kafka partitions, for example:
    {
        "partitions":
            [
                {
                "topic": "test.2",
                    "partition": 1,
                    "replicas": [1,2,4],
                }
            ],
        "version":1
    }
    If no conflict is found in current assignment, return None
    '''
    def reassign(self):
        ret = None
        reassignment={"partitions":[], "version":1}
        for i in range(self.partition_count):
            r = self._scan_partition_for_reassignment(i)
            if r is not None:
                reassignment["partitions"].append(r)
                ret = reassignment
        return ret
    
    '''
    Scan each replica from left to right, reassign the first replica that has
    conflict with previous replica. Having conflict means two brokers fall
    into the same domain in that dimension (update domain/fault domain).
    return: dictionary describing reassignment for partition
    '''
    def _scan_partition_for_reassignment(self, partition):
        ret = None
        seen = [set() for i in range(TOPOLOGY_DIMENSION)]
        leader = self.partitions_info[partition][0]
        reassignment = {"topic":self.topic, "partition":partition, "replicas":[leader]}
        for d in range(TOPOLOGY_DIMENSION):
            seen[d].add(self.topo_info[leader][d])
            
        for i in range(1, len(self.partitions_info[partition])):
            b = self.partitions_info[partition][i]
            if(self._is_conflict(seen, b)):
                b = self._reassign(partition, seen, b)
                #need to return reassignment dictionary
                ret = reassignment
            reassignment["replicas"].append(b)
            #mark dimentions as seen for broker b
            for d in range(TOPOLOGY_DIMENSION):
                seen[d].add(self.topo_info[b][d])

        return ret

    '''
    if broker b's domain in any dimension fall into one of the seen domains in
    that dimension, return True.
    param: seen[TOPOLOGY_DIMENSION] is a list of set, which contains the seen
    domains in each domain dimension.
    '''
    def _is_conflict(self, seen, b):
        for i in range(TOPOLOGY_DIMENSION):
            if self.topo_info[b][i] in seen[i]:
                return True
        return False
        
    def _reassign(self, partition, seen, b):
        for i in range(self.broker_count):
            c = self.sorted_broker[i]
            if not self._is_conflict(seen, c):
                #reassign broker b to broker c
                if LOG_INFO:
                    logger.info("reassigning partition: " + str(partition) + \
                        ", broker " + str(b) + " to " + str(c))
                self.broker_load[b] -= 1
                self.broker_load[c] += 1
                self.sorted_broker.sort(key=lambda b:self.broker_load[b])
                return c
        raise Exception("Cannot reassign replica " + str(b) +
            " for partition " + str(partition))

# Parse partition info from output of Kafka tools
# Since Kafka broker ID matches VM ID, we need to convert vmid to broker index
def parse_partitions_info(s, vmid_to_index):
    lines = s.split('\n')
    if len(lines) < 2:
        raise Exception("Failed to parse Kafka partition info")

    summary = lines[0].split()
    partition_count = int(summary[1].split(":")[1])
    replica_count = int(summary[2].split(":")[1])
    if replica_count > MAX_NUM_REPLICA:
        raise Exception("Replica count exceeds threshold")

    partitions_info = [[] for i in range(partition_count)]
    for i in range(1, len(lines)):
        if len(lines[i].strip())==0:
            continue
        partition = int(lines[i].split('Partition: ')[1].split()[0])
        replicas = map(int, lines[i].split('Replicas: ')[1].split()[0].split(','))
        index_replicas = map(lambda v: vmid_to_index[v], replicas)
        partitions_info[partition] = index_replicas
    return partitions_info

def get_zookeeper_connect_string():
    ah = AmbariHelper()
    hosts = ah.get_host_components()
    zkHosts = ""
    for item in hosts["items"]:
        if item["HostRoles"]["component_name"] == "ZOOKEEPER_SERVER":
            zkHosts += item["HostRoles"]["host_name"]
            zkHosts += ZOOKEEPER_PORT
            zkHosts += ","
    if len(zkHosts) > 2:
        return zkHosts[:-1]
    else:
        raise Exception("Failed to get Zookeeper information from Ambari!")

def get_topo_json_str():
    # TBD: the final solution is to read a file in local disk.
    # Need to wait until the VM agent is updated for this to work.

    # Read cluster manifest settings "cluster_topology_json_url"
    ah = AmbariHelper()
    cluster_manifest = ah.get_cluster_manifest()
    settings = cluster_manifest.settings
    logger.info(settings)
    if "cluster_topology_json_url" in settings:
        json_url = settings["cluster_topology_json_url"]
        logger.info("cluster_topology_json_url = %s", json_url)
        r = requests.get(json_url)
        topology_info = r.text
        logger.info(topology_info)
        return topology_info
    else:
        raise Exception("Failed to get cluster_topology_json_url from cluster manifest")

# Parse the topology json string, extract:
# 1) a list describing domains for each broker [[b1_d1,b1_d2], [b2_d1,b2_d2], ...]
# 2) VM ID to index map
# 3) index to VM ID map
# Note that 2) and 3) are needed because the VM IDs may contain gap, e.g. 0, 2, 3, 5
def parse_topo_info(s):
    v = json.loads(s)["hostGroups"]["workernode"]
    topo_info = [[0,0] for i in range(len(v))]
    vmid_to_index = {}
    index_to_vmid = {}
    logger.info(topo_info)
    aid = None
    index = 0
    for item in v:
        logger.info(item)
        vmid = item["vmId"]
        vmid_to_index[vmid] = index
        index_to_vmid[index] = vmid
        topo_info[index][0] = item["updateDomain"]
        topo_info[index][1] = item["faultDomain"]
        logger.info(topo_info[index])
        #make sure all VM falls into the same availability set
        if aid != None and item["availabilitySetId"] != aid:
            raise Exception("Not all VMs in the same availability set!")
        aid = item["availabilitySetId"]
        index += 1
    return topo_info,vmid_to_index, index_to_vmid

def convert_reassign_json_vmid(reassignment, index_to_vmid):
    if reassignment is None:
        return reassignment
    for r in reassignment["partitions"]:
        r["replicas"] = map(lambda i: index_to_vmid[i], r["replicas"])

def main():
    parser = argparse.ArgumentParser(description='Rebalance Kafka partition replicas for a list of topics to achieve High Availability')
    parser.add_argument('--topics', nargs='+', help='list of topics to reassign replicas, if not provided reassign all topics')
    parser.add_argument('--execute', nargs='?', const='true', default='false', help='whether or not to execute the reassignment plan')
    parser.add_argument('--verify', nargs='?', const='true', default='false', help='verify execution of the reassignment plan')
    args = parser.parse_args()

    if args.verify=='true':
        reassign_verify()
        return

    topics = args.topics
    if topics is None:
        topics = get_topic_list()
    logger.info('rebalancing topics: %s', str(topics))

    r = reassign_gen(topics)
    if r is None:
        logger.info("No need to rebalance. Current Kafka replica assignment has High Availability")
        return

    if args.execute=='true':
        reassign_exec()
    else:
        logger.info("Rebalance is needed. Please run this command with '--execute'")
        logger.info("This is the reassignment-json-file, saved as %s", REASSIGN_FILE_NAME)
        logger.info(r)

if __name__ == "__main__":
    hdinsightlogging.initialize_root_logger()
    main()
