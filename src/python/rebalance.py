"""
Rebalance Kafka partition replicas for a list of topics to achieve HA, given the
topology info (upgrade domain/fault domain) about each broker.

You need to run this scripts with admin privilege, i.e.: sudo python rebalance.py -h

Tested for Kafka 0.8.1.1
"""
import sys
import json
import subprocess
import os.path
import argparse
import requests
from hdinsight_common.AmbariHelper import AmbariHelper

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

    s = subprocess.check_output(["./kafka-topics.sh",
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
    if topo_info is None:
        topo_info = parse_topo_info(get_topo_json_str())
    ret = None
    for topic in topics:
        s = subprocess.check_output(["./kafka-topics.sh",
            ZOOKEEPER_PARAMS,
            "--describe",
            "--topic " + topic])
        if s is None or len(s)==0:
            raise Exception("Failed to get Kafka partition info for topic " + topic)
        partitions_info = parse_partitions_info(s)

        rgen = ReassignmentGenerator(topo_info, topic, partitions_info)
        r = rgen.reassign()
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
    s = subprocess.check_output(["./kafka-reassign-partitions.sh",
        ZOOKEEPER_PARAMS,
        "--reassignment-json-file " + REASSIGN_FILE_NAME,
        "--execute"])
    print s
    if "Successfully started reassignment of partitions" not in s:
        raise Exception("Operation Failed!")

def reassign_verify():
    global ZOOKEEPER_PARAMS
    if ZOOKEEPER_PARAMS is None:
        ZOOKEEPER_PARAMS = "--zookeeper " + get_zookeeper_connect_string()
    s = subprocess.check_output(["./kafka-reassign-partitions.sh",
        ZOOKEEPER_PARAMS,
        "--reassignment-json-file " + REASSIGN_FILE_NAME,
        "--verify"])
    print s

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
                    print "reassigning partition: " + str(partition) + \
                        ", broker " + str(b) + " to " + str(c)
                self.broker_load[b] -= 1
                self.broker_load[c] += 1
                self.sorted_broker.sort(key=lambda b:self.broker_load[b])
                return c
        raise Exception("Cannot reassign replica " + str(b) +
            " for partition " + str(partition))
    
def parse_partitions_info(s):
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
        partitions_info[partition] = replicas
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
    settings = ah.get_cluster_manifest().settings
    if "cluster_topology_json_url" in settings:
        json_url = ["cluster_topology_json_url"]
        r = requests.get(json_url)
        return r.text
    else:
        raise Exception("Failed to get cluster_topology_json_url from cluster manifest")

def parse_topo_info(s):
    v = json.loads(s)["hostGroups"]["workerNode"]
    topo_info = [[0,0] for i in range(len(v))]
    aid = -1
    for item in v:
        broker = item["vmId"]
        topo_info[broker][0] = item["updateDomain"]
        topo_info[broker][1] = item["faultDomain"]
        #make sure all VM falls into the same availability set
        if aid != -1 and item["availabilitySetId"] != aid:
            raise Exception("Not all VMs in the same availability set!")
        aid = item["availabilitySetId"]
    return topo_info

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
    print 'rebalancing topics: ' + str(topics)

    r = reassign_gen(topics)
    if r is None:
        print "No need to rebalance. Current Kafka replica assignment has HA"
        return

    if args.execute=='true':
        reassign_exec()
    else:
        print "Rebalance is needed. Please run this command with '--execute'"
        print "This is the reassignment-json-file, saved as " + REASSIGN_FILE_NAME
        print r

if __name__ == "__main__":
    main()