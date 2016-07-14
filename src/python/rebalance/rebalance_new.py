'''
Rebalance Kafka partition replicas to achieve HA (Fault Domain/Update Domain awareness). Rebelance can be executed for one or more topics.

PRE-REQS:
=========
sudo apt-get install libffi-dev libssl-dev
sudo pip install --upgrade requests[security] PyOpenSSL ndg-httpsclient pyasn1

RUNNING THE SCRIPT:
===================

1) Copy the script to /usr/hdp/current/kafka-broker/bin on your cluster

2) Run this script with sudo privilege due to permission issues on some python packages:
sudo python rebalance.py
'''

import logging
import sys
import json
import subprocess
import os.path
import argparse
import requests
from retry import retry
from operator import itemgetter

from hdinsight_common import hdinsightlogging
from hdinsight_common.AmbariHelper import AmbariHelper
from hdinsight_common import Constants as CommonConstants
from hdinsight_common import cluster_utilities

from kazoo.client import KazooClient
from kazoo.client import KazooState

logger = logging.getLogger(__name__)
amabriHelper = AmbariHelper()

# Constants
ASSIGNMENT_JSON_FILE = "/tmp/rebalancePlan.json"
ZOOKEEPER_PORT = ":2181"
ZOOKEEPER_HOSTS = None
MAX_NUM_REPLICA = 3
BROKERS_ID_PATH = "brokers/ids"

# Returns the hosts in the Zookeeper Qorum
def get_zookeeper_hosts():
    global ZOOKEEPER_HOSTS
    if ZOOKEEPER_HOSTS is None:
        hosts = amabriHelper.get_host_components()
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
    return ZOOKEEPER_HOSTS

# Get the list of all topics in Kafka
def get_topic_list():
    s = subprocess.check_output([
        "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh",
        "--zookeeper",
        get_zookeeper_hosts(),
        "--list"
    ])
    if len(s) > 0:
        return s.split()
    else:
        return []

# Returns the cluster topology JSON from the cluster manifest 
def get_cluster_topology_json():
    cluster_manifest = amabriHelper.get_cluster_manifest()
    settings = cluster_manifest.settings
    #logger.info(settings)
    if "cluster_topology_json_url" in settings:
        json_url = settings["cluster_topology_json_url"]
        #logger.info("cluster_topology_json_url = %s", json_url)
        r = requests.get(json_url)
        topology_info = r.text
        return topology_info
    else:
        raise Exception("Failed to get cluster_topology_json_url from cluster manifest")

# Parses the cluster topology JSON and returns vmInfo
def parse_topo_info(cluster_topology_json, zookeeper_client):
    workernode_info = json.loads(cluster_topology_json)["hostGroups"]["workernode"]
    host_info = []
    # Get broker Ids to Hosts mapping
    brokers_info = get_brokerhost_info(zookeeper_client)
    for node in workernode_info:
        host = { 
            "vmId": node["vmId"],
            "FD": str(node["faultDomain"]),
            "UD": str(node["updateDomain"]),
            "fqdn": node["fqdn"],
            "brokerId": brokers_info[node["fqdn"]] if node["fqdn"] in brokers_info else None,
            "rack": "FD" + str(node["faultDomain"]) + "UD" + str(node["updateDomain"])
        }
        host_info.append(host);
    return host_info

# Returns info about a partitcular topic
def get_topic_info(topic):
    topicInfo = subprocess.check_output([
        "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh",
        "--zookeeper",
        get_zookeeper_hosts(),
        "--describe",
        "--topic",
        topic
    ])
    if topicInfo is None or len(topicInfo)==0:
        raise Exception("Failed to get Kafka partition info for topic " + topic)
    return topicInfo

# Returns info about partitions for a given topic
def get_partition_info(topic):
    topicInfo = get_topic_info(topic)
    topicInfo_lines = topicInfo.split('\n')
    if len(topicInfo_lines) < 2:
        raise Exception("Failed to parse Kafka partition info")
    
    summary = topicInfo_lines[0].split()
    partition_count = int(summary[1].split(":")[1])
    replica_count = int(summary[2].split(":")[1])
    if replica_count > MAX_NUM_REPLICA:
        raise Exception("Replica count exceeds threshold")
    
    partitions_info = []
    for i in range(1, len(topicInfo_lines)):
        if len(topicInfo_lines[i].strip())==0:
            continue
        partition_info = {}
        partition = int(topicInfo_lines[i].split('Partition: ')[1].split()[0])
        leader = int(topicInfo_lines[i].split('Leader: ')[1].split()[0])
        replicas = map(int, topicInfo_lines[i].split('Replicas: ')[1].split()[0].split(','))
        isr = map(int, topicInfo_lines[i].split('Isr: ')[1].split()[0].split(','))
        partition_info = {
            "partition": partition,
            "leader": leader,
            "replicas": replicas,
            "isr": isr
        }
        partitions_info.append(partition_info)
    return partitions_info

# Connect to Zookeeper
@retry(exceptions=BaseException, tries=CommonConstants.MAX_RETRIES, delay=CommonConstants.RETRY_INTERVAL_DELAY, backoff=CommonConstants.RETRY_INTERVAL_BACKOFF, logger=logger)
def connect(zk_quorum):
    logger.info('Connecting to zookeeper quorum at: {0}'.format(zk_quorum))
    zk = KazooClient(hosts=zk_quorum)
    zk.start()
    zk.add_listener(connection_lost)
    return zk

def connection_lost(state):
    if state == KazooState.LOST or state == KazooState.SUSPENDED:
        raise RuntimeError("Fatal error lost connection to zookeeper.")

def get_brokerhost_info(zookeeper_client):
    zk_brokers_ids = zookeeper_client.get_children(BROKERS_ID_PATH)
    brokers_info = {}
    for zk_broker_id in zk_brokers_ids:
        zk_broker_id_data, stat = zookeeper_client.get('{0}/{1}'.format(BROKERS_ID_PATH, zk_broker_id))
        zk_broker_info = json.loads(zk_broker_id_data)
        zk_broker_host = zk_broker_info['host'].split('.')[0]
        brokers_info[zk_broker_host] = zk_broker_id
    return brokers_info

def generate_reassignment_plan(topics, zookeeper_client):
    ret = None
    # Retrieve Cluster topology
    cluster_topology_json = get_cluster_topology_json()
    # Parse JSON to retrieve information about hosts 
    host_info = parse_topo_info(cluster_topology_json, zookeeper_client)
    for topic in topics:
        print "TOPIC: " + str(topic)
        partition_info = get_partition_info(topic)
        rassignment_Generator = ReassignmentGenerator(host_info, topic, partition_info)
        reassignment_plan = rassignment_Generator.generate_reassignment_plan_for_topic()
        if ret is None:
            ret = reassignment_plan
        else:
            ret["partitions"] += reassignment_plan["partitions"]
    if ret is not None:
        ret = json.dumps(ret)
        f = open(ASSIGNMENT_JSON_FILE, "w")
        f.write(ret)
        f.close()
    return ret

class ReassignmentGenerator:
    def __init__(self, host_info, topic, partition_info):
        self.host_info = host_info
        self.topic = topic
        self.partition_info = partition_info
        self.partitions_count = len(partition_info)
    
    def generate_alternated_ud_fd_list(self):
        # Get set of UDs & FDs
        ud_set = set()
        for val in self.host_info:
            ud_set.add(val['UD'])
        ud_list = sorted(ud_set)

        fd_set = set()
        for val in self.host_info:
            fd_set.add(val['FD'])
        fd_list = sorted(fd_set)

        fd_index = 0
        ud_index = 0
        ud_fd_list = []
        for index in range(0,len(fd_list)*len(ud_list)):
            ud_fd_list.append("FD" + fd_list[fd_index % len(fd_list)] + "UD" + ud_list[ud_index % len(ud_list)])
            fd_index += 1
            ud_index += 1
        return ud_fd_list
        
    def is_partition_eligible_reassignment(self, partition):
        if len(partition["isr"]) >= 1 and partition["leader"] in partition["isr"]:
            return True
        return False
    
    def get_brokers_in_rack(self, rack):
        return [element for element in self.host_info if element['rack'] == rack]
    
    def get_broker_info(self, broker_id):
        return [element for element in self.host_info if element['brokerId'] == broker_id][0]
    
    def get_count_replicas_in_broker(self, broker_id, broker_replica_count):
        return [element for element in broker_replica_count if element["brokerId"] == broker_id][0]
    
    def increment_count_replicas_in_broker(self, broker_id, broker_replica_count, type_of_count):
        e = [element for element in broker_replica_count if element["brokerId"] == broker_id][0]
        e[type_of_count] += 1
    
    def assign_replica_for_partition(self, rack_alternated_list, broker_replica_count, next_rack, type_of_replica):
        eligible_brokers = self.get_brokers_in_rack(rack_alternated_list[next_rack])
        new_broker = eligible_brokers[0]
        for broker in eligible_brokers:
            a = self.get_count_replicas_in_broker(broker["brokerId"], broker_replica_count)[type_of_replica]
            b = self.get_count_replicas_in_broker(new_broker["brokerId"], broker_replica_count)[type_of_replica]
            if a < b:
                new_broker = broker
        self.increment_count_replicas_in_broker(new_broker["brokerId"], broker_replica_count, type_of_replica)
        return new_broker["brokerId"]

    def scan_partition_for_reassignment(self, index, brokers_replica_count, rack_alternated_list, next_Leader, next_Follower, round_robin_iteration):
        reassignment = { "topic" : self.topic,
        "partition" : self.partition_info[index]['partition'],
        "replicas" : []
        }

        replica_count = len(self.partition_info[index]["replicas"])
        rack_count = len(rack_alternated_list)  
        shift = rack_count * round_robin_iteration

        # Re-assign Leader
        leader_broker_id = self.assign_replica_for_partition(rack_alternated_list, brokers_replica_count, next_Leader % rack_count, "leaders")
        next_Leader += 1
        reassignment["replicas"].append(leader_broker_id)

        # Re-assign follower replicas
        if replica_count > 1:
            for j in range(0,replica_count-1):
                follower_broker_id = self.assign_replica_for_partition(rack_alternated_list, brokers_replica_count, (next_Follower + j + shift) % rack_count, "followers")
                reassignment["replicas"].append(follower_broker_id)
            next_Follower += 1

        print "Changing " + str(self.partition_info[index]["replicas"]) + " to " + str(reassignment["replicas"])

        # Check if round robin complete
        if next_Follower % rack_count == 0:
            round_robin_iteration += 1

        return reassignment, next_Leader, next_Follower, round_robin_iteration

    def generate_reassignment_plan_for_topic(self):
        ret = None
        reassignment={"partitions":[], "version":1}

        # Keep track of numbers of replicas assigned to each broker
        brokers_replica_count = []
        for host in self.host_info:
            b = {
                "brokerId" : host["brokerId"],
                "leaders" : 0,
                "followers": 0,
            }
            brokers_replica_count.append(b) 

        rack_alternated_list = self.generate_alternated_ud_fd_list()
        print rack_alternated_list

        next_Leader = 0
        next_Follower = 1
        round_robin_iteration = 0
        
        for i in range(0,len(self.partition_info)):
            if self.is_partition_eligible_reassignment(self.partition_info[i]):
                r, next_Leader, next_Follower, round_robin_iteration = self.scan_partition_for_reassignment(i, brokers_replica_count, rack_alternated_list, next_Leader, next_Follower, round_robin_iteration)
                if r is not None:
                    reassignment["partitions"].append(r)
                    ret = reassignment
        return ret

def reassign_verify():
    s = subprocess.check_output(["/usr/hdp/current/kafka-broker/bin/kafka-reassign-partitions.sh",
    "--zookeeper",
    get_zookeeper_hosts(),
    "--reassignment-json-file " + ASSIGNMENT_JSON_FILE,
    "--verify"
    ])
    logger.info(s)

def reassign_exec():
    s = subprocess.check_output(["/usr/hdp/current/kafka-broker/bin/kafka-reassign-partitions.sh",
    "--zookeeper",
    "--reassignment-json-file " + ASSIGNMENT_JSON_FILE,
    "--execute"
    ])
    logger.info(s)
    if "Successfully started reassignment of partitions" not in s:
        raise Exception("Operation Failed!")

def main():
    parser = argparse.ArgumentParser(description='Rebalance Kafka Replicas! :)')
    parser.add_argument('--topics', nargs='+', help='list of topics to reassign replicas')
    parser.add_argument('--balanceLeaders', action="store_true", help='Balance Leaders')
    parser.add_argument('--execute', nargs='?', const='true', default='false', help='whether or not to execute the reassignment plan')
    parser.add_argument('--verify', nargs='?', const='true', default='false', help='verify execution of the reassignment plan')
    args = parser.parse_args()

    topics = args.topics
    if topics is None:
        topics = get_topic_list()
    logger.info("Rebalancing following topics: %s", str(topics))

    # Initialize Zookeeper Client
    zookeeper_qorum = cluster_utilities.get_zk_quorum()
    zookeeper_client = connect(zookeeper_qorum)

    reassignment_plan = generate_reassignment_plan(topics, zookeeper_client)

    if args.execute=='true':
            reassign_exec()
    else:
        logger.info("Please run this command with '--execute'")
        logger.info("This is the reassignment-json-file, saved as %s", ASSIGNMENT_JSON_FILE)
        logger.info(r)

if __name__ == "__main__":
    hdinsightlogging.initialize_root_logger()
    main()