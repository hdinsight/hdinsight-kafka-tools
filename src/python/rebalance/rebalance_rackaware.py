"""
Rebalance Kafka partition replicas to achieve HA (Fault Domain/Update Domain awareness). The tool distributes
replicas of partitions of a topic across brokers in a manner such that each replica is in a separate fault domain and
update domain. The tool also distributes the leaders such that each broker has approximately the same number of
leaders for partitions. Rebalance can be executed for one or more topics.
PRE-REQS:
=========
sudo apt-get install -y libffi-dev libssl-dev
sudo pip install --upgrade requests[security] PyOpenSSL ndg-httpsclient pyasn1 kazoo retry pexpect
RUNNING THE SCRIPT:
===================
1) Copy the script to /usr/hdp/current/kafka-broker/bin on your cluster
2) Run this script with sudo privilege due to permission issues on some python packages: sudo python rebalance_new.py
"""

import argparse
import errno
import json
import logging
import os
import os.path
import pexpect
import random
import re
import requests
import socket
import subprocess
import sys
import tempfile
import traceback
from logging import StreamHandler
from logging.handlers import RotatingFileHandler
from logging.handlers import SysLogHandler
from operator import itemgetter

from hdinsight_common.AmbariHelper import AmbariHelper
from kazoo.client import KazooClient
from kazoo.client import KazooState
from retry import retry

# LOGGING

logger = logging.getLogger(__name__)
log_file = "rebalance_log"
log_dir = "/var/log/kafka/"
SIMPLE_FORMATTER = logging.Formatter('%(asctime)s - %(filename)s [%(process)d] %(name)s - %(levelname)s - %(message)s')
SYSLOG_FORMAT_STRING = ' %(filename)s [%(process)d] - %(name)s - %(levelname)s - %(message)s'
SYSLOG_FORMATTER = logging.Formatter(SYSLOG_FORMAT_STRING)
MIN_LOG_LEVEL = logging.DEBUG
MAX_RETRIES = 3
RETRY_INTERVAL_DELAY = 1
RETRY_INTERVAL_BACKOFF = 2

'''Filters (lets through) all messages with level < LEVEL'''


class LogFilter(logging.Filter):
    def __init__(self, level):
        self.level = level

    def filter(self, log_record):
        return log_record.levelno < self.level


# LOG_LOCAL0 - belongs to hdinsight-agent
# LOG_LOCAL1 - belongs to ambari-agent
# LOG_LOCAL2 - belongs to syslog catch all
# We want to log to LOG_LOCAL2
def initialize_logger(logger, log_file, syslog_facility=SysLogHandler.LOG_LOCAL2):
    logger.setLevel(MIN_LOG_LEVEL)
    if not len(logger.handlers):
        add_console_handler(logger)
        add_file_handler(logger, log_file)
        add_syslog_handler(logger, syslog_facility)


'''Given a logger, we attach a console handler that will log only error messages'''


def add_console_handler(logger):
    stdout_handler = StreamHandler(sys.stdout)
    stderr_handler = StreamHandler(sys.stderr)
    log_filter = LogFilter(logging.WARNING)

    stdout_handler.addFilter(log_filter)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(SIMPLE_FORMATTER)

    stderr_handler.setLevel(max(MIN_LOG_LEVEL, logging.WARNING))
    stderr_handler.setFormatter(SIMPLE_FORMATTER)

    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)


'''Given a logger, we attach a rotating file handler that will log to the specified output file'''


def add_file_handler(logger, log_file_name):
    if not log_file_name.endswith('.log'):
        log_file_name = log_file_name + '.log'

    try:
        os.makedirs(log_dir)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(log_dir):
            pass
        else:
            raise_error('Unable to create log dir: {0}'.format(log_dir))

    log_file_path = os.path.join(log_dir, log_file_name)
    file_handler = RotatingFileHandler(filename=log_file_path, maxBytes=104857600, backupCount=100)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(SIMPLE_FORMATTER)

    logger.addHandler(file_handler)


# add syslog handler if we are on linux. We need to add retry on this because /dev/log might not be created by
# rsyslog yet
@retry(exceptions=BaseException, tries=MAX_RETRIES, delay=RETRY_INTERVAL_DELAY, backoff=RETRY_INTERVAL_BACKOFF)
def add_syslog_handler(logger, syslog_facility):
    try:
        mds_syslog_handler = SysLogHandler(address="/dev/log",
                                           facility=syslog_facility)
        mds_syslog_handler.setLevel(MIN_LOG_LEVEL)
        mds_syslog_handler.setFormatter(SYSLOG_FORMATTER)
        logger.addHandler(mds_syslog_handler)
        return True
    except Exception:
        raise_error('Exception occurred when adding syslog handler: ' + traceback.format_exc())
        return False


# Constants
KAFKA_BROKER = "kafka-broker"
ASSIGNMENT_JSON_FILE = "kafkaRebalancePlan.json"
ZOOKEEPER_PORT = ":2181"
BROKER_PORT = ":9092"
ZOOKEEPER_HOSTS = None
BROKERS_ID_PATH = "brokers/ids"
KAFKA_HOME = "/usr/hdp/current/kafka-broker/"
KAFKA_LIBS_PATH = KAFKA_HOME + "libs/"
KAFKA_TOPICS_TOOL_PATH = KAFKA_HOME + "bin/kafka-topics.sh"
KAFKA_REASSIGN_PARTITIONS_TOOL_PATH = KAFKA_HOME + "bin/kafka-reassign-partitions.sh"
FQDN = "fqdn"
BROKER_ID = "brokerId"
FAULT_DOMAIN = "faultDomain"
UPDATE_DOMAIN = "updateDomain"
FAULT_DOMAIN_SHORT = "FD"
UPDATE_DOMAIN_SHORT = "UD"
RACK = "rack"
VM_ID = "vmId"
PARTITION = "partition"
REPLICAS = "replicas"
LEADER = "leader"
LEADERS = "leaders"
FOLLOWERS = "followers"
TOPICS = "topics"
ISR = "isr"
FREE_DISK_SPACE = "fds"
PARTITION_SIZE = "partition_size"
ASSIGNED = "assigned"
DEFAULT_REBALANCE_THROTTLE_RATE_BPS = "50000000"
ALL_TOPICS_STRING = "all"

'''
Get information of Zookeeper Hosts
'''


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
        raise_error("Failed to get Zookeeper information from Ambari!")


'''
Get information of Broker Hosts
'''


def get_broker_connect_string():
    ah = AmbariHelper()
    hosts = ah.get_host_components()
    brokerHosts = ""
    for item in hosts["items"]:
        if item["HostRoles"]["component_name"] == "KAFKA_BROKER":
            brokerHosts += item["HostRoles"]["host_name"]
            brokerHosts += BROKER_PORT
            brokerHosts += ","
    if len(brokerHosts) > 2:
        return brokerHosts[:-1]
    else:
        raise_error("Failed to get Broker information from Ambari!")


'''
Returns a list of all topics in Kafka by executing the Kafka-topics tool
'''


def get_topic_list():
    try:
        kafka_version, hdp_version = get_kafka_hdp_version()
        if kafka_version >= '3.2.0':
            s = subprocess.check_output([
                KAFKA_TOPICS_TOOL_PATH,
                "--bootstrap-server",
                get_broker_connect_string(),
                "--list"
            ])
        else:
            s = subprocess.check_output([
                KAFKA_TOPICS_TOOL_PATH,
                "--zookeeper",
                get_zookeeper_connect_string(),
                "--list"
            ])

        if len(s) > 0:
            logger.info("List of all topics: %s", s.split())
            return s.split()
        else:
            return []
    except Exception:
        raise_error('Exception occurred when calling Kafka topics tool: ' + traceback.format_exc())
        sys.exit()


'''
Uses AmbariHelper from hdinsight-common to get the cluster manifest and parses it to get the cluster topology JSON object.
'''


def get_cluster_topology_json():
    ambariHelper = AmbariHelper()
    cluster_manifest = ambariHelper.get_cluster_manifest()
    settings = cluster_manifest.settings
    if "cluster_topology_json_url" in settings:
        json_url = settings["cluster_topology_json_url"]
        logger.info("Retrieved Cluster Topology JSON document.")
        logger.info("URL: %s", json_url)
        r = requests.get(json_url)
        topology_info = r.text
        logger.debug("Cluster Topology: %s", topology_info)

        return topology_info
    else:
        raise_error("Failed to get cluster_topology_json_url from cluster manifest.")


'''
Parses the cluster topology JSON doc and returns Host information
Returns: A list of dictionaries.
host_info = [
    {
        "vmId": 1
        "FD": 0
        "UD": 0
        "fqdn": 'wn0-k09v3'
        "brokerId": 1024,
        "rack": FD0UD0,
        "free_disk_space": 0
    },
]
'''


def parse_topo_info(cluster_topology_json, brokers_info, dead_hosts=None):
    logger.info("Parsing topology info to retrieve information about hosts.")
    workernode_info = json.loads(cluster_topology_json)["hostGroups"]["workernode"]
    host_info = []
    for node in workernode_info:
        host = {
            VM_ID: node[VM_ID],
            FAULT_DOMAIN: str(node[FAULT_DOMAIN]),
            UPDATE_DOMAIN: str(node[UPDATE_DOMAIN]),
            FQDN: node[FQDN],
            BROKER_ID: brokers_info[node[FQDN]] if node[FQDN] in brokers_info else None,
            RACK: FAULT_DOMAIN_SHORT + str(node[FAULT_DOMAIN]) + UPDATE_DOMAIN_SHORT + str(node[UPDATE_DOMAIN]),
            FREE_DISK_SPACE: 0
        }
        if dead_hosts:
            if not node[FQDN] in dead_hosts:
                host_info.append(host)
        else:
            host_info.append(host)
    return host_info


'''
Call the Kafka topic tool to get partition info about a topic. The return format is:
[
    'Topic:dummyTopic PartitionCount:13 ReplicationFactor:3 Configs:segment.bytes=104857600,cleanup.policy=compact,compression.type=uncompressed',
    ' Topic: dummyTopic Partition: 0 Leader: 1026 Replicas: 1026,1028,1014 Isr: 1026,1028,1014',
    ' Topic: dummyTopic Partition: 1 Leader: 1020 Replicas: 1020,1014,1017 Isr: 1020,1014,1017',
    ..
    ]
'''


def get_topic_info(topic):
    kafka_version, hdp_version = get_kafka_hdp_version()
    if kafka_version >= '3.2.0':
        topicInfo = subprocess.check_output([
            KAFKA_TOPICS_TOOL_PATH,
            "--bootstrap-server",
            get_broker_connect_string(),
            "--describe",
            "--topic",
            topic
        ])
    else:
        topicInfo = subprocess.check_output([
            KAFKA_TOPICS_TOOL_PATH,
            "--zookeeper",
            get_zookeeper_connect_string(),
            "--describe",
            "--topic",
            topic
        ])
    if topicInfo is None or len(topicInfo) == 0:
        raise_error("Failed to get Kafka partition info for topic " + topic)
    return topicInfo


def get_replica_count_topic(topic):
    topicInfo = get_topic_info(topic)
    topicInfo_lines = topicInfo.split('\n')
    if len(topicInfo_lines) < 2:
        raise_error("Failed to parse Kafka topic info")

    summaryWithSpaceFixed = topicInfo_lines[0].replace(": ", ":")
    summary = summaryWithSpaceFixed.split()
    replica_count = int(summary[2].split(":")[1])
    return replica_count


'''
Parses through the output of the Kafka Topic tools and returns info about partitions for a given topic.
Return format: partitions_info = [
    {
        "partition": partition,
        "leader": leader,
        "replicas": replicas,
        "isr": isr
    },
    ...
]
'''


def get_partition_info(topic, topicInfo_lines, partition_sizes):
    logger.info("Retrieving partition information for topic: %s", topic)
    partitions_info = []
    for i in range(1, len(topicInfo_lines)):
        if len(topicInfo_lines[i].strip()) == 0:
            continue
        partition = int(topicInfo_lines[i].split('Partition: ')[1].split()[0])
        leader = int(topicInfo_lines[i].split('Leader: ')[1].split()[0])
        replicas = map(int, topicInfo_lines[i].split('Replicas: ')[1].split()[0].split(','))
        isr = map(int, topicInfo_lines[i].split('Isr: ')[1].split()[0].split(','))
        partition_name = str(topic + "-" + str(partition))
        partition_size = 0
        if partition_sizes:
            if partition_name in partition_sizes:
                partition_size = int(partition_sizes[partition_name])
        partition_info = {
            PARTITION: partition,
            LEADER: leader,
            REPLICAS: replicas,
            ISR: isr,
            PARTITION_SIZE: partition_size,
            ASSIGNED: None
        }
        partitions_info.append(partition_info)

    # Return the list sorted by increasing partition size so that we rebalance the smaller partitions first
    return sorted(partitions_info, key=itemgetter(PARTITION_SIZE))


# Connect to Zookeeper
@retry(exceptions=BaseException, tries=MAX_RETRIES, delay=RETRY_INTERVAL_DELAY, backoff=RETRY_INTERVAL_BACKOFF,
       logger=logger)
def connect(zk_quorum):
    logger.info('Connecting to zookeeper quorum at: {0}'.format(zk_quorum))
    zk = KazooClient(hosts=zk_quorum)
    zk.start()
    zk.add_listener(connection_lost)
    return zk


def connection_lost(state):
    if state == KazooState.LOST or state == KazooState.SUSPENDED:
        raise RuntimeError("Fatal error lost connection to zookeeper.")


'''
Get broker ID to Host mapping from zookeeper.
Returns a dictionary:
    brokers_info = {
        'wn30-foobar': '1017',
        'wn25-foobar': '1016',
        'wn7-foobar': '1008',
        ..
        }
'''


def get_brokerhost_info(zookeeper_client):
    logger.info("Associating brokers to hosts...")
    zk_brokers_ids = zookeeper_client.get_children(BROKERS_ID_PATH)
    brokers_info = {}
    for zk_broker_id in zk_brokers_ids:
        zk_broker_id_data, stat = zookeeper_client.get('{0}/{1}'.format(BROKERS_ID_PATH, zk_broker_id))
        zk_broker_info = json.loads(zk_broker_id_data)
        zk_broker_host = _get_fqdn(zk_broker_info['host'])
        brokers_info[zk_broker_host] = zk_broker_id
    return brokers_info


def _valid_ipv4_address(host):
    sub_parts = host.split('.')
    if len(sub_parts) != 4:
        return False
    try:
        return all(0 <= int(part) < 256 for part in sub_parts)
    except ValueError:
        return False


def _get_fqdn(zk_broker_host):
    if _valid_ipv4_address(zk_broker_host):
        zk_broker_host = socket.getfqdn(zk_broker_host)
    return zk_broker_host.split('.')[0]


def generate_fd_list_ud_list(host_info):
    # Get set of UDs & FDs
    ud_set = set()
    for val in host_info:
        ud_set.add(val[UPDATE_DOMAIN])
    ud_list = sorted(ud_set)

    fd_set = set()
    for val in host_info:
        fd_set.add(val[FAULT_DOMAIN])
    fd_list = sorted(fd_set)

    return fd_list, ud_list


def check_brokers_up(host_info):
    for host in host_info:
        if not host[BROKER_ID]:
            logger.error(
                "VM %s with FQDN: %s has no brokers assigned. Ensure that all brokers are up! It is not recommended "
                "to perform replica rebalance when brokers are down.", host[VM_ID], host[FQDN])
            return
    return True


'''
Determine the free space available on the brokers along with the sizes of the partitions hosted on them.
Returns a dictionary of the format: {"topic-partition_number, size"}
'''


def get_storage_info(host_info):
    logger.info("Querying partition sizes from all brokers... This operation can take a few minutes.")
    global_partition_sizes = {}
    for host in host_info:
        free_disk_space, partition_sizes = get_partition_sizes(host[FQDN])
        host[FREE_DISK_SPACE] = int(free_disk_space)

        for i in range(0, len(partition_sizes)):
            partitions = partition_sizes[i].split(';')
            for p in partitions[1:-1]:
                p_name = str(p.split(',')[1].split('/')[2])
                p_size = int(p.split(',')[0])
                if p_name in global_partition_sizes and not (global_partition_sizes[p_name] is None):
                    global_partition_sizes[p_name] = max(global_partition_sizes[p_name], p_size)
                else:
                    global_partition_sizes[p_name] = p_size
    return global_partition_sizes


'''
Generate a replica reassignment JSON file to be passed to the Kafka Replica reassignment tool.
The method parses the cluster manifest to retrieve the topology information about hosts, including the fault & update domains. These are passed to the ReassignmentGenerator class which checks if each topic is already balanced and generates a reassignment plan if not.
Return value (reassignment json) is of the format:
{"partitions":
    [{"topic": "foo",
    "partition": 1,
    "replicas": [1,2,3] }
    ],
    "version":1
}
Sample Cluster Topology JSON:
{
    "hostGroups": {
        "gateway": [
            {
                ...
            },
            .
            .
        ],
        "headnode": [
            {
                ...
            },
            .
            .
        ],
        "workernode": [
            {
                "vmId": 0,
                "fqdn": "wn0-k09v3",
                "state": "Succeeded",
                "faultDomain": 0,
                "updateDomain": 2,
                "availabilitySetId": "/subscriptions/abe48551-c98b-4263-97b3-098a4c35bc08/resourcegroups/rg0-d373d1ab2fb94339ad55b18da21bb049resourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            .
            .
        ],
        "zookeepernode": [
            {
                ...
            }
        ]
    }
}
'''


def generate_reassignment_plan(plan_directory, topics, brokers_info, compute_storage_cost=False, dead_hosts=None,
                               force_rebalance=False):
    logger.info("Starting tool execution...")
    ret = None
    # Retrieve Cluster topology
    cluster_topology_json = get_cluster_topology_json()
    # Parse JSON to retrieve information about hosts
    host_info = parse_topo_info(cluster_topology_json, brokers_info, dead_hosts)
    partitions_sizes = []
    if compute_storage_cost:
        partitions_sizes = get_storage_info(host_info)
        logger.debug("Partition Sizes: %s", str(partitions_sizes))
    logger.debug("\nHost Information: \n%s", host_info)
    logger.debug("\n\nHost Rack Information: \n%s\n", "\n".join(
        list(map(lambda datum: "Broker: {0} Rack: {1}".format(str(datum['brokerId']), datum['rack']), host_info))))
    fd_list, ud_list = generate_fd_list_ud_list(host_info)
    fd_count, ud_count = len(fd_list), len(ud_list)

    logger.info("Checking if all brokers are up.")
    if check_brokers_up(host_info):

        # Keep track of number of replicas we assign to each broker. This count is across all topics
        brokers_replica_count = []
        for host in host_info:
            b = {
                BROKER_ID: host[BROKER_ID],
                LEADERS: 0,
                FOLLOWERS: 0,
            }
            brokers_replica_count.append(b)

        # Keep track of already balanced partitions across topics. This is so that we can verify # of leaders across
        # brokers at the end.
        global_balanced_partitions = []

        for topic in topics:
            logger.info("Getting topic information for Topic: %s", topic)
            # Get topic info using the Kafka topic tool
            topicInfo = get_topic_info(topic)
            replica_count_topic = get_replica_count_topic(topic)
            logger.info("Replica count for topic %s is %s", topic, replica_count_topic)
            logger.debug("Info for topic %s: \n%s", topic, topicInfo)
            topicInfo_lines = topicInfo.split('\n')
            if len(topicInfo_lines) < 2:
                raise_error("Failed to parse Kafka topic info for topic: %s", topic)

            partition_info = get_partition_info(topic, topicInfo_lines, partitions_sizes)
            logger.debug("Partition info for topic %s : \n%s", topic, partition_info)
            reassignment_generator = ReassignmentGenerator(host_info, topic, partition_info, compute_storage_cost)
            # Generate Rack alternated list
            fd_ud_list = reassignment_generator._generate_fd_ud_list()
            rack_alternated_list = reassignment_generator._generate_alternated_fd_ud_list(fd_ud_list, fd_list, ud_list)
            logger.debug("Generated Rack alternated list: %s", str(rack_alternated_list))

            # Variables to keep track of which rack in the alternated list is the next one to be assigned a replica. 
            rand_rack = random.randrange(0, len(rack_alternated_list))
            logger.debug("Rand_rack: %s", str(rand_rack))
            next_Leader = (int(len(ud_list)) * rand_rack) % len(rack_alternated_list)

            logger.debug("Start with position in Rack Alternated List: %s", str(next_Leader))

            reassignment_plan, balanced_partitions_for_topic = reassignment_generator._generate_reassignment_plan_for_topic(
                replica_count_topic, next_Leader, rack_alternated_list, fd_count, ud_count, brokers_replica_count,
                force_rebalance)
            logger.debug("\nAlready balanced partitions for the topic %s are: \n%s", topic,
                         balanced_partitions_for_topic)

            for partition in balanced_partitions_for_topic:
                global_balanced_partitions.append(partition)
            if reassignment_plan is not None:
                logger.info("\n\nReassignment plan generated for topic: %s\n%s\n", topic, reassignment_plan)
                if ret is not None:
                    ret["partitions"] += reassignment_plan["partitions"]
                else:
                    ret = reassignment_plan
                verify_plan = reassignment_generator._verify_reassignment_plan(reassignment_plan, topic,
                                                                               replica_count_topic, fd_count, ud_count)
                verify_leaders_distributed(host_info, ret, global_balanced_partitions)

        if ret is not None:
            verify_leader_count_balanced = verify_leaders_distributed(host_info, ret, global_balanced_partitions)

            # save the reassignment plan in ASSIGNMENT_JSON_FILE
            ret = json.dumps(ret)
            f = open(os.path.join(plan_directory, ASSIGNMENT_JSON_FILE), "w")
            f.write(ret)
            f.close()

            logger.info("\n\nComplete Reassignment plan for all topics! \n%s\n", ret)
        else:
            # remove contents from ASSIGNMENT_JSON_FILE
            open(os.path.join(plan_directory, ASSIGNMENT_JSON_FILE), "w").close()

    return ret


class ReassignmentGenerator:
    def __init__(self, host_info, topic, partition_info, compute_storage_cost):
        self.host_info = host_info
        self.topic = topic
        self.partition_info = partition_info
        self.partitions_count = len(partition_info)
        self.compute_storage_cost = compute_storage_cost

    def _generate_fd_ud_list(self):
        # Get set of FD+UDs
        fd_ud_set = set()
        for val in self.host_info:
            fd_ud_set.add(val[RACK])
        return sorted(fd_ud_set)

    def _get_fd_rack(self, rack):
        domains = re.findall(r'\d+', rack)
        return int(domains[0])

    def _get_ud_rack(self, rack):
        domains = re.findall(r'\d+', rack)
        return int(domains[1])

    def _gcd(self, a, b):
        while b:
            a, b = b, a % b
        return a

    '''Generates a list of alternated FD + UD combinations. List = [ (fd1,ud1) , (fd2,ud2), ... ] Example with 3 FDs 
    and 3 UDs : ['FD0UD0', 'FD1UD1', 'FD2UD2', 'FD0UD1', 'FD1UD2', 'FD2UD0', 'FD0UD2', 'FD1UD0', 'FD2UD1'] '''

    def _generate_alternated_fd_ud_list(self, fd_ud_list, fd_list, ud_list):
        alternated_list = []

        # Find largest FD# & UD#. This is required because there could be gaps and we need to know the largest # to
        # compute the possible FD x UD matrix. Missing combinations of (FD,UD) in the VMs allocated are not added to
        # the final list.
        fd_length = max(map(int, fd_list)) + 1
        ud_length = max(map(int, ud_list)) + 1

        i = 0
        j = 0
        k = 1

        # Traverse matrix in diagonal slices
        while True:
            current_rack = FAULT_DOMAIN_SHORT + str(i % fd_length) + UPDATE_DOMAIN_SHORT + str(j % ud_length)
            if current_rack in fd_ud_list:
                if not current_rack in alternated_list:
                    alternated_list.append(current_rack)
                # If FD + UD combo is already present in alternated_list, we are revisiting this the second time. Hence,
                # break out of the loop.
                else:
                    break
            i += 1

            # If matrix inputs are of form (n,n) or (m,m), add a shift to UD index so that we get a different
            # diagonal slice. To get the next adjacent diagonal slice, we add a shift by ud_length - 1.
            if self._gcd(ud_length, fd_length) > 1 and i % fd_length == 0:
                j = k
                k += 1
            else:
                j += 1

        return alternated_list

    '''
        Conditions required for a partition to be eligible for ReassignmentGenerator
        1> MIN(len(ISR)) >= 1
        2> Leader is in ISR
        3> Leader is assigned (not -1)
        4> Replicas are present
        5> Number of replicas for partition should be equal to replica count for topic
    '''

    def _is_partition_eligible_reassignment(self, partition, replica_count_topic):
        does_not_meet_criteria_msg = "Partition for topic does not meet criteria for rebalance. Skipping."
        partition[ASSIGNED] = False
        if not len(partition[ISR]) >= 1:
            logger.warning(
                "%s - Topic: %s, Partition: %s. Criteria not met: 'There should be at least one replica in the ISR'.",
                does_not_meet_criteria_msg, self.topic, partition[PARTITION])
            return False
        if not partition[LEADER] in partition[ISR]:
            logger.warning("%s - Topic: %s, Partition: %s. Criteria not met: 'The leader should be in the ISR'.",
                           does_not_meet_criteria_msg, self.topic, partition[PARTITION])
            return False
        if not int(partition[LEADER]) != -1:
            logger.warning(
                "%s - Topic: %s, Partition: %s. Criteria not met: 'There should be an assigned leader. Leader cannot "
                "be -1'.",
                does_not_meet_criteria_msg, self.topic, partition[PARTITION])
            return False
        if not partition[REPLICAS]:
            logger.warning("%s - Topic: %s, Partition: %s. Criteria not met: 'Replicas cannot be null'.",
                           does_not_meet_criteria_msg, self.topic, partition[PARTITION])
            return False
        partition[ASSIGNED] = True
        return True

    def _get_brokers_in_rack(self, rack):
        return [element for element in self.host_info if element[RACK] == rack]

    def _get_broker_info(self, b_id):
        retList = [element for element in self.host_info if int(element[BROKER_ID]) == b_id]
        if retList:
            return retList[0]
        else:
            logger.warn("Cannot retrieve host associated with broker with ID: %s", b_id)
            return None

    def _get_count_replicas_in_broker(self, broker_id, broker_replica_count):
        retList = [element for element in broker_replica_count if element[BROKER_ID] == broker_id]
        if retList:
            return retList[0]
        else:
            return None

    def _increment_count_replicas_in_broker(self, broker_id, broker_replica_count, type_of_count):
        retList = [element for element in broker_replica_count if element[BROKER_ID] == broker_id]
        if retList:
            e = retList[0]
            e[type_of_count] += 1
        else:
            logger.warning("%s not found in broker_replica_count!")

    def _get_weighted_count_replicas_in_rack(self, broker_replica_count, rack_alternated_list, rack_index,
                                             type_of_replica):
        count = 0
        brokers_in_rack = self._get_brokers_in_rack(rack_alternated_list[rack_index])
        if brokers_in_rack:
            for broker in brokers_in_rack:
                count += self._get_count_replicas_in_broker(broker[BROKER_ID], broker_replica_count)[type_of_replica]
            return count / float(len(brokers_in_rack))
        else:
            logger.error("No brokers were found for rack %s. Please verify brokers are up!",
                         rack_alternated_list[rack_index])
            return

    '''Determines the rack (FD+UD combination) for the replica. Once determined, there could be multiple brokers that 
    meet the criteria. We choose the broker which has less number of replicas assigned to it. (distribute the load) '''

    def _assign_replica_for_partition(self, rack_alternated_list, broker_replica_count, next_rack, type_of_replica):

        eligible_brokers = self._get_brokers_in_rack(rack_alternated_list[next_rack])
        if eligible_brokers:
            new_broker = eligible_brokers[0]
            for broker in eligible_brokers:
                a = self._get_count_replicas_in_broker(broker[BROKER_ID], broker_replica_count)[type_of_replica]
                b = self._get_count_replicas_in_broker(new_broker[BROKER_ID], broker_replica_count)[type_of_replica]
                if a < b:
                    new_broker = broker

            self._increment_count_replicas_in_broker(new_broker[BROKER_ID], broker_replica_count, type_of_replica)
            return new_broker[BROKER_ID]
        else:
            logger.error("No eligible brokers found for rack: %s", rack_alternated_list[next_rack])
            return

    '''
        This method reassigns the replicas for the given partition. The algorithm for assignment is as follows:
        1>  Iterate through the rack alternated list and look at sets of size replica_count.
            For 3 x 3: the list is: (0,0), (1,1), (2,2), (0,1), (1,2), (2,0), (0,2), (1,0), (2,1)
            In first iteration we look at: (0,0) (1,1) (2,2) if replica count is 3.
            Each of these represent racks for which there could be multiple brokers.
        2>  Determine which of the racks has the least number of leaders.
        3>  Assign this rack as the leader for the partition.
        4>  Determine all eligible brokers within this rack. Assign the broker with the least number of leaders within the rack       as the leader for this partition.
        5> Assign the remaining replicas to the 2 other racks in the set. These are follower replicas.
        This is to ensure we will not always get the same set of sequences.
        6> Look at the next set of 3 Racks and repeat from 1>
    '''

    def _scan_partition_for_reassignment(self, index, brokers_replica_count, rack_alternated_list, start_index,
                                         ud_count, replica_count_topic):
        reassignment = {"topic": self.topic,
                        PARTITION: int(self.partition_info[index][PARTITION]),
                        REPLICAS: []
                        }

        replica_count = int(replica_count_topic)
        rack_count = len(rack_alternated_list)

        '''
        Re-assign replicas for the PARTITION.
        Replicas will be distributed across following racks: start_index, start_index + 1, ...., start_index + replica_count - 1.
        '''
        # Determine which rack has fewest LEADERS
        current_min = sys.maxint
        relative_rack_index = 0
        for i in range(0, replica_count):
            leaders_in_current_rack = self._get_weighted_count_replicas_in_rack(brokers_replica_count,
                                                                                rack_alternated_list,
                                                                                (start_index + i) % rack_count, LEADERS)
            if leaders_in_current_rack < current_min:
                current_min = leaders_in_current_rack
                relative_rack_index = i
        rack_index = (start_index + relative_rack_index) % rack_count

        # Do the actual assignment of leader
        leader_broker_id = self._assign_replica_for_partition(rack_alternated_list, brokers_replica_count, rack_index,
                                                              LEADERS)

        # Check if there is sufficient space on the broker, if not set the "ASSIGNED" property of partition to False
        # to indicate that it was not assigned
        if self.compute_storage_cost:
            logger.debug("Checking if there is sufficient disk space on broker.")
            host_for_broker = [element for element in self.host_info if element[BROKER_ID] == leader_broker_id][0]
            free_disk_space = host_for_broker[FREE_DISK_SPACE]
            if free_disk_space < self.partition_info[index][PARTITION_SIZE]:
                logger.warning(
                    "Not sufficient disk space on elected leader: %s with broker ID: %s. Skipping rebalance for "
                    "partition: %s",
                    host_for_broker[FQDN], host_for_broker[BROKER_ID], self.partition_info[index][PARTITION])
                self.partition_info[index][ASSIGNED] = False
                return None, start_index % rack_count
            else:
                # Since we are assigning the partition to the broker, reduce the available free space by the size of
                # the partition
                host_for_broker[FREE_DISK_SPACE] -= self.partition_info[index][PARTITION_SIZE]
        reassignment[REPLICAS].append(int(leader_broker_id))

        # Assign replicas
        p_size = "N/A"
        for follower_index in range(0, replica_count):
            if follower_index != relative_rack_index:
                follower_broker_id = self._assign_replica_for_partition(rack_alternated_list, brokers_replica_count,
                                                                        (start_index + follower_index) % rack_count,
                                                                        FOLLOWERS)
                reassignment[REPLICAS].append(int(follower_broker_id))
                if self.compute_storage_cost:
                    host_for_broker = \
                        [element for element in self.host_info if element[BROKER_ID] == follower_broker_id][0]
                    host_for_broker[FREE_DISK_SPACE] -= self.partition_info[index][PARTITION_SIZE]
                    p_size = self.partition_info[index][PARTITION_SIZE]
        logger.debug("Topic: %s Reassigning Partition: %s of SIZE: %s from %s --> %s", self.topic,
                     self.partition_info[index][PARTITION], p_size, self.partition_info[index][REPLICAS],
                     reassignment[REPLICAS])
        self.partition_info[index][ASSIGNED] = True

        start_index += min(ud_count, replica_count)
        return reassignment, start_index % rack_count

    '''
        Iterate through all replicas of a topic to determine if it is balanced:
            1) Add the UDs of the replicas to a list - fd_list
            1) Add the UDs of the replicas to a list - fd_list
            2) Verify that number of domains the replicas are in is equal min(#replicas, #domains). This ensures that all replicas are in separate UDs and separate FDs.
    '''

    def _check_if_partition_balanced(self, partition, replica_count, fd_count, ud_count, brokers_replica_count,
                                     balanced_partitions):
        logger.debug("Checking if Partition %s is balanced.", partition)
        if len(partition[REPLICAS]) != replica_count:
            logger.warning(
                "The replica count for the partition is not the same as the replica count for the topic. Rebalance "
                "recommended.")
            return False

        fd_list, ud_list = [], []
        for replica in partition[REPLICAS]:
            # Get the rack associated with the replica and add to list
            host = self._get_broker_info(int(replica))
            # If host was removed from the rack the above will return null. In this case the partition is not balanced
            if not host:
                return False
            current_FD = host[FAULT_DOMAIN]
            current_UD = host[UPDATE_DOMAIN]
            fd_list.append(current_FD)
            ud_list.append(current_UD)

        if len(set(fd_list)) == min(fd_count, replica_count) and len(set(ud_list)) == min(ud_count, replica_count):
            if brokers_replica_count:
                # Update brokers_replica_count to keep track of number of leaders, followers across brokers
                self._increment_count_replicas_in_broker(str(partition[REPLICAS][0]), brokers_replica_count, LEADERS)
                for i in range(1, len(partition[REPLICAS])):
                    self._increment_count_replicas_in_broker(str(partition[REPLICAS][i]), brokers_replica_count,
                                                             FOLLOWERS)
            balanced_partitions.append(partition)
            logger.debug("Partition is balanced across available fault and update domains!")
            return True
        logger.debug("Partition needs to be balanced.")
        return False

    def _generate_reassignment_plan_for_topic(self, replica_count_topic, next_Leader, rack_alternated_list, fd_count,
                                              ud_count, brokers_replica_count, force_rebalance):
        ret = None
        reassignment = {"partitions": [], "version": 1}
        reassignment_changes = []
        retained_assignment = {"partitions": []}
        balanced_partitions = []

        # Check if #replicas is less than 3 if #FD==3/#FD==1 or #replica is less than 4 if #FD is 2
        if (fd_count == 1 and replica_count_topic < 3) or (fd_count == 2 and replica_count_topic < 4) or (
                fd_count == 3 and replica_count_topic < 3):
            logger.warning("""
            There are not as many upgrade/fault domains as the replica count for the topic %s.\n
            Replica Count: %s, Number of Fault Domains: %s, Number of Update Domains: %s.\n
            The recommendation is to have at least 3 replicas if number of fault domains in the region is 3,
            and 4 replicas if number of fault domains is 2.
            """, self.topic, replica_count_topic, fd_count, ud_count)
            if not force_rebalance:
                logger.error(
                    "Rebalance with HA not possible! Skipping rebalance for the topic. If you like to rebalance "
                    "regardless, please run the tool with -force flag.")
                return ret, balanced_partitions
            else:
                logger.info("Proceeding with generation of reassignment plan since -force flag was specified.")

        # Check if there is a valid number of replicas for the topic
        if replica_count_topic <= 1:
            logger.warning(
                "Invalid number of replicas for topic %s. Rebalance with HA guarantee not possible! The tool will try "
                "to do whats possible.",
                self.topic)
            return ret, balanced_partitions

        logger.info("Checking if Topic: %s needs to be re-balanced.", self.topic)
        # Keep track of numbers of replicas assigned to each broker

        # Iterate through all partitions and check whether they need to be re-balanced
        for i in range(0, len(self.partition_info)):
            if (not self._check_if_partition_balanced(self.partition_info[i], replica_count_topic, fd_count, ud_count,
                                                      brokers_replica_count, balanced_partitions)):
                if self._is_partition_eligible_reassignment(self.partition_info[i], replica_count_topic):
                    r, next_Leader = self._scan_partition_for_reassignment(i, brokers_replica_count,
                                                                           rack_alternated_list, next_Leader, ud_count,
                                                                           replica_count_topic)
                    if r is not None:
                        reassignment["partitions"].append(r)
                        ret = reassignment
                        reassignment_changes.append(
                            "Partition {0} {1} --> {2}".format(int(self.partition_info[i][PARTITION]),
                                                               self.partition_info[i][REPLICAS], r[REPLICAS]))
            else:
                # Partition is already balanced. Add the existing assignment to the rebalance plan.
                current_partition_assignment = {
                    "topic": self.topic,
                    PARTITION: self.partition_info[i][PARTITION],
                    REPLICAS: self.partition_info[i][ISR]
                }
                retained_assignment["partitions"].append(current_partition_assignment)

        # If some partitions need to be rebalanced (ret is not None) then append the retained assignment to the
        # reassignment partitions and update ret
        if len(retained_assignment["partitions"]) > 0 and ret is not None:
            logger.info("Topic: %s Partitions Already Balanced: %s Partitions To Be Rebalanced: %s", self.topic,
                        list(map(lambda datum: datum['partition'], retained_assignment["partitions"])),
                        list(map(lambda datum: datum['partition'], ret["partitions"])))
            for j in retained_assignment["partitions"]:
                reassignment["partitions"].append(j)
            ret = reassignment

        if len(reassignment_changes) > 0:
            logger.info(
                "\n\nReassignment changes: Topic: {0} \n{1}\n".format(self.topic, "\n".join(reassignment_changes)))
        return ret, balanced_partitions

    '''
        Verifies that the reassignment plan generated for the topic guarantees high availability.
    '''

    def _verify_reassignment_plan(self, reassignment_plan, topic, replica_count, fd_count, ud_count,
                                  brokers_replica_count=None, balanced_partitions=[]):
        logger.info("Verifying that the rebalance plan generated meets conditions for HA.")
        partitions_plan = reassignment_plan["partitions"]
        for p in partitions_plan:
            if not self._check_if_partition_balanced(p, replica_count, fd_count, ud_count, brokers_replica_count,
                                                     balanced_partitions):
                logger.warning("Unable to generate reassignment plan that guarantees high availability for topic: %s",
                               topic)
                return False
        return True


def verify_leaders_distributed(host_info, reassignment_plan, balanced_partitions):
    # Keep track of numbers of replicas assigned to each broker
    brokers_replica_count = []
    for host in host_info:
        b = {
            BROKER_ID: host[BROKER_ID],
            LEADERS: 0,
            FOLLOWERS: 0,
            RACK: host[RACK]
        }
        brokers_replica_count.append(b)

    partitions = reassignment_plan["partitions"] + balanced_partitions

    for p in partitions:
        is_leader = True
        for replica in p[REPLICAS]:
            e = [element for element in brokers_replica_count if element[BROKER_ID] == str(replica)][0]
            if is_leader:
                e[LEADERS] += 1
                is_leader = False
            else:
                e[FOLLOWERS] += 1

    logger.debug("Count of Replicas Across Brokers: " + str(brokers_replica_count))


def reassign_verify(plan_directory):
    kafka_version, hdp_version = get_kafka_hdp_version()
    if kafka_version >= '3.2.0':
        s = subprocess.check_output([
            KAFKA_REASSIGN_PARTITIONS_TOOL_PATH,
            "--bootstrap-server",
            get_broker_connect_string(),
            "--reassignment-json-file",
            os.path.join(plan_directory, ASSIGNMENT_JSON_FILE),
            "--verify"
        ])
    else:
        s = subprocess.check_output([
            KAFKA_REASSIGN_PARTITIONS_TOOL_PATH,
            "--zookeeper",
            get_zookeeper_connect_string(),
            "--reassignment-json-file",
            os.path.join(plan_directory, ASSIGNMENT_JSON_FILE),
            "--verify"
        ])
    logger.info(s)


def reassign_exec(plan_directory, throttle_limit):
    if os.stat(os.path.join(plan_directory, ASSIGNMENT_JSON_FILE)).st_size == 0:
        logger.info("The reassignment plan is empty. The cluster is possibly already balanced.")
    else:
        kafka_version, hdp_version = get_kafka_hdp_version()
        if kafka_version >= '3.2.0':
            s = subprocess.check_output([
                KAFKA_REASSIGN_PARTITIONS_TOOL_PATH,
                "--bootstrap-server",
                get_broker_connect_string(),
                "--reassignment-json-file",
                os.path.join(plan_directory, ASSIGNMENT_JSON_FILE),
                "--throttle",
                throttle_limit if throttle_limit else DEFAULT_REBALANCE_THROTTLE_RATE_BPS,
                "--execute"
            ])
            expected_output = "Successfully started partition reassignments"
        else:
            s = subprocess.check_output([
                KAFKA_REASSIGN_PARTITIONS_TOOL_PATH,
                "--zookeeper",
                get_zookeeper_connect_string(),
                "--reassignment-json-file",
                os.path.join(plan_directory, ASSIGNMENT_JSON_FILE),
                "--throttle",
                throttle_limit if throttle_limit else DEFAULT_REBALANCE_THROTTLE_RATE_BPS,
                "--execute"
            ])
            expected_output = "Successfully started reassignment of partitions"
        logger.info(s)
        if expected_output not in s:
            raise_error("Rebalance operation failed!")


'''
    Log Kafka and HDP Version
'''


def get_kafka_hdp_version():
    p1 = subprocess.Popen(["find {0} -name \*kafka_\*".format(KAFKA_LIBS_PATH)], shell=True, stdout=subprocess.PIPE)
    data = p1.stdout.readline()
    assert p1.wait() == 0
    data = data.split('\n')[0].split('/')[-1].split('-')
    splits = data[1].split('.')
    kafka_version = ".".join(splits[:3])

    # Verify Kafka version is >= 0.8.1. The official partition reassignment tool is not stable for lower versions
    if int(splits[0]) < 1:
        if int(splits[1]) < 9:
            if int(splits[2]) < 1:
                logger.warning(
                    "The official Kafka Partition reassignment tool has known bugs for versions 0.8.0 and below, "
                    "and can render a topic unusable. Please see "
                    "https://cwiki.apache.org/confluence/display/KAFKA/Replication+tools for more info. The tool is "
                    "stable from version 0.8.1. It is highly discouraged to continue execution.")

    hdp_version = ".".join(splits[3:])
    return kafka_version, hdp_version


'''
    Queries service information from Ambari to get the Kafka log directories
'''


def get_kafka_log_dirs():
    ah = AmbariHelper()
    json_output = ah.query_url(
        "clusters/" + ah.cluster_name() + "/configurations/service_config_versions?service_name.in(KAFKA)&is_current=true")
    kafka_brokers_configs = \
        [element for element in json_output["items"][0]["configurations"] if element["type"] == KAFKA_BROKER][0]
    return kafka_brokers_configs["properties"]["log.dirs"].split(',')


'''
    SSH'es to a host using the supplied credentials and executes a command.
    Throws an exception if the command doesn't return 0.
    bgrun: run command in the background
'''


def ssh(host, cmd, user, password, timeout=30, bg_run=False):
    fname = tempfile.mktemp()
    fout = open(fname, 'w')

    options = '-q -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -oPubkeyAuthentication=no'
    if bg_run:
        options += ' -f'
    ssh_cmd = 'ssh %s@%s %s "%s"' % (user, host, options, cmd)
    child = pexpect.spawn(ssh_cmd, timeout=timeout)
    child.expect(['password: '])
    child.sendline(password)
    child.logfile = fout
    child.expect(pexpect.EOF)
    child.close()
    fout.close()

    fin = open(fname, 'r')
    stdout = fin.read()
    fin.close()

    if 0 != child.exitstatus:
        raise Exception(stdout)

    return stdout


def get_partition_sizes(fqdn):
    kafka_log_dirs = get_kafka_log_dirs()
    disk_space_query = "df %s --output=avail | awk 'NR>1'" % kafka_log_dirs[0]
    free_disk_space = ssh(fqdn, disk_space_query, user_name, password)

    partition_sizes = []
    for log_dir in kafka_log_dirs:
        partition_sizes_query = "sudo du %s -d 1 | sort -nr | tr '\t' ',' | tr '\n' ';'" % log_dir
        partition_sizes.append(ssh(fqdn, partition_sizes_query, user_name, password))
    return free_disk_space, partition_sizes


def raise_error(msg):
    logger.error(msg)
    raise Exception(msg)


def main():
    parser = argparse.ArgumentParser(description='Kafka Replica Reassignment Tool')
    parser.add_argument('--topics',
                        help='Comma separated list of topics to reassign replicas. Use ALL|all to rebalance all topics',
                        type=str)
    parser.add_argument('--execute', action='store_true', default=False,
                        help='whether or not to execute the reassignment plan')
    parser.add_argument('--verify', action='store_true', default=False,
                        help='Execute rebalance of given plan and verify execution')
    parser.add_argument('--force', action='store_true', default=False,
                        help='Force rebalance of all partitions in a topic, even if already balanaced.')
    parser.add_argument('--throttle', help='Upper bound on bandwidth used to move replicas from machine to machine.')
    parser.add_argument('--rebalancePlanDir',
                        help='Directory where the rebalance plan should be saved or retrieved from.')
    parser.add_argument('--computeStorageCost', action='store_true', default=False,
                        help='Use this for a non-new cluster to use compute free disk space per broker and partition sizes to determine the best reassignment plan. ')
    parser.add_argument('--deadhosts', help='Comma separated list of hosts which have been removed from the cluster',
                        type=str)
    parser.add_argument('--username', help='Username for current user.')
    parser.add_argument('--password', help='Password for current user.')
    args = parser.parse_args()
    parser.print_help()

    kafka_version, hdp_version = get_kafka_hdp_version()
    logger.info("Kafka Version: %s", kafka_version)
    logger.info("HDP Version: %s", hdp_version)

    topics = args.topics
    compute_storage_cost = args.computeStorageCost
    global user_name
    user_name = args.username
    global password
    password = args.password
    dead_hosts = None
    force_rebalance = args.force
    throttle_limit = args.throttle

    if args.deadhosts:
        dead_hosts = [item for item in args.deadhosts.split(',')]

    # Create directory to store rebalance plan if the specified directory not exist.
    if args.rebalancePlanDir:
        try:
            os.makedirs(args.rebalancePlanDir)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(args.rebalancePlanDir):
                pass
            else:
                raise_error('Unable to create log dir: {0}'.format(args.rebalancePlanDir))
    else:
        logger.info("Please specify path the directory where the rebalance plan should be saved/read from using "
                    "--rebalancePlanDir.")
        sys.exit()

    if args.verify:
        reassign_verify(args.rebalancePlanDir)
        return

    if args.execute:
        reassign_exec(args.rebalancePlanDir, throttle_limit)
        return

    if topics is None:
        logger.info("Please specify topics to rebalance using -topics. Use ALL to rebalance all topics.")
        sys.exit()

    if topics.lower() == ALL_TOPICS_STRING.lower():
        topics = get_topic_list()
    else:
        topics = [item for item in topics.split(',')]

    logger.info("Following topics selected: %s", str(topics))

    # Initialize Zookeeper Client
    zookeeper_quorum = get_zookeeper_connect_string()
    zookeeper_client = connect(zookeeper_quorum)
    # Get broker Ids to Hosts mapping
    brokers_info = get_brokerhost_info(zookeeper_client)
    reassignment_plan = generate_reassignment_plan(args.rebalancePlanDir, topics, brokers_info, compute_storage_cost,
                                                   dead_hosts, force_rebalance)

    if reassignment_plan is None:
        logger.info(
            "No need to rebalance. Current Kafka replica assignment has High Availability OR minimum requirements for "
            "rebalance not met. Check logs at %s for more info.",
            str(log_dir) + str(log_file))
        return
    else:
        logger.info("This is the reassignment-json-file, saved as %s at the specified directory: %s",
                    ASSIGNMENT_JSON_FILE, args.rebalancePlanDir)
        logger.info("Please re-run this tool with '-execute' to perform rebalance operation.")


if __name__ == "__main__":
    initialize_logger(logger, log_file)
    main()
