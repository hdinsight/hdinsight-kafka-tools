'''
Rebalance Kafka partition replicas to achieve HA (Fault Domain/Update Domain awareness). Rebelance can be executed for one or more topics.

PRE-REQS:
=========
sudo apt-get install libffi-dev libssl-dev
sudo pip install --upgrade requests[security] PyOpenSSL ndg-httpsclient pyasn1 kazoo retry

RUNNING THE SCRIPT:
===================

1) Copy the script to /usr/hdp/current/kafka-broker/bin on your cluster

2) Run this script with sudo privilege due to permission issues on some python packages:
sudo python rebalance.py
'''

import logging, sys, json, subprocess, os.path, errno, traceback, argparse, requests
from retry import retry
from operator import itemgetter
from logging import StreamHandler
from logging.handlers import RotatingFileHandler
from logging.handlers import SysLogHandler
from hdinsight_common import hdinsightlogging
from hdinsight_common.AmbariHelper import AmbariHelper
from hdinsight_common import Constants as CommonConstants
from hdinsight_common import cluster_utilities
from hdinsight_kafka import Utilities
from kazoo.client import KazooClient
from kazoo.client import KazooState

amabriHelper = AmbariHelper()

# LOGGING 

logger = logging.getLogger(__name__)
log_file = "rebalance_log"
log_dir = "/tmp/kafka_rebalance/";
SIMPLE_FORMATTER= logging.Formatter('%(asctime)s - %(filename)s [%(process)d] %(name)s - %(levelname)s - %(message)s')
SYSLOG_FORMAT_STRING = ' %(filename)s [%(process)d] - %(name)s - %(levelname)s - %(message)s'
SYSLOG_FORMATTER = logging.Formatter(SYSLOG_FORMAT_STRING)
MIN_LOG_LEVEL = logging.INFO

'''Filters (lets through) all messages with level < LEVEL'''
class LogFilter(logging.Filter):
    def __init__(self, level):
        self.level = level

    def filter(self, log_record):
        return log_record.levelno < self.level

#LOG_LOCAL0 - belongs to hdinsight-agent
#LOG_LOCAL1 - belongs to ambari-agent
#LOG_LOCAL2 - belongs to syslog catch all
#We want to log to LOG_LOCAL2
def initialize_logger(logger, log_file, syslog_facility = SysLogHandler.LOG_LOCAL2):
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
    stdout_handler.setLevel(MIN_LOG_LEVEL)

    stderr_handler.setLevel(max(MIN_LOG_LEVEL, logging.WARNING))

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
            raise 'Unable to create log dir: {0}'.format(log_dir)

    log_file_path = os.path.join(log_dir, log_file_name)
    file_handler = RotatingFileHandler(filename=log_file_path, maxBytes=104857600, backupCount=100)
    file_handler.setLevel(MIN_LOG_LEVEL)
    file_handler.setFormatter(SIMPLE_FORMATTER)

    logger.addHandler(file_handler)

#add syslog handler if we are on linux. We need to add retry on this because /dev/log might not be created by rsyslog yet
@retry(exceptions=BaseException, tries=CommonConstants.MAX_RETRIES, delay=CommonConstants.RETRY_INTERVAL_DELAY, backoff=CommonConstants.RETRY_INTERVAL_BACKOFF)
def add_syslog_handler(logger, syslog_facility):
    try:
        mds_syslog_handler = SysLogHandler(address="/dev/log",
                                           facility=syslog_facility)
        mds_syslog_handler.setLevel(MIN_LOG_LEVEL)
        mds_syslog_handler.setFormatter(SYSLOG_FORMATTER)
        logger.addHandler(mds_syslog_handler)
        return True
    except Exception:
        logger.error('Exception occurred when adding syslog handler: ' + traceback.format_exc())
        return False

# Constants
ASSIGNMENT_JSON_FILE = "/tmp/rebalancePlan.json"
ZOOKEEPER_PORT = ":2181"
ZOOKEEPER_HOSTS = None
MAX_NUM_REPLICA = 3
BROKERS_ID_PATH = "brokers/ids"
KAFKA_TOPICS_TOOL_PATH = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh"
KAFKA_REASSING_PARTITIONS_TOOL_PATH = "/usr/hdp/current/kafka-broker/bin/kafka-reassign-partitions.sh"
FQDN = "fqdn"
BROKER_ID = "brokerId"
FAULT_DOMAIN = "faultDomain"
UPDATE_DOMAIN = "updateDomain"
RACK = "rack"
VM_ID = "vmId"
PARTITION = "partition"
REPLICAS = "replicas"
LEADER = "leader"
FOLLOWERS = "followers"
TOPICS = "topics"
ISR = "isr"

# Get the list of all topics in Kafka
def get_topic_list():
    s = subprocess.check_output([
        KAFKA_TOPICS_TOOL_PATH,
        "--zookeeper",
        cluster_utilities.get_zk_quorum(),
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
    if "cluster_topology_json_url" in settings:
        json_url = settings["cluster_topology_json_url"]
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
            VM_ID: node[VM_ID],
            FAULT_DOMAIN: str(node[FAULT_DOMAIN]),
            UPDATE_DOMAIN: str(node[UPDATE_DOMAIN]),
            FQDN: node[FQDN],
            BROKER_ID: brokers_info[node[FQDN]] if node[FQDN] in brokers_info else None,
            RACK: FAULT_DOMAIN + str(node[FAULT_DOMAIN]) + UPDATE_DOMAIN + str(node[UPDATE_DOMAIN])
        }
        host_info.append(host);
    return host_info

# Returns info about a partitcular topic
def get_topic_info(topic):
    topicInfo = subprocess.check_output([
        KAFKA_TOPICS_TOOL_PATH,
        "--zookeeper",
        cluster_utilities.get_zk_quorum(),
        "--describe",
        "--topic",
        topic
    ])
    if topicInfo is None or len(topicInfo)==0:
        raise Exception("Failed to get Kafka partition info for topic " + topic)
    return topicInfo

def get_replica_count_topic(topic):
    topicInfo = get_topic_info(topic)
    topicInfo_lines = topicInfo.split('\n')
    if len(topicInfo_lines) < 2:
        raise Exception("Failed to parse Kafka topic info")
    
    summary = topicInfo_lines[0].split()
    replica_count = int(summary[2].split(":")[1])
    if replica_count > MAX_NUM_REPLICA:
        raise Exception("Replica count exceeds threshold")
    return replica_count

# Returns info about partitions for a given topic
def get_partition_info(topic):
    topicInfo = get_topic_info(topic)
    topicInfo_lines = topicInfo.split('\n')
    if len(topicInfo_lines) < 2:
        raise Exception("Failed to parse Kafka partition info")
    
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
            PARTITION: partition,
            LEADER: leader,
            REPLICAS: replicas,
            ISR: isr
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
            logger.info("VM %s with FQDN: %s has no brokers assigned. Ensure that all brokers are up. Stopping rebalance.", host[VM_ID], host[FQDN])
            return False
    return True

def generate_reassignment_plan(topics, zookeeper_client):
    ret = None
    # Retrieve Cluster topology
    cluster_topology_json = get_cluster_topology_json()
    # Parse JSON to retrieve information about hosts 
    host_info = parse_topo_info(cluster_topology_json, zookeeper_client)
    fd_list, ud_list = generate_fd_list_ud_list(host_info)

    if check_brokers_up(host_info):
        for topic in topics:
            partition_info = get_partition_info(topic)
            rassignment_Generator = ReassignmentGenerator(host_info, topic, partition_info, fd_list, ud_list)
            reassignment_plan = rassignment_Generator.generate_reassignment_plan_for_topic()
            if reassignment_plan is not None:
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
    def __init__(self, host_info, topic, partition_info, fd_list, ud_list):
        self.host_info = host_info
        self.topic = topic
        self.partition_info = partition_info
        self.partitions_count = len(partition_info)
        self.fd_list = fd_list
        self.ud_list = ud_list
    
    def generate_alternated_ud_fd_list(self):
        fd_index = 0
        ud_index = 0
        ud_fd_list = []
        for index in range(0,len(self.fd_list)*len(self.ud_list)):
            ud_fd_list.append(FAULT_DOMAIN + self.fd_list[fd_index % len(self.fd_list)] + UPDATE_DOMAIN + self.ud_list[ud_index % len(self.ud_list)])
            fd_index += 1
            ud_index += 1
        return ud_fd_list
        
    def is_partition_eligible_reassignment(self, partition):
        ''' 
        Conditions required for a partition to be eligible for ReassignmentGenerator
        1> MIN(len(ISR)) >= 1
        2> Leader is in ISR
        3> Leader is assigned (not -1)
        4> Replicas are present
        ''' 
        if all([len(partition[ISR]) >= 1, partition[LEADER] in partition[ISR], int(partition[LEADER]) != -1, partition[REPLICAS]]):
            return True
        logger.info("Partition %s for Topic %s does not meet criteria for rebalance. Skipping.", partition[PARTITION], self.topic)
        return False
    
    def get_brokers_in_rack(self, rack):
        return [element for element in self.host_info if element[RACK] == rack]
    
    def get_broker_info(self, b_id):
        return [element for element in self.host_info if int(element[BROKER_ID]) == b_id][0]
    
    def get_count_replicas_in_broker(self, broker_id, broker_replica_count):
        return [element for element in broker_replica_count if element[BROKER_ID] == broker_id][0]
    
    def increment_count_replicas_in_broker(self, broker_id, broker_replica_count, type_of_count):
        e = [element for element in broker_replica_count if element[BROKER_ID] == broker_id][0]
        e[type_of_count] += 1
    
    def assign_replica_for_partition(self, rack_alternated_list, broker_replica_count, next_rack, type_of_replica):
        eligible_brokers = self.get_brokers_in_rack(rack_alternated_list[next_rack])
        new_broker = eligible_brokers[0]
        for broker in eligible_brokers:
            a = self.get_count_replicas_in_broker(broker[BROKER_ID], broker_replica_count)[type_of_replica]
            b = self.get_count_replicas_in_broker(new_broker[BROKER_ID], broker_replica_count)[type_of_replica]
            if a < b:
                new_broker = broker
        self.increment_count_replicas_in_broker(new_broker[BROKER_ID], broker_replica_count, type_of_replica)
        return new_broker[BROKER_ID]

    def scan_partition_for_reassignment(self, index, brokers_replica_count, rack_alternated_list, next_Leader, next_Follower, round_robin_iteration):
        reassignment = { "topic" : self.topic,
        PARTITION : self.partition_info[index][PARTITION],
        REPLICAS : []
        }

        replica_count = len(self.partition_info[index][REPLICAS])
        rack_count = len(rack_alternated_list)  
        shift = rack_count * round_robin_iteration

        # Re-assign Leader
        leader_broker_id = self.assign_replica_for_partition(rack_alternated_list, brokers_replica_count, next_Leader % rack_count, "leaders")
        next_Leader += 1
        reassignment[REPLICAS].append(leader_broker_id)

        # Re-assign follower replicas
        if replica_count > 1:
            for j in range(0,replica_count-1):
                follower_broker_id = self.assign_replica_for_partition(rack_alternated_list, brokers_replica_count, (next_Follower + j + shift) % rack_count, FOLLOWERS)
                reassignment[REPLICAS].append(follower_broker_id)
            next_Follower += 1
        
        #logger.info("Partition: " + str(self.partition_info[index][PARTITION]) + " - Changing " + str(self.partition_info[index][REPLICAS]) + " to " + str(reassignment[REPLICAS]))

        # Check if round robin complete
        if next_Follower % rack_count == 0:
            round_robin_iteration += 1

        return reassignment, next_Leader, next_Follower, round_robin_iteration

    def check_if_topic_balanced(self):
        for p in self.partition_info:
            fd_ud_list = []
            for replica in p[REPLICAS]:
                # Get the rack associated with the replica and add to list
                fd_ud_list.append(self.get_broker_info(int(replica))[RACK])
            if len(fd_ud_list) > len(set(fd_ud_list)):
                return False
        return True       

    def generate_reassignment_plan_for_topic(self):
        ret = None
        reassignment={"partitions":[], "version":1}

        # Check if(Min(#UD,#FD) > = #replicas)
        if min ([len(self.ud_list), len(self.fd_list)]) < get_replica_count_topic(self.topic):
            logger.info("There are not as many upgrade/fault domains as the replica count for the topic %s. Rebalance with HA guarantee not possible! Skipping rebalance for the topic.", self.topic)
            return ret
        
        # Check if there is a valid number of replicas for the topic
        if get_replica_count_topic(self.topic) <= 1:
            logger.info("Invalid number of replicas for topic %s. Rebalance with HA guarantee not possible! Skipping rebalance for the topic.", self.topic)
            return ret
        
        # Check if the topic is already balanced. If not generate rebalance plan
        if not self.check_if_topic_balanced():
            logger.info("Generating Rebalance plan for TOPIC: %s", self.topic)
            # Keep track of numbers of replicas assigned to each broker
            brokers_replica_count = []
            for host in self.host_info:
                b = {
                    BROKER_ID : host[BROKER_ID],
                    "leaders" : 0,
                    FOLLOWERS: 0,
                }
                brokers_replica_count.append(b) 

            rack_alternated_list = self.generate_alternated_ud_fd_list()
            next_Leader = 0
            next_Follower = 1
            round_robin_iteration = 0
            
            for i in range(0,len(self.partition_info)):
                if self.is_partition_eligible_reassignment(self.partition_info[i]):
                    r, next_Leader, next_Follower, round_robin_iteration = self.scan_partition_for_reassignment(i, brokers_replica_count, rack_alternated_list, next_Leader, next_Follower, round_robin_iteration)
                    if r is not None:
                        reassignment["partitions"].append(r)
                        ret = reassignment
        else:
            logger.info("TOPIC: %s is already balanced. Skipping rebalance.", self.topic)

        return ret

def reassign_verify():
    s = subprocess.check_output([KAFKA_REASSING_PARTITIONS_TOOL_PATH,
    "--zookeeper",
    cluster_utilities.get_zk_quorum(),
    "--reassignment-json-file " + ASSIGNMENT_JSON_FILE,
    "--verify"
    ])
    logger.info(s)

def reassign_exec():
    s = subprocess.check_output([KAFKA_REASSING_PARTITIONS_TOOL_PATH,
    "--zookeeper",
    "--reassignment-json-file " + ASSIGNMENT_JSON_FILE,
    "--execute"
    ])
    logger.info(s)
    if "Successfully started reassignment of partitions" not in s:
        raise Exception("Operation Failed!")

def main():
    parser = argparse.ArgumentParser(description='Rebalance Kafka Replicas! :)')
    parser.add_argument('--topics', nargs='+', help='Comma separated list of topics to reassign replicas. Use ALL|all to rebalance all topics')
    parser.add_argument('--balanceLeaders', action="store_true", help='Balance Leaders')
    parser.add_argument('--execute', nargs='?', const='true', default='false', help='whether or not to execute the reassignment plan')
    parser.add_argument('--verify', nargs='?', const='true', default='false', help='verify execution of the reassignment plan')
    args = parser.parse_args()

    topics = args.topics

    if topics is None:
        logger.info("Pleae specify topics to rebalance using --topics. Use ALL to rebalance all topics.")
        sys.exit()

    if topics[0].lower() == "all".lower():
        topics = get_topic_list()

    logger.info("Following topics selected: %s", str(topics))

    # Initialize Zookeeper Client
    zookeeper_quorum = cluster_utilities.get_zk_quorum()
    zookeeper_client = connect(zookeeper_quorum)

    reassignment_plan = generate_reassignment_plan(topics, zookeeper_client)

    if reassignment_plan is None:
        logger.info("No need to rebalance. Current Kafka replica assignment has High Availability OR minimum requirements for rebalance not met. Check logs for more info.")
        return

    if args.verify=='true':
        reassign_verify()
        return

    if args.execute=='true':
        reassign_exec()
    else:
        logger.info("Please re-run this tool with '--execute' to perform rebalancing.")
        logger.info("This is the reassignment-json-file, saved as %s", ASSIGNMENT_JSON_FILE)
        #logger.info(reassignment_plan)

if __name__ == "__main__":
    initialize_logger(logger, log_file)
    main()