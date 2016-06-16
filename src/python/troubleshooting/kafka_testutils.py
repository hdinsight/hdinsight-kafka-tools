import logging, subprocess, time, sys

from hdinsight_common.AmbariHelper import AmbariHelper
from hdinsight_common import Constants
from hdinsight_kafka.kafka_remote_storage import KafkaRemoteStorage

class KafkaTestUtils:
    def __init__(self, logger, log_file, debug = False):
        self.debug = debug
        self.logger = logger
        self.logger.setLevel(logging.DEBUG)

        logging_format = "%(asctime)s - %(filename)s - %(name)s - %(levelname)s - %(message)s"
        # create console handler and set level to info
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(logging_format)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # create file handler and set level to debug
        handler = logging.FileHandler(log_file,"a")
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(logging_format)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def runShellCommand(self, shell_command, throw_on_error = True):
        self.logger.info(shell_command)
        if not self.debug:
            p = subprocess.Popen(shell_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = p.communicate()

            if stdout:
                self.logger.info("\n" + stdout)

            if stderr:
                self.logger.error("\n" + stderr)
        
            if throw_on_error and (not (p.returncode == 0)):
                self.logger.error("Process returned non-zero exit code: {0}".format(p.returncode))
                sys.exit(p.returncode)

            return stdout, stderr

    def getBrokerInformation(self):
        ambari_helper = AmbariHelper()
        cluster_manifest = ambari_helper.get_cluster_manifest()
        cluster_name = cluster_manifest.deployment.cluster_name
        hosts_result = ambari_helper.query_url('clusters/{0}/hosts'.format(cluster_name))

        zookeepers = reduce(lambda r1,r2 : r1 + ',' + r2,
                        map(lambda m : m['Hosts']['host_name'] + ':' + Constants.ZOOKEEPER_CLIENT_PORT,
                            filter(lambda h:h['Hosts']['host_name'].startswith(cluster_manifest.settings[Constants.ZOOKEEPER_VM_NAME_PREFIX_SETTING_KEY]),
                                hosts_result['items'])))
        self.logger.info("zookeepers: {0}\n".format(zookeepers))

        broker_hosts = map(lambda m : m['Hosts']['host_name'],
                          filter(lambda h : h['Hosts']['host_name'].startswith(cluster_manifest.settings[Constants.WORKERNODE_VM_NAME_PREFIX_SETTING_KEY]), 
                              hosts_result['items']))
        self.logger.info("broker_hosts: {0}\n".format(broker_hosts))

        brokers = reduce(lambda r1, r2 : r1 + ',' + r2,
                      map(lambda m : m + ':9092',
                          broker_hosts))
        self.logger.info("brokers: {0}\n".format(brokers))

        return zookeepers, broker_hosts, brokers

    def getShellInputs(self):
        zookeepers, broker_hosts, brokers = self.getBrokerInformation()
        broker_hosts_count = len(broker_hosts)

        self.logger.info("Choosing defaults:")
        partitions=broker_hosts_count
        replicationfactor=min(3,broker_hosts_count)
        messages=(partitions*1000000)
        threads=min(4,partitions)
        messagesize=100
        batchsize=10000

        self.logger.info("\n" + 
            "zookeepers = {0}".format(zookeepers) +
            "brokers = {0}".format(brokers) +
            "partitions = {0}".format(partitions) +
            "replicationfactor = {0}".format(replicationfactor) +
            "messages = {0}".format(messages) +
            "threads = {0}".format(threads) +
            "messagesize = {0}".format(messagesize) +
            "batchsize = {0}".format(batchsize)
        )
    
        return zookeepers, brokers, partitions, replicationfactor, messages, threads, messagesize, batchsize

    def getShareNames(self):
        kfr = KafkaRemoteStorage()
        data = kfr.get_kafka_remote_storage_map()
        return map(lambda k:k, data['kafkaRemoteStorageMap']['0']['azureFileShareMap'])
