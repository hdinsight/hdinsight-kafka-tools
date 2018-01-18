"""A script to perform a quick Kafka Performance test.
"""
import logging, time

from kafka_utils import KafkaUtils

logger = logging.getLogger(__name__)
debug = False

def get_kafka_shell_inputs(utils):
    """Gets the inputs for Kafka Shell. Takes KafkaUtils class object as the input.
    
    Returns the following:
    Zookeeper quorum
    Broker connection string
    Number of partitions = Number of broker hosts
    replicationfactor = 3 (default)
    messages = 1M per partition
    threads = min(4, partitions)
    messagesize = 100 Bytes
    batchsize = 10000
    """
    zookeepers = utils.get_zookeeper_quorum()
    broker_hosts, brokers = utils.get_brokers_from_ambari()
    broker_hosts_count = len(broker_hosts)

    utils.logger.info('Choosing defaults:')
    partitions=broker_hosts_count
    replicationfactor=min(3,broker_hosts_count)
    messages=(partitions*1000000)
    threads=min(4,partitions)
    messagesize=100
    batchsize=10000

    logger.info('\n' + 
        'zookeepers = {0}\n'.format(zookeepers) +
        'brokers = {0}\n'.format(brokers) +
        'partitions = {0}\n'.format(partitions) +
        'replicationfactor = {0}\n'.format(replicationfactor) +
        'messages = {0}\n'.format(messages) +
        'threads = {0}\n'.format(threads) +
        'messagesize = {0}\n'.format(messagesize) +
        'batchsize = {0}\n'.format(batchsize)
    )

    return zookeepers, brokers, partitions, replicationfactor, messages, threads, messagesize, batchsize

def main(utils, topic):
    zookeepers, brokers, partitions, replicationfactor, messages, threads, messagesize, batchsize = get_kafka_shell_inputs(utils)

    #Create
    logger.info("Creating topic: {0}".format(topic))
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper {0} --topic {1} --partitions {2} --replication-factor {3}".format(
        zookeepers, topic, partitions, replicationfactor)
    utils.run_shell_command(shell_command)

    #List
    logger.info("Listing topics")
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper {0}".format(zookeepers)
    utils.run_shell_command(shell_command)

    #Describe
    logger.info("Describing topic: {0}".format(topic))
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --describe --zookeeper {0} --topic {1}".format(zookeepers, topic)
    utils.run_shell_command(shell_command)

    #Produce
    logger.info("Producing {0} messages to topic {1}".format(messages, topic))
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-producer-perf-test.sh --broker-list {0} --topics {1} --messages {2} --message-size {3} --batch-size {4} --request-num-acks 0 --compression-codec 0 --threads {5}".format(
        brokers, topic, messages, messagesize, batchsize, threads)
    utils.run_shell_command(shell_command)

    #Offset
    partitions_list = ",".join(map(str, range(0,partitions)))
    logger.info("Listing offsets of partitions {0} of topic {1}".format(partitions_list, topic))
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list {0} --topic {1} --partitions {2} --time -1 --offsets 1".format(
        brokers, topic, partitions_list)
    utils.run_shell_command(shell_command)

    #Consume
    logger.info("Consuming {0} messages from topic {1}".format(messages, topic))
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-consumer-perf-test.sh --zookeeper {0} -messages {1} --topic {2} --threads {3}".format(
        zookeepers, messages, topic, threads)
    utils.run_shell_command(shell_command)

    #Delete
    logger.info("Deleting topic: {0}".format(topic))
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --delete --zookeeper {0} --topic {1}".format(
        zookeepers, topic)
    utils.run_shell_command(shell_command)
    
if __name__ == '__main__':
    topic="kafkaperftest{0}".format(int(time.time()))
    utils = KafkaUtils(logger, topic + ".log", debug)
    main(utils, topic)
