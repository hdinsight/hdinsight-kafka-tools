import logging, time

from kafka_testutils import KafkaTestUtils

logger = logging.getLogger(__name__)
debug = False

def main(ktu, topic):
    zookeepers, brokers, partitions, replicationfactor, messages, threads, messagesize, batchsize = ktu.getShellInputs()

    #Create
    logger.info("Creating topic: {0}".format(topic))
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper {0} --topic {1} --partitions {2} --replication-factor {3}".format(
        zookeepers, topic, partitions, replicationfactor)
    ktu.runShellCommand(shell_command)

    #List
    logger.info("Listing topics")
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper {0}".format(zookeepers)
    ktu.runShellCommand(shell_command)

    #Describe
    logger.info("Describing topic: {0}".format(topic))
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --describe --zookeeper {0} --topic {1}".format(zookeepers, topic)
    ktu.runShellCommand(shell_command)

    #Producer
    logger.info("Producing {0} messages to topic {1}".format(messages, topic))
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-producer-perf-test.sh --broker-list {0} --topics {1} --messages {2} --message-size {3} --batch-size {4} --request-num-acks 0 --compression-codec 0 --threads {5}".format(
        brokers, topic, messages, messagesize, batchsize, threads)
    ktu.runShellCommand(shell_command)

    #Offset
    partitions_list = ",".join(map(str, range(0,partitions)))
    logger.info("Listing offsets of partitions {0} of topic {1}".format(partitions_list, topic))
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list {0} --topic {1} --partitions {2} --time -1 --offsets 1".format(
        brokers, topic, partitions_list)
    ktu.runShellCommand(shell_command)

    #Consumer
    logger.info("Consuming {0} messages from topic {1}".format(messages, topic))
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-consumer-perf-test.sh --zookeeper {0} -messages {1} --topic {2} --threads {3}".format(
        zookeepers, messages, topic, threads)
    ktu.runShellCommand(shell_command)

if __name__ == '__main__':
    topic="eventcounttest{0}".format(int(time.time()))
    ktu = KafkaTestUtils(logger, topic + ".log", debug)
    main(ktu, topic)
