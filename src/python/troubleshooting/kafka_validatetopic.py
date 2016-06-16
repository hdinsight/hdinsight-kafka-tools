import logging, time
from kafka_testutils import KafkaTestUtils

logger = logging.getLogger(__name__)
debug = False

def main(ktu):
    zookeepers, brokers, partitions, replicationfactor, messages, threads, messagesize, batchsize = ktu.getShellInputs()

    logger.info("Listing topics")
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper {0}".format(zookeepers)
    stdout, stderr = ktu.runShellCommand(shell_command)
    topics = stdout

    if debug and not topics:
        topics = "debug"

    logger.info("topics:\n{0}\n".format(topics))

    topic_list = topics.split('\n')
    logger.info("topic list: {0}\n".format(topic_list))

    for topic in topic_list:
        if topic:
            #Describe
            logger.info("Describing topic: {0}".format(topic))
            shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --describe --zookeeper {0} --topic {1}".format(zookeepers, topic)
            stdout, stderr = ktu.runShellCommand(shell_command)

            #Offset
            partitions_list = ",".join(map(str, range(0,partitions)))
            logger.info("Listing offsets of partitions {0} of topic {1}".format(partitions_list, topic))
            shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list {0} --topic {1} --partitions {2} --time -1 --offsets 1".format(
                brokers, topic, partitions_list)
            stdout, stderr = ktu.runShellCommand(shell_command)

            #Consumer
            logger.info("Consuming {0} messages from topic {1}".format(messages, topic))
            shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-consumer-perf-test.sh --zookeeper {0} -messages {1} --topic {2} --threads {3}".format(
                zookeepers, messages, topic, threads)
            stdout, stderr = ktu.runShellCommand(shell_command)

            #Producer
            logger.info("Producing {0} messages to topic {1}".format(messages, topic))
            shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-producer-perf-test.sh --broker-list {0} --topics {1} --messages {2} --message-size {3} --batch-size {4} --request-num-acks 0 --compression-codec 0 --threads {5}".format(
                brokers, topic, messages, messagesize, batchsize, threads)
            stdout, stderr = ktu.runShellCommand(shell_command)

            #Offset
            partitions_list = ",".join(map(str, range(0,partitions)))
            logger.info("Listing offsets of partitions {0} of topic {1}".format(partitions_list, topic))
            shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list {0} --topic {1} --partitions {2} --time -1 --offsets 1".format(
                brokers, topic, partitions_list)
            stdout, stderr = ktu.runShellCommand(shell_command)

            #Consumer
            logger.info("Consuming {0} messages from topic {1}".format(messages, topic))
            shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-consumer-perf-test.sh --zookeeper {0} --messages {1} --topic {2} --threads {3}".format(
                zookeepers, messages, topic, threads)
            stdout, stderr = ktu.runShellCommand(shell_command)

if __name__ == '__main__':
    ktu = KafkaTestUtils(logger, "validatetopic{0}.log".format(int(time.time())), debug)
    main(ktu)
